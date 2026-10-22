"""
AutoPulse - AWS Glue job: vehicle telemetry Bronze -> Silver (+ quarantine).

Incremental, using Glue JOB BOOKMARKS: Glue remembers which Bronze files it
already read, so each run reads only the files that arrived since the last
successful run, appends the results, and saves the bookmark (job.commit()).

Why incremental: telemetry grows by ~7,200 events an hour. Rebuilding all of
Silver every run would rewrite (and make Snowflake reload) the whole history.

Reading and writing both use Glue DynamicFrames: the read is tracked by the
job bookmark; the writes append new Parquet files with Glue's Parquet writer.

Output is organized by date, one new file per date per run:
    silver/vehicle_telemetry/event_date=2026-10-21/...       (date of the event, UTC)
    quarantine/vehicle_telemetry/ingest_date=2026-10-21/...  (date it reached Bronze;
                                                               a bad row may have no valid event time)

Per Bronze row:
  1. Parse the raw JSON into columns.
  2. Validate: IDs and vehicle details (driver, make, model, year) present,
     timestamp parseable, coordinates/speed/odometer/fuel in range, engine
     status one of running/idling/off.
  3. Valid rows -> Silver. Invalid rows -> quarantine, with a validation_error
     explaining why. Duplicate event_ids within a run -> quarantine too.
  Each row is classified once (one pass), then split into the two outputs.

--- Glue job setup (console) ---
  Name: autopulse-telemetry-silver
  Type: Spark, Glue 5.0, Python 3, worker G.1X
  Workers: 2, with "Automatically scale the number of workers" on, maximum 10
  Timeout: 30 min
  Job bookmark: ENABLE   (required - this is what makes it incremental)
  Paste this file into the script editor.
  Job parameters (optional; defaults below):
      --BRONZE_PATH      s3://autopulse-julio-leon/bronze/vehicle_telemetry
      --SILVER_PATH      s3://autopulse-julio-leon/silver/vehicle_telemetry
      --QUARANTINE_PATH  s3://autopulse-julio-leon/quarantine/vehicle_telemetry

To reprocess everything: delete Silver and quarantine, then in the Glue
console choose the job -> Actions -> Reset job bookmark.

Runs in AWS Glue only (the awsglue library exists only there).
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

# --------------------------------------------------
# Job parameters
# --------------------------------------------------

DEFAULTS = {
    "BRONZE_PATH": "s3://autopulse-julio-leon/bronze/vehicle_telemetry",
    "SILVER_PATH": "s3://autopulse-julio-leon/silver/vehicle_telemetry",
    "QUARANTINE_PATH": "s3://autopulse-julio-leon/quarantine/vehicle_telemetry",
}


def read_job_parameters():
    """JOB_NAME is always passed by Glue; the paths are optional (defaults above)."""
    passed = [name for name in DEFAULTS if f"--{name}" in sys.argv]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", *passed])
    return {**DEFAULTS, **args}


# The JSON event, as sent by sources/vehicle_simulator/simulate_vehicle_telemetry.py
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("driver_name", StringType()),
    StructField("make", StringType()),
    StructField("model", StringType()),
    StructField("model_year", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_mph", DoubleType()),
    StructField("odometer_miles", DoubleType()),
    StructField("fuel_level_pct", DoubleType()),
    StructField("engine_status", StringType()),
    StructField("is_simulated", BooleanType()),
])


# --------------------------------------------------
# Transformation
# --------------------------------------------------

def validation_error():
    """The first rule a row breaks, or null if it passes them all."""
    rules = [
        (F.col("event_id").isNull() | (F.col("event_id") == ""), "missing_event_id"),
        (F.col("vehicle_id").isNull() | (F.col("vehicle_id") == ""), "missing_vehicle_id"),
        (F.col("driver_name").isNull() | F.col("make").isNull() | F.col("model").isNull()
         | F.col("model_year").isNull(), "missing_vehicle_details"),
        (F.col("event_timestamp").isNull(), "invalid_timestamp"),
        (~F.col("latitude").between(-90, 90) | F.col("latitude").isNull(), "invalid_latitude"),
        (~F.col("longitude").between(-180, 180) | F.col("longitude").isNull(), "invalid_longitude"),
        ((F.col("speed_mph") < 0) | F.col("speed_mph").isNull(), "invalid_speed"),
        ((F.col("odometer_miles") < 0) | F.col("odometer_miles").isNull(), "invalid_odometer"),
        (~F.col("fuel_level_pct").between(0, 100) | F.col("fuel_level_pct").isNull(), "invalid_fuel_level"),
        (~F.col("engine_status").isin("running", "idling", "off") | F.col("engine_status").isNull(),
         "invalid_engine_status"),
        (F.col("is_simulated").isNull(), "missing_simulation_flag"),
    ]
    error = F.lit(None).cast("string")
    for broken, reason in reversed(rules):      # reversed, so the first rule wins
        error = F.when(broken, F.lit(reason)).otherwise(error)
    return error


def classify(bronze):
    """
    Every Bronze row once, with validation_error = null for good rows (-> Silver)
    or the reason it was rejected (-> quarantine).
    """
    parsed = (
        bronze
        .withColumn("event", F.from_json("raw_payload", EVENT_SCHEMA))
        .select(
            "raw_payload", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "ingested_at",
            F.trim("event.event_id").alias("event_id"),
            F.upper(F.trim("event.vehicle_id")).alias("vehicle_id"),
            F.trim("event.driver_name").alias("driver_name"),
            F.trim("event.make").alias("make"),
            F.trim("event.model").alias("model"),
            "event.model_year",
            F.to_timestamp("event.timestamp").alias("event_timestamp"),
            "event.latitude", "event.longitude", "event.speed_mph",
            "event.odometer_miles", "event.fuel_level_pct",
            F.lower(F.trim("event.engine_status")).alias("engine_status"),
            "event.is_simulated",
        )
        .withColumn("validation_error", validation_error())
    )

    # The same event can arrive twice (Kafka delivers "at least once"): keep the first
    # valid copy, and mark later copies as duplicates.
    first_seen = (Window.partitionBy("event_id", F.col("validation_error").isNull())
                        .orderBy("kafka_partition", "kafka_offset"))
    return (
        parsed
        .withColumn("_copy", F.row_number().over(first_seen))
        .withColumn("validation_error",
                    F.when(F.col("validation_error").isNull() & (F.col("_copy") > 1), F.lit("duplicate_event_id"))
                     .otherwise(F.col("validation_error")))
        .drop("_copy")
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("ingest_date", F.to_date("ingested_at"))
    )


# --------------------------------------------------
# Writing
# --------------------------------------------------
def write_parquet(glue_context, df, path, partition_column, name):
    """
    Append a DataFrame to S3 as Parquet through a Glue DynamicFrame (Glue's Parquet
    writer), in one folder per value of partition_column, one new file per folder.
    """
    frame = DynamicFrame.fromDF(df.repartition(partition_column), glue_context, name)
    glue_context.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        connection_options={"path": path.rstrip("/") + "/", "partitionKeys": [partition_column]},
        format="parquet",
        format_options={"useGlueParquetWriter": True, "compression": "snappy"},
    )


# --------------------------------------------------
# Job
# --------------------------------------------------

def main():
    args = read_job_parameters()

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    print(f"Bronze: {args['BRONZE_PATH']}\nSilver: {args['SILVER_PATH']}\n"
          f"Quarantine: {args['QUARANTINE_PATH']}")

    # Read only Bronze files not seen before (job bookmark, keyed by transformation_ctx).
    # Skip Spark's streaming log folder (_spark_metadata): it isn't data.
    bronze_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [args["BRONZE_PATH"]],
            "recurse": True,
            "exclusions": '["**_spark_metadata**"]',
        },
        format="parquet",
        transformation_ctx="bronze_vehicle_telemetry",
    )

    bronze = bronze_frame.toDF()
    if bronze.isEmpty():
        print("No new Bronze files since the last run.")
        job.commit()
        return

    # Classify every row once and keep the result in memory for the writes and the counts.
    rows = classify(bronze).cache()

    # One pass for all the counts: rows per outcome (null = Silver).
    outcomes = {r["validation_error"]: r["count"] for r in rows.groupBy("validation_error").count().collect()}
    silver_count = outcomes.pop(None, 0)
    quarantine_count = sum(outcomes.values())
    print(f"New Bronze rows: {silver_count + quarantine_count} -> Silver: {silver_count}, "
          f"quarantine: {quarantine_count} {outcomes or ''}")

    if silver_count:
        silver = rows.filter(F.col("validation_error").isNull()).drop("raw_payload", "validation_error", "ingest_date")
        write_parquet(glue_context, silver, args["SILVER_PATH"], "event_date", "silver_vehicle_telemetry")
    if quarantine_count:
        quarantine = rows.filter(F.col("validation_error").isNotNull()).drop("event_date")
        write_parquet(glue_context, quarantine, args["QUARANTINE_PATH"], "ingest_date", "quarantine_vehicle_telemetry")

    # Save the bookmark only after the output is written, so a failed run is retried in full.
    job.commit()


main()
