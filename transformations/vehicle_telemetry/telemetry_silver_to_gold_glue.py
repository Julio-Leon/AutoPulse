"""
AutoPulse - AWS Glue job: vehicle telemetry Silver -> Gold (incremental).

Builds two Gold tables from the new Silver telemetry:

  fact_vehicle_telemetry   one row per telemetry event (appended)
      event_id, vehicle_id
      date_key               New York date as a number (20261022) -> dim_date
      event_timestamp        UTC
      event_timestamp_local  New York time
      event_hour_local       0-23, New York time
      cell_id                ~500 m grid cell -> dim_location (null outside NYC)
      latitude, longitude, speed_mph, odometer_miles, fuel_level_pct, engine_status
      is_speeding            speed above New York City's 25 mph default limit

      gold/fact_vehicle_telemetry/event_date_local=2026-10-22/<new file per run>

  dim_vehicle              one row per vehicle (replaced every run)
      vehicle_id, driver_name, make, model, model_year,
      first_seen_at, last_seen_at (UTC), updated_at

      gold/dim_vehicle/<one file>

How a run works:
  1. JOB BOOKMARK: read only the Silver files that are new since the last run.
  2. Append the new events to fact_vehicle_telemetry.
  3. dim_vehicle: combine the vehicles in this batch with the current table
     (newest driver/make/model/year wins; first/last seen widened), then
     replace the table. It's 10 rows, so this costs nothing.
  4. job.commit() saves the bookmark (only after the writes succeed).

--- Glue job setup (console) ---
  Name: autopulse-telemetry-gold
  Type: Spark, Glue 5.0, Python 3, worker G.1X
  Workers: 2, with "Automatically scale the number of workers" on, maximum 10
  Timeout: 30 min
  Job bookmark: ENABLE   (required - this is what makes it incremental)
  Paste this file into the script editor.
  Job parameters:
      --extra-py-files      s3://autopulse-julio-leon/deployment/autopulse/transformations/shared/nyc_grid.py   (required)
      --SILVER_PATH         s3://autopulse-julio-leon/silver/vehicle_telemetry        (optional)
      --GOLD_FACT_PATH      s3://autopulse-julio-leon/gold/fact_vehicle_telemetry     (optional)
      --GOLD_VEHICLE_PATH   s3://autopulse-julio-leon/gold/dim_vehicle               (optional)

To rebuild: delete both Gold folders, reset the job bookmark, run.

Runs in AWS Glue only (the awsglue library exists only there).
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

import nyc_grid     # attached with --extra-py-files

# --------------------------------------------------
# Job parameters
# --------------------------------------------------

DEFAULTS = {
    "SILVER_PATH": "s3://autopulse-julio-leon/silver/vehicle_telemetry",
    "GOLD_FACT_PATH": "s3://autopulse-julio-leon/gold/fact_vehicle_telemetry",
    "GOLD_VEHICLE_PATH": "s3://autopulse-julio-leon/gold/dim_vehicle",
}


def read_job_parameters():
    """JOB_NAME is always passed by Glue; the paths are optional (defaults above)."""
    passed = [name for name in DEFAULTS if f"--{name}" in sys.argv]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", *passed])
    return {**DEFAULTS, **args}


LOCAL_TIMEZONE = "America/New_York"
SPEED_LIMIT_MPH = 25            # New York City's default speed limit
VEHICLE_DETAILS = ["driver_name", "make", "model", "model_year"]


# --------------------------------------------------
# fact_vehicle_telemetry
# --------------------------------------------------

def build_fact_vehicle_telemetry(silver):
    local_time = F.from_utc_timestamp("event_timestamp", LOCAL_TIMEZONE)
    return silver.select(
        "event_id",
        "vehicle_id",
        F.date_format(local_time, "yyyyMMdd").cast("int").alias("date_key"),
        "event_timestamp",
        local_time.alias("event_timestamp_local"),
        F.hour(local_time).alias("event_hour_local"),
        nyc_grid.cell_id_for("latitude", "longitude").alias("cell_id"),
        "latitude", "longitude", "speed_mph",
        (F.col("speed_mph") > SPEED_LIMIT_MPH).alias("is_speeding"),
        "odometer_miles", "fuel_level_pct", "engine_status",
        F.date_format(local_time, "yyyy-MM-dd").alias("event_date_local"),   # Gold folder (not stored in the file)
    )


# --------------------------------------------------
# dim_vehicle
# --------------------------------------------------

def summarize_vehicles(rows, seen_from, seen_until):
    """
    One row per vehicle: the details from its most recent row (max of a struct
    compares the first field, the time, first), plus the first/last time seen.
    """
    latest = F.max(F.struct(F.col(seen_until), *[F.col(c) for c in VEHICLE_DETAILS])).alias("latest")
    return (
        rows.groupBy("vehicle_id")
        .agg(latest, F.min(seen_from).alias("first_seen_at"), F.max(seen_until).alias("last_seen_at"))
        .select("vehicle_id", *[F.col(f"latest.{c}").alias(c) for c in VEHICLE_DETAILS],
                "first_seen_at", "last_seen_at")
    )


def build_dim_vehicle(spark, silver, vehicle_path):
    """The current dim_vehicle combined with the vehicles in this batch, materialized in memory."""
    in_batch = summarize_vehicles(silver, "event_timestamp", "event_timestamp")

    try:
        current = spark.read.parquet(vehicle_path).select(in_batch.columns)
        combined = summarize_vehicles(in_batch.unionByName(current), "first_seen_at", "last_seen_at")
    except AnalysisException:          # first run: the table doesn't exist yet
        combined = in_batch

    dim_vehicle = combined.withColumn("updated_at", F.current_timestamp())
    # Collect the (10) rows before the old files are deleted: Spark reads lazily, so
    # without this it would read the folder only after it was emptied.
    return spark.createDataFrame(dim_vehicle.collect(), dim_vehicle.schema)


# --------------------------------------------------
# Writing
# --------------------------------------------------

def write_parquet(glue_context, df, path, name, partition_column=None):
    """Write a DataFrame to S3 as Parquet through a Glue DynamicFrame (Glue's Parquet writer)."""
    options = {"path": path.rstrip("/") + "/"}
    if partition_column:
        options["partitionKeys"] = [partition_column]
        df = df.repartition(partition_column)       # one file per folder
    else:
        df = df.coalesce(1)                         # one file
    glue_context.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(df, glue_context, name),
        connection_type="s3",
        connection_options=options,
        format="parquet",
        format_options={"useGlueParquetWriter": True, "compression": "snappy"},
    )


# --------------------------------------------------
# Job
# --------------------------------------------------

def main():
    args = read_job_parameters()
    vehicle_path = args["GOLD_VEHICLE_PATH"].rstrip("/") + "/"

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    print(f"Silver:      {args['SILVER_PATH']}\nFact:        {args['GOLD_FACT_PATH']}\n"
          f"dim_vehicle: {vehicle_path}")

    # Only Silver files that are new since the last run (job bookmark, keyed by transformation_ctx).
    silver = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [args["SILVER_PATH"]], "recurse": True},
        format="parquet",
        transformation_ctx="silver_vehicle_telemetry",
    ).toDF().cache()

    if silver.isEmpty():
        print("No new Silver files since the last run.")
        job.commit()
        return

    # 1. Append the new events.
    fact = build_fact_vehicle_telemetry(silver)
    write_parquet(glue_context, fact, args["GOLD_FACT_PATH"], "fact_vehicle_telemetry", "event_date_local")
    print(f"fact_vehicle_telemetry: appended {silver.count()} events.")

    # 2. Replace dim_vehicle with the updated version.
    dim_vehicle = build_dim_vehicle(spark, silver, vehicle_path)
    glue_context.purge_s3_path(vehicle_path, {"retentionPeriod": 0})
    write_parquet(glue_context, dim_vehicle, vehicle_path, "dim_vehicle")
    print(f"dim_vehicle: {dim_vehicle.count()} vehicles.")

    job.commit()


main()
