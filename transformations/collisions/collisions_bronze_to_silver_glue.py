"""
AutoPulse - AWS Glue job: collisions Bronze -> Silver (incremental, by crash date).

Bronze and Silver are both organized by crash date:

    bronze/collisions/crash_date_part=2026-06-01/<one file per date>   (written by the Lambda)
    silver/collisions/crash_date_part=2026-06-01/<one file per date>   (written by this job)

How a run works:
  1. Glue's JOB BOOKMARK hands this job only the Bronze files that are new
     since the last successful run. The Lambda writes each crash date as one
     complete file, so a new file = the full, current content of that date.
  2. Clean those rows (steps below).
  3. For each crash date found: empty that date's Silver folder, then write
     the cleaned rows. Dates that didn't change are never read or rewritten,
     so run time depends on how much changed, not on how much history exists.
  4. job.commit() saves the bookmark (only after the writes succeed).

Cleaning steps:
  1. Make sure every expected column exists. The API leaves out fields that
     have no value, so a column can be missing from a whole date window.
  2. Convert types: IDs and counts to numbers, coordinates to decimals,
     crash_date to a timestamp.
  3. Drop rows without a collision_id or crash_date.
  4. Remove duplicates within the run: keep the most recently ingested copy of
     each collision (the city revises records).
  5. Borough: trim and upper-case; missing becomes "UNKNOWN", with a
     borough_known flag. Those collisions are kept, because many are highway
     crashes that still count.
  6. coordinates_valid: true only inside New York City's bounding box (this
     filters out the 0,0 placeholder coordinates in the city's data).

--- Glue job setup (console) ---
  Name: autopulse-collisions-silver
  Type: Spark, Glue 5.0, Python 3, worker G.1X
  Workers: 2, with "Automatically scale the number of workers" on, maximum 10
  Timeout: 30 min
  Job bookmark: ENABLE   (required - this is what makes it incremental)
  Paste this file into the script editor.
  Job parameters (optional; defaults below):
      --BRONZE_PATH  s3://autopulse-julio-leon/bronze/collisions
      --SILVER_PATH  s3://autopulse-julio-leon/silver/collisions

To rebuild all of Silver: delete silver/collisions/, then in the Glue console
choose the job -> Actions -> Reset job bookmark, and run it.

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

# --------------------------------------------------
# Job parameters
# --------------------------------------------------

DEFAULTS = {
    "BRONZE_PATH": "s3://autopulse-julio-leon/bronze/collisions",
    "SILVER_PATH": "s3://autopulse-julio-leon/silver/collisions",
}


def read_job_parameters():
    """JOB_NAME is always passed by Glue; the paths are optional (defaults above)."""
    passed = [name for name in DEFAULTS if f"--{name}" in sys.argv]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", *passed])
    return {**DEFAULTS, **args}


# Columns every Silver row must have (missing ones are added as empty).
EXPECTED_COLUMNS = [
    "collision_id", "crash_date", "crash_time", "borough", "zip_code",
    "latitude", "longitude", "on_street_name", "cross_street_name", "off_street_name",
    "number_of_persons_injured", "number_of_persons_killed",
    "number_of_pedestrians_injured", "number_of_pedestrians_killed",
    "number_of_cyclist_injured", "number_of_cyclist_killed",
    "number_of_motorist_injured", "number_of_motorist_killed",
    "contributing_factor_vehicle_1", "contributing_factor_vehicle_2",
    "vehicle_type_code1", "vehicle_type_code2",
]
COUNT_COLUMNS = [c for c in EXPECTED_COLUMNS if c.startswith("number_of_")]

# New York City bounding box.
NYC_LATITUDE = (40.49, 40.92)
NYC_LONGITUDE = (-74.27, -73.68)


# --------------------------------------------------
# Transformation
# --------------------------------------------------

def transform(bronze):
    df = bronze

    # 1. Every expected column exists.
    for column in EXPECTED_COLUMNS:
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast("string"))

    # The Silver folder this row belongs to: "2026-06-01" from "2026-06-01T00:00:00.000".
    df = df.withColumn("crash_date_part", F.substring("crash_date", 1, 10))

    # 2. Types.
    df = (
        df.withColumn("collision_id", F.col("collision_id").cast("long"))
        .withColumn("crash_date", F.to_timestamp("crash_date"))
        .withColumn("crash_time", F.trim("crash_time"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
    )
    for column in COUNT_COLUMNS:
        df = df.withColumn(column, F.col(column).cast("int"))

    # 3. A collision needs an ID and a crash date (to know which folder it goes in).
    df = df.filter(F.col("collision_id").isNotNull() & F.col("crash_date").isNotNull())

    # 4. One row per collision: the most recently ingested copy.
    newest_first = Window.partitionBy("collision_id").orderBy(F.col("ingested_at").desc_nulls_last())
    df = (df.withColumn("_rank", F.row_number().over(newest_first))
            .filter(F.col("_rank") == 1)
            .drop("_rank"))

    # 5. Borough.
    borough = F.upper(F.trim("borough"))
    df = (df.withColumn("borough_known", borough.isNotNull() & (borough != ""))
            .withColumn("borough", F.when(F.col("borough_known"), borough).otherwise(F.lit("UNKNOWN"))))

    # 6. Coordinates inside New York City.
    df = df.withColumn(
        "coordinates_valid",
        F.coalesce(
            F.col("latitude").between(*NYC_LATITUDE) & F.col("longitude").between(*NYC_LONGITUDE),
            F.lit(False),
        ),
    )
    return df


# --------------------------------------------------
# Job
# --------------------------------------------------

def main():
    args = read_job_parameters()
    silver_folder = args["SILVER_PATH"].rstrip("/")

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    print(f"Bronze: {args['BRONZE_PATH']}\nSilver: {silver_folder}")

    # Only Bronze files that are new since the last run (job bookmark, keyed by transformation_ctx).
    # DynamicFrames also combine files whose columns differ (a field missing on some dates).
    bronze_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [args["BRONZE_PATH"]], "recurse": True},
        format="parquet",
        transformation_ctx="bronze_collisions",
    )
    bronze = bronze_frame.toDF()

    if bronze.isEmpty():
        print("No new Bronze files since the last run.")
        job.commit()
        return

    silver = transform(bronze).cache()

    # The crash dates this run touches (a small list: one entry per date).
    changed_dates = sorted(row.crash_date_part for row in silver.select("crash_date_part").distinct().collect())
    print(f"Crash dates to refresh: {len(changed_dates)} ({changed_dates[0]} .. {changed_dates[-1]})")

    # Replace those dates only: empty each date's folder (retentionPeriod 0 = delete
    # every file, not only files older than the default 7 days)...
    for crash_day in changed_dates:
        glue_context.purge_s3_path(f"{silver_folder}/crash_date_part={crash_day}/", {"retentionPeriod": 0})

    # ...then write all of them in one go: one file per date, dates written in parallel.
    glue_context.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(silver.repartition("crash_date_part"), glue_context, "silver_collisions"),
        connection_type="s3",
        connection_options={"path": silver_folder + "/", "partitionKeys": ["crash_date_part"]},
        format="parquet",
        format_options={"useGlueParquetWriter": True, "compression": "snappy"},
    )
    print(f"Silver written for {len(changed_dates)} crash dates.")

    # Save the bookmark only after the writes succeeded, so a failed run is retried in full.
    job.commit()


main()
