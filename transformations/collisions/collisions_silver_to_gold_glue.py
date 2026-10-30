"""
AutoPulse - AWS Glue job: collisions Silver -> Gold fact_collision (incremental, by crash date).

fact_collision has one row per collision, ready for the hotspot map:

    collision_id           the city's ID
    date_key               crash date as a number (20260611) -> dim_date
    crash_timestamp_local  crash date + time, New York time
    crash_hour             0-23, New York time
    cell_id                ~500 m grid cell -> dim_location (null if coordinates are invalid)
    borough, zip_code, latitude, longitude, coordinates_valid, on/cross street
    persons/pedestrians/cyclists/motorists injured and killed
    contributing_factor    main cause (vehicle 1), e.g. "Driver Inattention/Distraction"
    vehicle_type           vehicle 1's type, e.g. "Sedan"

Same pattern as the Silver job:
  1. The JOB BOOKMARK hands this job only the Silver files that are new since
     the last run. Silver writes each crash date as one complete file, so a new
     file = the full, current content of that date.
  2. Build the fact rows.
  3. For each crash date found: empty that date's Gold folder, then write.
       gold/fact_collision/crash_date_part=2026-06-01/<one file per date>
  4. job.commit() saves the bookmark (only after the writes succeed).

--- Glue job setup (console) ---
  Name: autopulse-collisions-gold
  Type: Spark, Glue 5.0, Python 3, worker G.1X
  Workers: 2, with "Automatically scale the number of workers" on, maximum 10
  Timeout: 30 min
  Job bookmark: ENABLE   (required - this is what makes it incremental)
  Paste this file into the script editor.
  Job parameters:
      --extra-py-files  s3://autopulse-julio-leon/deployment/autopulse/transformations/shared/nyc_grid.py   (required)
      --SILVER_PATH     s3://autopulse-julio-leon/silver/collisions        (optional)
      --GOLD_PATH       s3://autopulse-julio-leon/gold/fact_collision      (optional)

To rebuild all of it: delete gold/fact_collision/, reset the job bookmark, run.

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

import nyc_grid     # attached with --extra-py-files

# --------------------------------------------------
# Job parameters
# --------------------------------------------------

DEFAULTS = {
    "SILVER_PATH": "s3://autopulse-julio-leon/silver/collisions",
    "GOLD_PATH": "s3://autopulse-julio-leon/gold/fact_collision",
}


def read_job_parameters():
    """JOB_NAME is always passed by Glue; the paths are optional (defaults above)."""
    passed = [name for name in DEFAULTS if f"--{name}" in sys.argv]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", *passed])
    return {**DEFAULTS, **args}


INJURY_AND_DEATH_COLUMNS = [
    "number_of_persons_injured", "number_of_persons_killed",
    "number_of_pedestrians_injured", "number_of_pedestrians_killed",
    "number_of_cyclist_injured", "number_of_cyclist_killed",
    "number_of_motorist_injured", "number_of_motorist_killed",
]


# --------------------------------------------------
# Transformation
# --------------------------------------------------

def build_fact_collision(silver):
    # The city's crash_date is a local (New York) date; crash_time is "H:mm", e.g. "9:05".
    crash_day = F.date_format("crash_date", "yyyy-MM-dd")
    crash_time = F.lpad(F.trim("crash_time"), 5, "0")              # "9:05" -> "09:05"

    # One row per collision: if a run sees a collision twice, keep the newest copy.
    newest_first = Window.partitionBy("collision_id").orderBy(F.col("ingested_at").desc_nulls_last())
    latest = (silver.withColumn("_rank", F.row_number().over(newest_first))
                    .filter(F.col("_rank") == 1)
                    .drop("_rank"))

    return latest.select(
        "collision_id",
        F.date_format("crash_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.to_timestamp(F.concat_ws(" ", crash_day, crash_time), "yyyy-MM-dd HH:mm").alias("crash_timestamp_local"),
        F.split(crash_time, ":").getItem(0).cast("int").alias("crash_hour"),
        # Only crashes with valid New York coordinates are placed on the grid.
        F.when(F.col("coordinates_valid"), nyc_grid.cell_id_for("latitude", "longitude")).alias("cell_id"),
        "borough", "zip_code", "latitude", "longitude", "coordinates_valid",
        "on_street_name", "cross_street_name",
        *INJURY_AND_DEATH_COLUMNS,
        F.col("contributing_factor_vehicle_1").alias("contributing_factor"),
        F.col("vehicle_type_code1").alias("vehicle_type"),
        crash_day.alias("crash_date_part"),                         # Gold folder (not stored in the file)
    )


# --------------------------------------------------
# Job
# --------------------------------------------------

def main():
    args = read_job_parameters()
    gold_folder = args["GOLD_PATH"].rstrip("/")

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    print(f"Silver: {args['SILVER_PATH']}\nGold:   {gold_folder}")

    # Only Silver files that are new since the last run (job bookmark, keyed by transformation_ctx).
    silver = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [args["SILVER_PATH"]], "recurse": True},
        format="parquet",
        transformation_ctx="silver_collisions",
    ).toDF()

    if silver.isEmpty():
        print("No new Silver files since the last run.")
        job.commit()
        return

    fact = build_fact_collision(silver).cache()

    changed_dates = sorted(r.crash_date_part for r in fact.select("crash_date_part").distinct().collect())
    if not changed_dates:
        print("New Silver files, but no rows to load into fact_collision: nothing to write.")
        job.commit()
        return
    print(f"Crash dates to refresh: {len(changed_dates)} ({changed_dates[0]} .. {changed_dates[-1]})")

    # Replace those dates only: empty each date's folder (retentionPeriod 0 = every file)...
    for crash_day in changed_dates:
        glue_context.purge_s3_path(f"{gold_folder}/crash_date_part={crash_day}/", {"retentionPeriod": 0})

    # ...then write them all: one file per date, dates written in parallel.
    glue_context.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(fact.repartition("crash_date_part"), glue_context, "fact_collision"),
        connection_type="s3",
        connection_options={"path": gold_folder + "/", "partitionKeys": ["crash_date_part"]},
        format="parquet",
        format_options={"useGlueParquetWriter": True, "compression": "snappy"},
    )
    print(f"fact_collision written for {len(changed_dates)} crash dates.")

    job.commit()


main()
