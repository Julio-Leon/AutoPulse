"""
AutoPulse - AWS Glue job: build the fixed Gold dimensions.

Two dimension tables that are generated, not read from any source:

    gold/dim_date/       one row per calendar day, 2012-07-01 to 2030-12-31
                         (NYC collisions data starts in July 2012)
    gold/dim_location/   one row per ~500 m grid cell of New York City (9,600 cells),
                         defined in transformations/shared/nyc_grid.py

Both facts point at them: fact_collision and fact_vehicle_telemetry have a
date_key (-> dim_date) and a cell_id (-> dim_location).

They never change, so this job runs once (and again only if the grid or the
date range is changed). Each run replaces both tables completely: one Parquet
file per table.

--- Glue job setup (console) ---
  Name: autopulse-gold-dimensions
  Type: Spark, Glue 5.0, Python 3, worker G.1X, 2 workers (no auto-scaling needed)
  Timeout: 15 min
  Job bookmark: Disable   (nothing is read, so there is nothing to remember)
  Paste this file into the script editor.
  Job parameters:
      --extra-py-files  s3://autopulse-julio-leon/deployment/autopulse/transformations/shared/nyc_grid.py   (required)
      --GOLD_PATH       s3://autopulse-julio-leon/gold                                                       (optional)

Runs in AWS Glue only (the awsglue library exists only there).
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

import nyc_grid     # attached with --extra-py-files

# --------------------------------------------------
# Job parameters
# --------------------------------------------------

DEFAULTS = {
    "GOLD_PATH": "s3://autopulse-julio-leon/gold",
}


def read_job_parameters():
    """JOB_NAME is always passed by Glue; GOLD_PATH is optional (default above)."""
    passed = [name for name in DEFAULTS if f"--{name}" in sys.argv]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", *passed])
    return {**DEFAULTS, **args}


FIRST_DATE = "2012-07-01"
LAST_DATE = "2030-12-31"


# --------------------------------------------------
# Dimensions
# --------------------------------------------------

def build_dim_date(spark):
    """One row per day. date_key is the date as a number: 2026-10-22 -> 20261022."""
    days = spark.sql(
        f"SELECT explode(sequence(DATE'{FIRST_DATE}', DATE'{LAST_DATE}', INTERVAL 1 DAY)) AS full_date"
    )
    day = F.col("full_date")
    return days.select(
        F.date_format(day, "yyyyMMdd").cast("int").alias("date_key"),
        day.alias("full_date"),
        F.year(day).alias("year"),
        F.quarter(day).alias("quarter"),
        F.month(day).alias("month"),
        F.date_format(day, "MMMM").alias("month_name"),
        F.dayofmonth(day).alias("day_of_month"),
        # Spark counts Sunday=1 ... Saturday=7; turn that into Monday=1 ... Sunday=7.
        ((F.dayofweek(day) + 5) % 7 + 1).alias("day_of_week"),
        F.date_format(day, "EEEE").alias("day_name"),
        F.dayofweek(day).isin(1, 7).alias("is_weekend"),
        F.weekofyear(day).alias("week_of_year"),
    )


def build_dim_location(spark):
    """One row per grid cell: cell_id, its row/column, corners and center."""
    return nyc_grid.all_cells(spark)


# --------------------------------------------------
# Writing
# --------------------------------------------------

def replace_table(glue_context, df, path, name):
    """Delete the table's old files, then write it as one Parquet file."""
    folder = path.rstrip("/") + "/"
    glue_context.purge_s3_path(folder, {"retentionPeriod": 0})   # 0 = delete every file
    glue_context.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(df.coalesce(1), glue_context, name),
        connection_type="s3",
        connection_options={"path": folder},
        format="parquet",
        format_options={"useGlueParquetWriter": True, "compression": "snappy"},
    )


# --------------------------------------------------
# Job
# --------------------------------------------------

def main():
    args = read_job_parameters()
    gold = args["GOLD_PATH"].rstrip("/")

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    dim_date = build_dim_date(spark)
    replace_table(glue_context, dim_date, f"{gold}/dim_date", "dim_date")
    print(f"dim_date: {dim_date.count()} days ({FIRST_DATE} .. {LAST_DATE})")

    dim_location = build_dim_location(spark)
    replace_table(glue_context, dim_location, f"{gold}/dim_location", "dim_location")
    print(f"dim_location: {nyc_grid.NUMBER_OF_ROWS * nyc_grid.NUMBER_OF_COLUMNS} cells "
          f"({nyc_grid.NUMBER_OF_ROWS} rows x {nyc_grid.NUMBER_OF_COLUMNS} columns)")

    job.commit()


main()
