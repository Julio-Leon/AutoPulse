"""
AutoPulse - AWS Glue job: NYC Motor Vehicle Collisions API -> S3 Bronze.

Pulls the collisions for a date window from NYC Open Data and writes them to
Bronze, one folder (and one Parquet file) per crash date:

    s3://autopulse-julio-leon/bronze/collisions/crash_date_part=2026-06-01/<file>.parquet

Every API field is stored as text (the API leaves out empty fields, so storing
text keeps the columns identical from run to run), plus `ingested_at`.
Re-running a window replaces those dates' files, so running twice never
duplicates data, and dates outside the window are never touched.

The city publishes with a delay of weeks to months, so by default the window
is "the latest 30 days that exist in the dataset", not "the last 30 days".

How it scales:
  1. On the driver: find the window and ask the API how many rows it has.
  2. Split that into pages of 50,000 rows (the API's maximum per request) and
     fetch ALL pages at the same time, spread across the Glue workers.
  3. Write one file per crash date, dates written in parallel.
  No time or memory limit: a multi-year backfill is one run.

--- Glue job setup (console) ---
  Name: autopulse-collisions-ingestion
  Type: Spark, Glue 5.0, Python 3, worker G.1X
  Workers: 2, with "Automatically scale the number of workers" on, maximum 10
  Timeout: 60 min
  Job bookmark: Disable   (the source is an API, not S3 files)
  Paste this file into the script editor.
  Job parameters (all optional):
      --BRONZE_PATH  s3://autopulse-julio-leon/bronze/collisions
      --DAYS_BACK    30                    window length when START_DATE is not given
      --START_DATE   2025-06-01            fixed window (e.g. a backfill) ...
      --END_DATE     2026-06-11            ... default end: latest date in the dataset
      --APP_TOKEN    <token>               NYC Open Data app token (higher API limits)

Runs in AWS Glue only (the awsglue library exists only there).
"""

import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, timedelta

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# --------------------------------------------------
# Job parameters
# --------------------------------------------------

DEFAULTS = {
    "BRONZE_PATH": "s3://autopulse-julio-leon/bronze/collisions",
    "DAYS_BACK": "30",
    "START_DATE": "",
    "END_DATE": "",
    "APP_TOKEN": "",
}


def read_job_parameters():
    """JOB_NAME is always passed by Glue; everything else is optional (defaults above)."""
    passed = [name for name in DEFAULTS if f"--{name}" in sys.argv]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", *passed])
    return {**DEFAULTS, **args}


API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
PAGE_SIZE = 50_000      # the API's maximum rows per request
MAX_RETRIES = 3


# --------------------------------------------------
# NYC Open Data API (runs on the driver and on the workers)
# --------------------------------------------------

def call_api(params, app_token):
    """GET the API with query parameters; retry a few times on errors."""
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    headers = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=120) as response:
                return json.loads(response.read())
        except Exception as error:
            if attempt == MAX_RETRIES:
                raise
            print(f"API call failed (attempt {attempt}): {error}; retrying")
            time.sleep(2 ** attempt)


def window_filter(start_date, end_date):
    return f"crash_date between '{start_date}T00:00:00' and '{end_date}T23:59:59'"


def fetch_page(offset, where, app_token):
    """
    One page of collisions, as JSON text lines with every value as text
    (nested objects such as `location` become JSON text too).
    """
    page = call_api({"$where": where, "$order": "collision_id",
                     "$limit": PAGE_SIZE, "$offset": offset}, app_token)
    return [
        json.dumps({key: (json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else str(value))
                    for key, value in record.items() if value is not None})
        for record in page
    ]


# --------------------------------------------------
# Job
# --------------------------------------------------

def main():
    args = read_job_parameters()
    bronze_folder = args["BRONZE_PATH"].rstrip("/")
    app_token = args["APP_TOKEN"] or None

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    # 1. The window, and how many rows it holds.
    if args["END_DATE"]:
        end_date = date.fromisoformat(args["END_DATE"])
    else:
        latest = call_api({"$select": "max(crash_date) AS latest"}, app_token)
        end_date = date.fromisoformat(latest[0]["latest"][:10])
    start_date = (date.fromisoformat(args["START_DATE"]) if args["START_DATE"]
                  else end_date - timedelta(days=int(args["DAYS_BACK"])))

    where = window_filter(start_date, end_date)
    total_rows = int(call_api({"$select": "count(*) AS total", "$where": where}, app_token)[0]["total"])
    page_offsets = list(range(0, total_rows, PAGE_SIZE))
    print(f"Window {start_date} to {end_date}: {total_rows} rows in {len(page_offsets)} page(s)")

    if total_rows == 0:
        print("Nothing to ingest.")
        job.commit()
        return

    # 2. Fetch every page at the same time, one page per task across the workers.
    json_lines = (
        spark.sparkContext
        .parallelize(page_offsets, numSlices=len(page_offsets))
        .flatMap(lambda offset: fetch_page(offset, where, app_token))
    )
    # primitivesAsString: keep every value as text (no type guessing in Bronze).
    records = (
        spark.read.option("primitivesAsString", "true").json(json_lines)
        .withColumn("crash_date_part", F.substring("crash_date", 1, 10))
        .withColumn("ingested_at", F.current_timestamp())
        .filter(F.col("crash_date_part").isNotNull())
        .cache()
    )

    # 3. Replace exactly the crash dates in this window: empty each date's folder
    #    (retentionPeriod 0 = delete every file, not only files older than 7 days)...
    dates = sorted(r.crash_date_part for r in records.select("crash_date_part").distinct().collect())
    for crash_day in dates:
        glue_context.purge_s3_path(f"{bronze_folder}/crash_date_part={crash_day}/", {"retentionPeriod": 0})

    # ...then write them all: one file per date, dates written in parallel.
    glue_context.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(records.repartition("crash_date_part"), glue_context, "bronze_collisions"),
        connection_type="s3",
        connection_options={"path": bronze_folder + "/", "partitionKeys": ["crash_date_part"]},
        format="parquet",
        format_options={"useGlueParquetWriter": True, "compression": "snappy"},
    )
    print(f"Wrote {total_rows} rows to {len(dates)} crash dates ({dates[0]} .. {dates[-1]})")

    job.commit()


main()
