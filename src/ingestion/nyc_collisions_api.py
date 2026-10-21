"""
NYC collisions API -> Bronze, run locally with Spark.

In the cloud, the same job runs as the Lambda function
`src/lambda_functions/collisions_ingestion.py`. Both use the shared API client
in `collisions_api.py` and write the same Bronze layout:

    bronze/collisions/crash_date_part=YYYY-MM-DD/*.parquet   (all columns as strings)

Re-running the same window replaces only those date partitions, so runs are
idempotent and safe to schedule.

Examples
--------
    # Latest 30 days of available data (default; the dataset lags behind today)
    python src/ingestion/nyc_collisions_api.py

    # Explicit window
    python src/ingestion/nyc_collisions_api.py --start-date 2026-05-01 --end-date 2026-06-30

    # Quick smoke test
    python src/ingestion/nyc_collisions_api.py --days-back 7 --max-records 1000
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

if __package__ in (None, ""):
    # Allow `python src/ingestion/nyc_collisions_api.py` as well as `python -m ...`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common import config
from src.ingestion import collisions_api as api

logger = config.setup_logging("ingestion.log", "autopulse.ingestion")


def ingest_collisions(
    start_date: date,
    end_date: date,
    max_records: int | None = None,
) -> int:
    """Fetch the window and write it to Bronze. Returns the record count."""
    logger.info("Starting collisions ingestion | %s", config.describe())
    logger.info("Crash-date window: %s to %s", start_date, end_date)

    records = api.fetch_collisions(
        start_date,
        end_date,
        max_records,
        api_url=config.NYC_COLLISIONS_API_URL,
        app_token=config.NYC_OPEN_DATA_APP_TOKEN,
    )

    if not records:
        logger.info("The API returned no records for this window. Nothing written.")
        return 0

    columns, rows = api.normalize_records(records)

    spark = config.build_spark("AutoPulse-NYC-Collisions-Ingestion", shuffle_partitions=12)

    try:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([StructField(c, StringType(), True) for c in columns])

        df = (
            spark.createDataFrame(rows, schema=schema)
            .withColumn("crash_date_part", F.substring("crash_date", 1, 10))
            .withColumn("ingested_at", F.current_timestamp())
        )

        # Only overwrite the date partitions present in this batch.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        (
            df.repartition("crash_date_part")
            .write
            .mode("overwrite")
            .partitionBy("crash_date_part")
            .parquet(config.BRONZE_COLLISIONS)
        )

        count = len(rows)
        logger.info("Wrote %d collision records to %s", count, config.BRONZE_COLLISIONS)
        return count

    finally:
        spark.stop()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest NYC collisions into Bronze.")
    parser.add_argument("--start-date", type=date.fromisoformat, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", type=date.fromisoformat, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--days-back", type=int, default=30,
                        help="Window size when --start-date is not given (default 30). "
                             "Without --end-date, the window ends at the latest date in the dataset.")
    parser.add_argument("--max-records", type=int, default=None,
                        help="Optional cap on records fetched (for quick tests)")
    args = parser.parse_args(argv)

    def latest() -> date:
        found = api.latest_available_crash_date(
            config.NYC_COLLISIONS_API_URL, config.NYC_OPEN_DATA_APP_TOKEN
        )
        logger.info("Latest crash date available in the dataset: %s", found)
        return found

    try:
        args.start_date, args.end_date = api.resolve_window(
            args.start_date, args.end_date, args.days_back, latest
        )
    except ValueError as error:
        parser.error(str(error))

    return args


if __name__ == "__main__":
    arguments = parse_args()
    ingest_collisions(arguments.start_date, arguments.end_date, arguments.max_records)
