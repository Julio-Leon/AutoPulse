"""
NYC collisions API -> Bronze.

Pulls collisions for a crash-date window from the NYC Open Data (Socrata) API,
paging through all results, and writes them to Bronze partitioned by
`crash_date_part` (yyyy-MM-dd).

Re-running the same window replaces only those date partitions (dynamic
partition overwrite), so runs are idempotent and safe to schedule in Airflow.

Examples
--------
    # Latest 30 days of available data (default; the dataset lags behind today)
    python src/ingestion/nyc_collisions_api.py

    # Explicit window
    python src/ingestion/nyc_collisions_api.py --start-date 2026-09-01 --end-date 2026-09-30

    # Quick smoke test
    python src/ingestion/nyc_collisions_api.py --days-back 7 --max-records 1000
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

if __package__ in (None, ""):
    # Allow `python src/ingestion/nyc_collisions_api.py` as well as `python -m ...`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import requests

from src.common import config

logger = config.setup_logging("ingestion.log", "autopulse.ingestion")

PAGE_SIZE = 50_000          # Socrata maximum per request
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3


def build_where_clause(start_date: date, end_date: date) -> str:
    """SoQL filter for an inclusive crash-date window."""
    return (
        f"crash_date between '{start_date.isoformat()}T00:00:00' "
        f"and '{end_date.isoformat()}T23:59:59'"
    )


def latest_available_crash_date(session: requests.Session | None = None) -> date:
    """
    Most recent crash_date in the dataset. The city publishes with a lag of
    weeks to months, so default windows are anchored here instead of today.
    Falls back to today if the lookup fails.
    """
    session = session or requests.Session()
    headers = {}
    if config.NYC_OPEN_DATA_APP_TOKEN:
        headers["X-App-Token"] = config.NYC_OPEN_DATA_APP_TOKEN

    try:
        response = session.get(
            config.NYC_COLLISIONS_API_URL,
            params={"$select": "max(crash_date) AS max_crash_date"},
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        value = response.json()[0]["max_crash_date"]
        return date.fromisoformat(value[:10])
    except (requests.RequestException, KeyError, IndexError, ValueError, TypeError):
        logger.warning("Could not look up the latest crash date; using today.")
        return datetime.now(timezone.utc).date()


def _to_string(value) -> str | None:
    """
    Bronze keeps every field as a string so the schema is identical across
    runs (Socrata omits empty fields, which would otherwise change inferred
    types). Nested objects such as `location` are kept as JSON text.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def fetch_collisions(
    start_date: date,
    end_date: date,
    max_records: int | None = None,
    session: requests.Session | None = None,
) -> list[dict]:
    """Fetch all collisions in the window, paging by $offset."""
    session = session or requests.Session()
    headers = {}
    if config.NYC_OPEN_DATA_APP_TOKEN:
        headers["X-App-Token"] = config.NYC_OPEN_DATA_APP_TOKEN

    where = build_where_clause(start_date, end_date)
    records: list[dict] = []
    offset = 0

    while True:
        limit = PAGE_SIZE
        if max_records is not None:
            limit = min(PAGE_SIZE, max_records - len(records))
            if limit <= 0:
                break

        params = {
            "$where": where,
            "$order": "collision_id",
            "$limit": limit,
            "$offset": offset,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = session.get(
                    config.NYC_COLLISIONS_API_URL,
                    params=params,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                page = response.json()
                break
            except requests.RequestException:
                if attempt == MAX_RETRIES:
                    raise
                wait = 2 ** attempt
                logger.warning("API request failed (attempt %d); retrying in %ds", attempt, wait)
                time.sleep(wait)

        logger.info("Fetched %d records at offset %d", len(page), offset)
        records.extend(page)

        if len(page) < limit:
            break
        offset += len(page)

    return records


def ingest_collisions(
    start_date: date,
    end_date: date,
    max_records: int | None = None,
) -> int:
    """Fetch the window and write it to Bronze. Returns the record count."""
    logger.info("Starting collisions ingestion | %s", config.describe())
    logger.info("Crash-date window: %s to %s", start_date, end_date)

    records = fetch_collisions(start_date, end_date, max_records)

    if not records:
        logger.info("The API returned no records for this window. Nothing written.")
        return 0

    # Normalize every record to the same set of string columns.
    columns = sorted({key for record in records for key in record})
    rows = [
        tuple(_to_string(record.get(column)) for column in columns)
        for record in records
    ]

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

    if args.end_date is None:
        args.end_date = latest_available_crash_date()
        logger.info("Latest crash date available in the dataset: %s", args.end_date)
    args.start_date = args.start_date or (args.end_date - timedelta(days=args.days_back))

    if args.start_date > args.end_date:
        parser.error("--start-date must be on or before --end-date")

    return args


if __name__ == "__main__":
    arguments = parse_args()
    ingest_collisions(arguments.start_date, arguments.end_date, arguments.max_records)
