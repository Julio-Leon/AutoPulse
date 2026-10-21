"""
AWS Lambda: NYC collisions API -> S3 Bronze.

Cloud version of `src/ingestion/nyc_collisions_api.py`. It uses the same API
client and writes the same Bronze layout, but uses pyarrow + boto3 instead
of Spark, so it runs in seconds on Lambda:

    s3://<bucket>/bronze/collisions/crash_date_part=YYYY-MM-DD/part-<uuid>.snappy.parquet

Each date partition in the window is replaced: the new file is written first,
then the partition's older files are deleted. Re-running a window is safe.

Runtime: Python 3.12 with the AWS-managed "AWSSDKPandas-Python312" layer
(provides pyarrow; boto3 is built into Lambda).

Event (all fields optional):
    {
      "start_date": "2026-05-01",   # inclusive
      "end_date":   "2026-06-30",   # inclusive; default = latest date in the dataset
      "days_back":  30,             # used when start_date is missing
      "max_records": 5000           # cap, for testing
    }

Environment:
    DATA_ROOT=s3://<bucket>   AUTOPULSE_RUNTIME=lambda   [NYC_OPEN_DATA_APP_TOKEN]
"""

from __future__ import annotations

import io
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone

from src.common import config
from src.ingestion import collisions_api as api

logger = config.setup_logging("collisions_ingestion_lambda.log", "autopulse.lambda.collisions")

# Spark's name for a null partition value, so Spark reads both layouts the same way.
NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


def _parse_date(value) -> date | None:
    return date.fromisoformat(str(value)) if value else None


def group_rows_by_partition(records: list[dict]) -> tuple[list[str], dict[str, list[tuple]]]:
    """Normalize records to string rows and group them by crash-date partition."""
    columns, rows = api.normalize_records(records)
    crash_date_index = columns.index("crash_date") if "crash_date" in columns else None

    partitions: dict[str, list[tuple]] = defaultdict(list)
    for row in rows:
        crash_date = row[crash_date_index] if crash_date_index is not None else None
        partitions[api.crash_date_partition(crash_date) or NULL_PARTITION].append(row)

    return columns, dict(partitions)


def build_parquet(columns: list[str], rows: list[tuple], ingested_at: datetime) -> bytes:
    """One Parquet file: every API column as string, plus ingested_at (UTC)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    arrays = {
        column: pa.array([row[i] for row in rows], type=pa.string())
        for i, column in enumerate(columns)
    }
    arrays["ingested_at"] = pa.array(
        [ingested_at] * len(rows), type=pa.timestamp("us", tz="UTC")
    )

    buffer = io.BytesIO()
    pq.write_table(pa.table(arrays), buffer, compression="snappy")
    return buffer.getvalue()


def replace_partition(s3, bucket: str, partition_prefix: str, body: bytes) -> str:
    """Write the new file, then delete the partition's previous files."""
    paginator = s3.get_paginator("list_objects_v2")
    old_keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=partition_prefix)
        for obj in page.get("Contents", [])
    ]

    new_key = f"{partition_prefix}part-00000-{uuid.uuid4()}.snappy.parquet"
    s3.put_object(Bucket=bucket, Key=new_key, Body=body)

    for start in range(0, len(old_keys), 1000):
        batch = old_keys[start:start + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )

    return new_key


def handler(event, context=None, s3=None):
    """Lambda entry point."""
    event = event or {}

    start_date, end_date = api.resolve_window(
        _parse_date(event.get("start_date")),
        _parse_date(event.get("end_date")),
        int(event.get("days_back", 30)),
        lambda: api.latest_available_crash_date(
            config.NYC_COLLISIONS_API_URL, config.NYC_OPEN_DATA_APP_TOKEN
        ),
    )
    max_records = event.get("max_records")
    max_records = int(max_records) if max_records else None

    logger.info("Collisions ingestion | %s", config.describe())
    logger.info("Crash-date window: %s to %s (max_records=%s)", start_date, end_date, max_records)

    records = api.fetch_collisions(
        start_date,
        end_date,
        max_records,
        api_url=config.NYC_COLLISIONS_API_URL,
        app_token=config.NYC_OPEN_DATA_APP_TOKEN,
    )

    result = {
        "status": "ok",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "records": len(records),
        "partitions": 0,
        "bronze_path": config.BRONZE_COLLISIONS,
    }

    if not records:
        logger.info("The API returned no records for this window. Nothing written.")
        return result

    columns, partitions = group_rows_by_partition(records)
    bucket, bronze_prefix = config.split_s3_uri(config.BRONZE_COLLISIONS)
    ingested_at = datetime.now(timezone.utc)

    if s3 is None:
        import boto3

        s3 = boto3.client("s3")

    for partition, rows in sorted(partitions.items()):
        partition_prefix = f"{bronze_prefix}/crash_date_part={partition}/"
        key = replace_partition(s3, bucket, partition_prefix, build_parquet(columns, rows, ingested_at))
        logger.info("Wrote %d rows to s3://%s/%s", len(rows), bucket, key)

    result["partitions"] = len(partitions)
    logger.info("Done: %s", result)
    return result
