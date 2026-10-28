"""
AutoPulse - data lake validation: did the pipeline move the data correctly?

Checks the S3 layers against each other (Bronze -> Silver -> Gold) by reading
only each Parquet file's footer: every Parquet file stores its own row count
and column names at its end, so only the end of each file is downloaded (our
files are small, usually the whole file) and the checks take seconds.

Telemetry (one UTC day, default today):
  telemetry_bronze_fully_processed   Silver + quarantine rows = the Bronze files the
                                     Silver job has read (the oldest N files of the day;
                                     newer ones are just waiting for the next run)
  telemetry_gold_matches_silver      Gold fact rows = Silver rows
  telemetry_quarantine_rate          rejected rows are under 1%
  telemetry_stream_is_fresh          during working hours, a Bronze file arrived in the
                                     last 15 minutes (else the EC2 stream has stopped)
  dim_vehicle_complete               dim_vehicle is one file with 10 vehicles
Collisions (every crash date):
  collisions_dates_in_every_layer    each date exists in Bronze, Silver and Gold
  collisions_one_file_per_date       each date folder holds exactly one file
                                     (two files = rows counted twice)
  collisions_silver_not_above_bronze Silver never has more rows than Bronze
  collisions_gold_matches_silver     Gold rows = Silver rows, date by date
  collisions_rows_dropped_in_silver  rows Silver dropped (no ID/date, duplicates): warn above 1%
Dimensions: dim_date and dim_location have their expected row counts.
Columns: the newest file of every table has the columns the next step expects.

Each check ends PASS, WARN, FAIL or SKIP (nothing to check). Any FAIL makes
the run fail: exit code 1 on a laptop; in Airflow the validate_data_lake task
fails and the DAG run turns red.

--- Run from your laptop (project root, normal AWS login) ---
    python validation/data_lake/validate_data_lake.py                 # both pipelines, today
    python validation/data_lake/validate_data_lake.py --pipeline telemetry --date 2026-10-26

--- Run in Airflow (last task of each pipeline DAG, see orchestration/airflow_dags/) ---
  autopulse_common.validate_data_lake() calls run(pipeline, cutoff=<DAG run start>):
  every Bronze file written before the run started must have been processed by it.
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
from botocore.config import Config
import pyarrow as pa
import pyarrow.parquet as pq

# --------------------------------------------------
# Settings
# --------------------------------------------------

BUCKET = "autopulse-julio-leon"
REGION = "us-east-1"
NEW_YORK = ZoneInfo("America/New_York")

MAX_QUARANTINE_RATE = 0.01                    # 1% of telemetry rows rejected
MAX_SILVER_DROP_RATE = 0.01                   # 1% of collision rows dropped by Silver
MAX_BRONZE_TELEMETRY_AGE = timedelta(minutes=15)
MAX_UNPROCESSED_AGE = timedelta(minutes=30)   # Bronze waiting longer than this: Silver isn't running
FLEET_SIZE = 10
DIM_DATE_FIRST, DIM_DATE_LAST = date(2012, 7, 1), date(2030, 12, 31)
DIM_LOCATION_CELLS = 96 * 100

# Columns each table's files must have (the next step, or Snowflake, reads them).
EXPECTED_COLUMNS = {
    "bronze/vehicle_telemetry/": {"raw_payload", "kafka_partition", "kafka_offset", "ingested_at"},
    "silver/vehicle_telemetry/": {"event_id", "vehicle_id", "driver_name", "make", "model", "model_year",
                                  "event_timestamp", "latitude", "longitude", "speed_mph",
                                  "odometer_miles", "fuel_level_pct", "engine_status", "ingested_at"},
    "gold/fact_vehicle_telemetry/": {"event_id", "vehicle_id", "date_key", "event_timestamp",
                                     "event_timestamp_local", "event_hour_local", "cell_id", "latitude",
                                     "longitude", "speed_mph", "is_speeding", "odometer_miles",
                                     "fuel_level_pct", "engine_status"},
    "gold/dim_vehicle/": {"vehicle_id", "driver_name", "make", "model", "model_year",
                          "first_seen_at", "last_seen_at", "updated_at"},
    "bronze/collisions/": {"collision_id", "crash_date", "ingested_at"},
    "silver/collisions/": {"collision_id", "crash_date", "crash_time", "borough", "latitude", "longitude",
                           "coordinates_valid", "number_of_persons_injured", "number_of_persons_killed",
                           "ingested_at"},
    "gold/fact_collision/": {"collision_id", "date_key", "crash_timestamp_local", "crash_hour", "cell_id",
                             "borough", "number_of_persons_injured", "number_of_persons_killed",
                             "contributing_factor", "vehicle_type"},
    "gold/dim_date/": {"date_key", "full_date", "year", "month", "day_of_week", "is_weekend"},
    "gold/dim_location/": {"cell_id", "center_latitude", "center_longitude",
                           "south_latitude", "north_latitude", "west_longitude", "east_longitude"},
}

# Bytes downloaded from the end of each file: enough for the footer of any of our
# files (and the whole file for most of them, which are well under 1 MB).
FOOTER_FETCH_BYTES = 1024 * 1024

# 32 connections: one per reading thread (boto3 keeps only 10 by default).
s3 = boto3.client("s3", region_name=REGION, config=Config(max_pool_connections=32))


# --------------------------------------------------
# Reading the lake (file listings and Parquet footers only)
# --------------------------------------------------

@dataclass
class ParquetFile:
    key: str
    modified: datetime          # when S3 received it (UTC)
    partition: str | None       # e.g. "2026-06-01" for .../crash_date_part=2026-06-01/<file>
    rows: int = 0
    columns: frozenset = frozenset()


def list_parquet(prefix):
    """Every .parquet file under a prefix, oldest first (skips Spark's _spark_metadata log)."""
    files = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet") or "_spark_metadata" in key:
                continue
            folder = key[len(prefix):].split("/")[0]
            partition = folder.split("=", 1)[1] if "=" in folder else None
            files.append(ParquetFile(key, obj["LastModified"], partition))
    return sorted(files, key=lambda f: f.modified)


def read_footer(key):
    """
    A Parquet file's footer (row count, columns), downloading only the end of the file.

    A Parquet file ends with: <footer> <footer length: 4 bytes> "PAR1".
    boto3 downloads the last 1 MB (the whole file if it's smaller); pyarrow then
    reads the footer from memory. (Done with boto3 so it works with any pyarrow
    build, including ones without S3 support.)
    """
    response = s3.get_object(Bucket=BUCKET, Key=key, Range=f"bytes=-{FOOTER_FETCH_BYTES}")
    tail = response["Body"].read()
    file_size = int(response.get("ContentRange", "/0").split("/")[-1]) or len(tail)

    if len(tail) >= file_size:                          # we have the whole file
        return pq.read_metadata(pa.BufferReader(tail))

    footer_length = int.from_bytes(tail[-8:-4], "little")
    if footer_length + 8 > len(tail):                   # unusually large footer: fetch it all
        tail = s3.get_object(Bucket=BUCKET, Key=key, Range=f"bytes=-{footer_length + 8}")["Body"].read()
    # Footer + length + "PAR1", with "PAR1" in front: the smallest valid Parquet
    # "file" holding just the footer.
    return pq.read_metadata(pa.BufferReader(b"PAR1" + tail[-(footer_length + 8):]))


def read_footers(files):
    """Fill in rows and columns from each file's footer, 32 files at a time."""
    def read(f):
        metadata = read_footer(f.key)
        f.rows, f.columns = metadata.num_rows, frozenset(metadata.schema.names)
    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(read, files))
    return files


def total_rows(files):
    return sum(f.rows for f in files)


# --------------------------------------------------
# Results
# --------------------------------------------------

results = []


def record(check, status, detail):
    results.append({"check": check, "status": status, "detail": detail})


def check_columns(table_prefix, files):
    """The newest file has every expected column."""
    if not files:
        return
    missing = EXPECTED_COLUMNS[table_prefix] - files[-1].columns
    name = "columns_" + table_prefix.strip("/").replace("/", "_")
    if missing:
        record(name, "FAIL", f"newest file {files[-1].key} is missing {sorted(missing)}")
    else:
        record(name, "PASS", f"{len(EXPECTED_COLUMNS[table_prefix])} expected columns present")


# --------------------------------------------------
# Telemetry
# --------------------------------------------------

def validate_telemetry(day, cutoff, now):
    bronze_all = list_parquet("bronze/vehicle_telemetry/")
    bronze = read_footers([f for f in bronze_all if f.modified.date() == day])
    silver = read_footers(list_parquet(f"silver/vehicle_telemetry/event_date={day}/"))
    quarantine = read_footers(list_parquet(f"quarantine/vehicle_telemetry/ingest_date={day}/"))
    gold = read_footers(list_parquet(f"gold/fact_vehicle_telemetry/event_date_local={day}/"))

    # Freshness: only while the fleet drives (Mon-Fri 8:00-17:00 New York, plus 10 min slack).
    local = now.astimezone(NEW_YORK)
    driving = local.weekday() < 5 and (8, 10) <= (local.hour, local.minute) < (17, 5)
    if not driving:
        record("telemetry_stream_is_fresh", "SKIP", "outside working hours (Mon-Fri 8:00-17:00 New York)")
    elif not bronze_all or now - bronze_all[-1].modified > MAX_BRONZE_TELEMETRY_AGE:
        age = "never" if not bronze_all else f"{(now - bronze_all[-1].modified).seconds // 60} min ago"
        record("telemetry_stream_is_fresh", "FAIL",
               f"newest Bronze telemetry file arrived {age}: check the EC2 streaming host")
    else:
        record("telemetry_stream_is_fresh", "PASS",
               f"newest Bronze file {(now - bronze_all[-1].modified).seconds // 60} min old")

    if not bronze:
        record("telemetry_bronze_fully_processed", "SKIP", f"no Bronze telemetry written on {day} (UTC)")
        return

    # Bronze -> Silver + quarantine. Glue's bookmark processes every file that existed
    # when the Silver job started, so the processed files are always the OLDEST ones of
    # the day: Silver + quarantine must equal the rows of the oldest N Bronze files.
    processed = total_rows(silver) + total_rows(quarantine)
    running, prefix_sums = 0, {0: 0}
    for count, f in enumerate(bronze, start=1):
        running += f.rows
        prefix_sums[running] = count
    files_processed = prefix_sums.get(processed)
    must_be_processed = sum(1 for f in bronze if cutoff and f.modified < cutoff)

    if files_processed is None:
        record("telemetry_bronze_fully_processed", "FAIL",
               f"Silver+quarantine has {processed} rows, which matches no set of Bronze files "
               f"({total_rows(bronze)} rows in {len(bronze)} files): rows were lost or duplicated")
    elif files_processed < must_be_processed:
        record("telemetry_bronze_fully_processed", "FAIL",
               f"only {files_processed} of the {must_be_processed} Bronze files written before this run "
               f"reached Silver")
    else:
        waiting = bronze[files_processed:]
        stale = [f for f in waiting if now - f.modified > MAX_UNPROCESSED_AGE]
        summary = (f"{processed} rows from {files_processed} Bronze files are in Silver/quarantine"
                   + (f"; {len(waiting)} newer file(s) wait for the next run" if waiting else ""))
        if stale:
            record("telemetry_bronze_fully_processed", "WARN",
                   summary + f" ({len(stale)} waiting over {MAX_UNPROCESSED_AGE.seconds // 60} min: "
                             f"is the telemetry pipeline running?)")
        else:
            record("telemetry_bronze_fully_processed", "PASS", summary)

    # Quarantine rate.
    if processed:
        rate = total_rows(quarantine) / processed
        record("telemetry_quarantine_rate", "PASS" if rate <= MAX_QUARANTINE_RATE else "FAIL",
               f"{total_rows(quarantine)} of {processed} rows rejected ({rate:.2%}, limit {MAX_QUARANTINE_RATE:.0%})")

    # Silver -> Gold.
    difference = total_rows(silver) - total_rows(gold)
    if difference == 0:
        record("telemetry_gold_matches_silver", "PASS", f"{total_rows(gold)} rows in both")
    elif difference > 0 and not cutoff:
        record("telemetry_gold_matches_silver", "WARN",
               f"Gold is {difference} rows behind Silver (a pipeline run may be in progress)")
    else:
        record("telemetry_gold_matches_silver", "FAIL",
               f"Silver {total_rows(silver)} rows vs Gold {total_rows(gold)} rows")

    # dim_vehicle.
    vehicles = read_footers(list_parquet("gold/dim_vehicle/"))
    if len(vehicles) != 1:
        record("dim_vehicle_complete", "FAIL", f"{len(vehicles)} files in gold/dim_vehicle/ (expected 1)")
    else:
        record("dim_vehicle_complete", "PASS" if vehicles[0].rows == FLEET_SIZE else "WARN",
               f"{vehicles[0].rows} vehicles (fleet size {FLEET_SIZE})")

    for prefix, files in [("bronze/vehicle_telemetry/", bronze), ("silver/vehicle_telemetry/", silver),
                          ("gold/fact_vehicle_telemetry/", gold), ("gold/dim_vehicle/", vehicles)]:
        check_columns(prefix, files)


# --------------------------------------------------
# Collisions and dimensions
# --------------------------------------------------

def by_date(files):
    dates = {}
    for f in files:
        dates.setdefault(f.partition, []).append(f)
    return dates


def validate_collisions():
    bronze = read_footers(list_parquet("bronze/collisions/"))
    silver = read_footers(list_parquet("silver/collisions/"))
    gold = read_footers(list_parquet("gold/fact_collision/"))
    if not bronze:
        record("collisions_dates_in_every_layer", "SKIP", "no Bronze collisions yet")
        return
    b, s, g = by_date(bronze), by_date(silver), by_date(gold)

    missing = {"Silver": sorted(set(b) - set(s)), "Gold": sorted(set(s) - set(g))}
    if any(missing.values()):
        record("collisions_dates_in_every_layer", "FAIL",
               "; ".join(f"{len(d)} dates missing in {layer} (e.g. {d[:3]})" for layer, d in missing.items() if d))
    else:
        record("collisions_dates_in_every_layer", "PASS", f"{len(b)} crash dates in Bronze, Silver and Gold")

    extra = {layer: sorted(d for d, fs_ in dates.items() if len(fs_) > 1)
             for layer, dates in [("Bronze", b), ("Silver", s), ("Gold", g)]}
    if any(extra.values()):
        record("collisions_one_file_per_date", "FAIL",
               "; ".join(f"{layer}: {d[:3]}" for layer, d in extra.items() if d) + " have more than one file")
    else:
        record("collisions_one_file_per_date", "PASS", "every date folder holds one file")

    above = [d for d in s if d in b and total_rows(s[d]) > total_rows(b[d])]
    record("collisions_silver_not_above_bronze", "FAIL" if above else "PASS",
           f"Silver has more rows than Bronze on {above[:5]}" if above else "Silver <= Bronze on every date")

    mismatched = [d for d in g if d in s and total_rows(g[d]) != total_rows(s[d])]
    record("collisions_gold_matches_silver", "FAIL" if mismatched else "PASS",
           f"rows differ on {len(mismatched)} dates, e.g. {mismatched[:5]}" if mismatched
           else f"{total_rows(gold)} rows in both")

    dropped = total_rows(bronze) - total_rows(silver)
    rate = dropped / total_rows(bronze)
    record("collisions_rows_dropped_in_silver", "PASS" if rate <= MAX_SILVER_DROP_RATE else "WARN",
           f"{dropped} of {total_rows(bronze)} rows dropped (missing ID/date or duplicates, {rate:.2%})")

    for prefix, files in [("bronze/collisions/", bronze), ("silver/collisions/", silver),
                          ("gold/fact_collision/", gold)]:
        check_columns(prefix, files)


def validate_dimensions():
    expected = {"gold/dim_date/": (DIM_DATE_LAST - DIM_DATE_FIRST).days + 1,
                "gold/dim_location/": DIM_LOCATION_CELLS}
    for prefix, rows in expected.items():
        files = read_footers(list_parquet(prefix))
        name = prefix.strip("/").split("/")[1] + "_complete"
        if not files:
            record(name, "FAIL", f"no files in {prefix} (run autopulse-gold-dimensions)")
        else:
            record(name, "PASS" if total_rows(files) == rows and len(files) == 1 else "FAIL",
                   f"{total_rows(files)} rows in {len(files)} file(s) (expected {rows} in 1)")
            check_columns(prefix, files)


# --------------------------------------------------
# Entry points
# --------------------------------------------------

def run(pipeline, day=None, cutoff=None):
    results.clear()
    now = datetime.now(timezone.utc)
    if pipeline in ("telemetry", "all"):
        validate_telemetry(day or now.date(), cutoff, now)
    if pipeline in ("collisions", "all"):
        validate_collisions()
        validate_dimensions()
    failures = [r for r in results if r["status"] == "FAIL"]
    return {"pipeline": pipeline, "checked_at": now.isoformat(), "passed": not failures,
            "failures": failures, "results": list(results)}


def main():
    parser = argparse.ArgumentParser(description="Validate the AutoPulse data lake layers.")
    parser.add_argument("--pipeline", choices=["telemetry", "collisions", "all"], default="all")
    parser.add_argument("--date", type=date.fromisoformat, help="telemetry day (UTC), default today")
    args = parser.parse_args()

    report = run(args.pipeline, day=args.date)
    for r in report["results"]:
        print(f"{r['status']:<5} {r['check']:<38} {r['detail']}")
    print("\nRESULT:", "PASSED" if report["passed"] else f"FAILED ({len(report['failures'])} check(s))")
    sys.exit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
