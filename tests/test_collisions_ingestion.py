"""
Unit tests for the collisions API client and the Lambda ingestion logic.

No Spark, network, or AWS access needed: HTTP and S3 are replaced by fakes.
Run with:  pytest tests/test_collisions_ingestion.py -q
"""

from datetime import date

import pytest

from src.ingestion import collisions_api as api
from src.lambda_functions import collisions_ingestion as lam


# --------------------------------------------------
# Fakes
# --------------------------------------------------

def fake_api(total_records, latest="2026-06-30T00:00:00.000"):
    """HTTP fake: serves `total_records` collisions, paged by $offset/$limit."""
    calls = []

    def get_json(url, params, headers, timeout):
        calls.append(params)
        if "$select" in params:
            return [{"max_crash_date": latest}]
        offset, limit = params["$offset"], params["$limit"]
        return [
            {
                "collision_id": str(i),
                "crash_date": f"2026-06-{1 + i % 3:02d}T00:00:00.000",
                "crash_time": "9:05",
                "location": {"type": "Point", "coordinates": [-73.98, 40.75]},
            }
            for i in range(offset, min(offset + limit, total_records))
        ]

    return get_json, calls


class FakePaginator:
    def __init__(self, s3):
        self.s3 = s3

    def paginate(self, Bucket, Prefix):
        keys = [k for (b, k) in self.s3.objects if b == Bucket and k.startswith(Prefix)]
        yield {"Contents": [{"Key": k} for k in keys]} if keys else {}


class FakeS3:
    def __init__(self, existing=()):
        self.objects = {("bkt", key): b"old" for key in existing}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator(self)

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body

    def delete_objects(self, Bucket, Delete):
        for item in Delete["Objects"]:
            self.objects.pop((Bucket, item["Key"]), None)


# --------------------------------------------------
# API client
# --------------------------------------------------

def test_fetch_pages_through_all_records():
    get_json, calls = fake_api(250)
    records = api.fetch_collisions(date(2026, 6, 1), date(2026, 6, 30), get_json=get_json, page_size=100)
    assert len(records) == 250
    assert [(c["$offset"], c["$limit"]) for c in calls] == [(0, 100), (100, 100), (200, 100)]


def test_fetch_respects_max_records():
    get_json, _ = fake_api(250)
    records = api.fetch_collisions(date(2026, 6, 1), date(2026, 6, 30), max_records=120,
                                   get_json=get_json, page_size=100)
    assert len(records) == 120


def test_fetch_retries_then_raises(monkeypatch):
    monkeypatch.setattr(api.time, "sleep", lambda seconds: None)

    def always_fails(*args):
        raise OSError("network down")

    with pytest.raises(api.ApiError):
        api.fetch_collisions(date(2026, 6, 1), date(2026, 6, 30), get_json=always_fails)


def test_where_clause_is_inclusive():
    assert api.build_where_clause(date(2026, 6, 1), date(2026, 6, 30)) == (
        "crash_date between '2026-06-01T00:00:00' and '2026-06-30T23:59:59'"
    )


def test_latest_available_date_and_fallback():
    get_json, _ = fake_api(0, latest="2026-06-11T00:00:00.000")
    assert api.latest_available_crash_date(get_json=get_json) == date(2026, 6, 11)

    def broken(*args):
        return [{}]

    assert isinstance(api.latest_available_crash_date(get_json=broken), date)


def test_resolve_window_defaults_to_latest_date():
    start, end = api.resolve_window(None, None, 30, lambda: date(2026, 6, 30))
    assert (start, end) == (date(2026, 5, 31), date(2026, 6, 30))

    with pytest.raises(ValueError):
        api.resolve_window(date(2026, 7, 1), date(2026, 6, 1), 30, lambda: date(2026, 6, 30))


def test_normalize_records_uses_strings_and_all_columns():
    columns, rows = api.normalize_records([
        {"collision_id": 1, "location": {"type": "Point"}},
        {"collision_id": "2", "borough": "QUEENS"},
    ])
    assert columns == ["borough", "collision_id", "location"]
    assert rows == [(None, "1", '{"type": "Point"}'), ("QUEENS", "2", None)]


# --------------------------------------------------
# Lambda
# --------------------------------------------------

def test_group_rows_by_partition_handles_missing_dates():
    columns, partitions = lam.group_rows_by_partition([
        {"collision_id": "1", "crash_date": "2026-06-01T00:00:00.000"},
        {"collision_id": "2", "crash_date": "2026-06-02T00:00:00.000"},
        {"collision_id": "3", "crash_date": "2026-06-01T00:00:00.000"},
        {"collision_id": "4"},
    ])
    assert columns == ["collision_id", "crash_date"]
    assert {k: len(v) for k, v in partitions.items()} == {
        "2026-06-01": 2, "2026-06-02": 1, lam.NULL_PARTITION: 1,
    }


def test_replace_partition_writes_new_file_then_removes_old():
    s3 = FakeS3(existing=[
        "bronze/collisions/crash_date_part=2026-06-01/part-old.snappy.parquet",
        "bronze/collisions/crash_date_part=2026-06-02/part-keep.snappy.parquet",
    ])
    new_key = lam.replace_partition(s3, "bkt", "bronze/collisions/crash_date_part=2026-06-01/", b"new")

    keys = {k for (_, k) in s3.objects}
    assert new_key in keys
    assert "bronze/collisions/crash_date_part=2026-06-01/part-old.snappy.parquet" not in keys
    assert "bronze/collisions/crash_date_part=2026-06-02/part-keep.snappy.parquet" in keys


def test_handler_end_to_end_with_fakes(monkeypatch):
    get_json, _ = fake_api(30)
    monkeypatch.setattr(api, "http_get_json", get_json)
    monkeypatch.setattr(lam.config, "BRONZE_COLLISIONS", "s3://bkt/bronze/collisions")
    # pyarrow isn't needed for this test.
    monkeypatch.setattr(lam, "build_parquet", lambda columns, rows, ts: f"{len(rows)} rows".encode())

    s3 = FakeS3()
    result = lam.handler({"days_back": 5}, s3=s3)

    assert result["records"] == 30
    assert result["partitions"] == 3
    assert result["end_date"] == "2026-06-30"
    written = sorted(k for (_, k) in s3.objects)
    assert len(written) == 3
    assert all(k.startswith("bronze/collisions/crash_date_part=2026-06-0") for k in written)
