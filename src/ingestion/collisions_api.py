"""
NYC Open Data (Socrata) client for the Motor Vehicle Collisions dataset.

Pure Python (standard library only) so the same code runs in three places:
the local Spark CLI (`nyc_collisions_api.py`), the AWS Lambda ingestion
function, and the unit tests. It has no file-logging or Spark side effects.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Callable

logger = logging.getLogger("autopulse.collisions_api")

DEFAULT_API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
PAGE_SIZE = 50_000          # Socrata maximum per request
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3

# Signature of the HTTP function: (url, params, headers, timeout) -> parsed JSON
HttpGetJson = Callable[[str, dict, dict, int], object]


class ApiError(RuntimeError):
    """The API could not be reached or returned an error after retries."""


def http_get_json(url: str, params: dict, headers: dict, timeout: int) -> object:
    """GET url?params and parse the JSON body (standard library only)."""
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{url}?{query}",
        headers={"Accept": "application/json", **headers},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _headers(app_token: str | None) -> dict:
    return {"X-App-Token": app_token} if app_token else {}


def build_where_clause(start_date: date, end_date: date) -> str:
    """SoQL filter for an inclusive crash-date window."""
    return (
        f"crash_date between '{start_date.isoformat()}T00:00:00' "
        f"and '{end_date.isoformat()}T23:59:59'"
    )


def latest_available_crash_date(
    api_url: str = DEFAULT_API_URL,
    app_token: str | None = None,
    get_json: HttpGetJson | None = None,
) -> date:
    """
    Most recent crash_date in the dataset. The city publishes with a lag of
    weeks to months, so default windows are anchored here instead of today.
    Falls back to today if the lookup fails.
    """
    get_json = get_json or http_get_json
    try:
        result = get_json(
            api_url,
            {"$select": "max(crash_date) AS max_crash_date"},
            _headers(app_token),
            REQUEST_TIMEOUT_SECONDS,
        )
        return date.fromisoformat(result[0]["max_crash_date"][:10])
    except (OSError, urllib.error.URLError, KeyError, IndexError, ValueError, TypeError):
        logger.warning("Could not look up the latest crash date; using today.")
        return datetime.now(timezone.utc).date()


def resolve_window(
    start_date: date | None,
    end_date: date | None,
    days_back: int,
    latest: Callable[[], date],
) -> tuple[date, date]:
    """
    Fill in a missing window: without end_date, end at the latest available
    crash date; without start_date, go back `days_back` days from the end.
    """
    end = end_date or latest()
    start = start_date or (end - timedelta(days=days_back))
    if start > end:
        raise ValueError("start_date must be on or before end_date")
    return start, end


def fetch_collisions(
    start_date: date,
    end_date: date,
    max_records: int | None = None,
    api_url: str = DEFAULT_API_URL,
    app_token: str | None = None,
    get_json: HttpGetJson | None = None,
    page_size: int = PAGE_SIZE,
) -> list[dict]:
    """Fetch every collision in the window, paging with $offset."""
    get_json = get_json or http_get_json
    where = build_where_clause(start_date, end_date)
    records: list[dict] = []
    offset = 0

    while True:
        limit = page_size
        if max_records is not None:
            limit = min(page_size, max_records - len(records))
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
                page = get_json(api_url, params, _headers(app_token), REQUEST_TIMEOUT_SECONDS)
                break
            except (OSError, urllib.error.URLError, ValueError) as error:
                if attempt == MAX_RETRIES:
                    raise ApiError(f"NYC collisions API failed after {MAX_RETRIES} attempts") from error
                wait = 2 ** attempt
                logger.warning("API request failed (attempt %d): %s; retrying in %ds", attempt, error, wait)
                time.sleep(wait)

        logger.info("Fetched %d records at offset %d", len(page), offset)
        records.extend(page)

        if len(page) < limit:
            break
        offset += len(page)

    return records


def to_string(value) -> str | None:
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


def normalize_records(records: list[dict]) -> tuple[list[str], list[tuple]]:
    """Return (sorted column names, rows of strings) covering every field seen."""
    columns = sorted({key for record in records for key in record})
    rows = [tuple(to_string(record.get(column)) for column in columns) for record in records]
    return columns, rows


def crash_date_partition(crash_date: str | None) -> str | None:
    """'2026-06-01T00:00:00.000' -> '2026-06-01' (the Bronze partition value)."""
    return crash_date[:10] if crash_date else None
