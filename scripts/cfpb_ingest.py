"""
ingest.py - Incremental ingestion of CFPB consumer complaints into the BRONZE layer.

How it stays incremental:
  1. A watermark (newest date_received we have loaded) is stored in a state file.
  2. Each run fetches from (watermark - lookback_days) to today, so late or
     updated records are picked up again. Duplicates are fine: silver.py dedupes.
  3. Each run writes a NEW bronze file (append-only, never edited).
  4. The watermark moves forward only AFTER the bronze file is safely written.

Run:
    python cfpb_ingest.py                              # normal incremental run
    python cfpb_ingest.py --backfill-from 2026-01-01   # reload from a specific date
"""

import argparse
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------------Config-----------------
CFPB_API_URL = (
    "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
)
BRONZE_DIR = Path("include/data/bronze")
STATE_FILE = Path("include/state/cfpb_state.json")

PAGE_SIZE = 1000
REQUEST_TIMEOUT = 30  # seconds
LOOKBACK_DAYS = 1  # re-fetch yesterday, because today was partial last run
INITIAL_LOAD_DAYS = 1  # first run: yesterday + today only
DEFAULT_MAX_RECORDS = 150000  ## headroom for spikes

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# -----------State(watermark)----------


def load_watermark() -> date | None:
    """Return the last loaded date_received,
    or None on the first run."""
    if not STATE_FILE.exists():
        return None
    with open(STATE_FILE, encoding="utf-8") as f:
        return date.fromisoformat(json.load(f)["watermark"])


def save_watermark(value: date) -> None:
    """Persist the watermark atomically (temp_file, then
    rename)."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(
            {
                "watermark": value.isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            f,
        )
    tmp.replace(STATE_FILE)


def newest_received_dates(records: list[dict]) -> date | None:
    """Find the max date_received among fetched records."""
    dates = []
    for r in records:
        try:
            dates.append(date.fromisoformat(str(r.get("date_received", ""))[:10]))
        except ValueError:
            continue

    return max(dates) if dates else None


def build_session() -> requests.Session:
    """Session that retries temporary failures with exponential backoff."""
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_page(
    session, size: int, date_min: str, date_max: str, cursor=None
    ) -> list[dict]:
    """Fetch one page of complaints"""
    params = {
        "size": size,
        "sort": "created_date_asc",
        "no_aggs": "true",
        "date_received_min": date_min,
        "date_received_max": date_max,
    }
    if cursor:
        params["search_after"] = cursor
    response = session.get(CFPB_API_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json().get("hits", {}).get("hits", [])

def fetch_all(date_min: date, date_max: date, max_records: int) -> list[dict]:
    """Paginate until the window is exhausted or max_records is reached."""
    session = build_session()
    records: list[dict] = []
    cursor = None
    while len(records) < max_records:
        page = fetch_page(
            session, PAGE_SIZE, date_min.isoformat(), date_max.isoformat(), cursor
        )
        if not page:
            break
        records.extend(hit["_source"] for hit in page)
        last = page[-1]["sort"]
        cursor = f"{last[0]}_{last[1]}"
        logger.info("Fetched %d records so far...", len(records))
    return records[:max_records]


# --------------------Bronze storage-------------------


def save_bronze(records: list[dict]) -> Path:
    """Write a new file on every run, bronze is append only."""
    now = datetime.now(timezone.utc)
    out_dir = BRONZE_DIR / f"ingest_date={now.date().isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    final_path = out_dir / f"run_{now:%Y%m%dT%H%M%S}.json"

    tmp_path = final_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)
    tmp_path.replace(final_path)
    return final_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental CFPB ingestion (bronze)")
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS)
    parser.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    parser.add_argument(
        "--backfill-from",
        type=date.fromisoformat,
        default=None,
        help="YYYY-MM-DD. Ignore the watermark and reload from this date.",
    )
    args = parser.parse_args()

    today = date.today()
    watermark = load_watermark()

    if args.backfill_from:
        start = args.backfill_from
        logger.info("BACKFILL mode from %s", start)
    elif watermark:
        start = watermark - timedelta(days=args.lookback_days)
        logger.info(
            "Incremental run. Watermark=%s, lookback=%d days",
            watermark,
            args.lookback_days,
        )
    else:
        start = today - timedelta(days=INITIAL_LOAD_DAYS)
        logger.info("First run. loading last %d days", INITIAL_LOAD_DAYS)
    # date_received_max is exclusive, so use tomorrow to include today's complaints
    records = fetch_all(start, today + timedelta(days=1), args.max_records)

    if not records:
        logger.warning(
            "No records returned for %s -> %s. Watermark unchanged.", start, today
        )
        return

    path = save_bronze(records)
    logger.info("Saved %d records to %s", len(records), path)

    # Move the watermark ONLY after the bronze file is safely on disk.

    newest = newest_received_dates(records)
    if newest and (watermark is None or newest > watermark):
        save_watermark(newest)
        logger.info("Watermark moved to %s", newest)


if __name__ == "__main__":
    main()
