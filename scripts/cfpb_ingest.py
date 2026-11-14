"""
ingest.py - Incremental ingestion of CFPB consumer complaints into the BRONZE layer.

How it stays incremental:
  1. A watermark (newest date_received we have loaded) is stored in a state file.
  2. Each run fetches from (watermark - lookback_days) to today, so late or
     updated records are picked up again. Duplicates are fine: silver.py dedupes.
  3. Each run writes a NEW bronze file (append-only, never edited).
  4. The watermark moves forward only AFTER the bronze file is safely written.

Run:
    python ingest.py                              # normal incremental run
    python ingest.py --backfill-from 2026-01-01   # reload from a specific date
"""
import argparse
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

#-------------Config-----------------
CFPB_API_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
BRONZE_DIR = Path("include/data/bronze")
STATE_FILE = Path("include/state/cfpb_state.json")

PAGE_SIZE = 100
REQUEST_TIMEOUT = 30   # seconds
LOOK_BACK_DAYS = 7     # refetch these days before watermark
INITIAL_LOAD_DAYS = 30 # how far back the very first run goes
DEFAULT_MAX_RECORDS = 5000

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


#-----------State(watermark)----------

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
    json.dump({"watermark": value.isoformat(),
    "updated_at": datetime.now(timezone.utc).isoformat()}, f)
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
