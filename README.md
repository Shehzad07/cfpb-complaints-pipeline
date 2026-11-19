# CFPB Consumer Complaints Pipeline

An **incremental batch data pipeline** that pulls consumer complaints from the public [CFPB Consumer Complaint Database API](https://cfpb.github.io/ccdb5-api/) and loads only what is new on every run.

I built this project to learn how production data pipelines behave over time: what happens when they run daily, when an API fails, when records arrive late, when one day holds tens of thousands of rows, or when the same job runs twice.

> **Status:** work in progress. The bronze (raw) ingestion layer is built and tested against the live API. Silver, gold, and orchestration are planned. See the [Roadmap](#roadmap).

---

## The problem

A simple script that downloads "the last N days" every time has four problems:

- It **re-downloads** the same records on every run, creating duplicates.
- It **loses data** if a run fails halfway and nothing remembers where it stopped.
- It **misses updates**, because complaints can arrive late or change after they are first published.
- It **breaks on volume**. This dataset is large: a single day can hold tens of thousands of complaints (I measured 48,662 for 2026-11-10), and the API will not let you page that deep with a normal offset.

This pipeline is designed so that it can run on a schedule, fail and be re-run safely, and always keep the newest data on top.

## How it works

```
CFPB API
   |
   |  1. read watermark -> decide start date
   |  2. fetch pages with a cursor (search_after) until a page is empty
   |     (retry + timeout on every request)
   v
Bronze   raw JSON, one NEW file per run (append-only)
   |
   |  3. update watermark ONLY after the file is safely written
   v
Silver   (planned) cleaned, typed, one row per complaint_id (upsert)
   |
   v
Gold     (planned) aggregates for analysis
```

### Key design decisions

| Concept | What I did | Why |
|---|---|---|
| **Watermark** | The newest `date_received` loaded is saved in `include/state/cfpb_state.json` | The next run only asks for newer data |
| **Lookback window** | Each run re-fetches 1 day before the watermark (`--lookback-days`) | Today is only partly loaded on the previous run, so the rest is picked up next time |
| **Cursor pagination** | Each hit carries a `sort` value. I pass the last one back as `search_after` to get the next page | The API rejects `frm` above 10,000, so offset paging cannot reach a full day |
| **Large pages** | `size=1000` per request | Fewer requests. The live API accepts it even though its published definition says 100 |
| **Retries with backoff** | Retries on 429/500/502/503/504, waiting longer each time | Temporary API problems do not crash the run |
| **Timeouts + `raise_for_status()`** | 30 second timeout, errors raised clearly | A hung request never blocks the pipeline forever |
| **Oldest-first sorting** | `sort=created_date_asc` | If the record limit is hit, the watermark can never skip data |
| **Record cap** | `--max-records` (default 150,000) | A safety limit so one run cannot download unlimited data |
| **Append-only bronze** | Each run writes a new `run_<timestamp>.json` | Raw data is never edited, so it works as an audit trail |
| **Atomic writes** | Write to a `.tmp` file, then rename | A crash cannot leave a half-written file |
| **Safe watermark update** | Moved only after the bronze file is saved, and never backwards | A failed run cannot silently skip data |
| **Backfill** | `--backfill-from YYYY-MM-DD` ignores the watermark | Lets me reload history without changing code |

### How paging works

```
page 1:  request with no cursor            -> hits, last hit has sort = [t1, id1]
page 2:  request with search_after=t1_id1   -> hits, last hit has sort = [t2, id2]
page 3:  request with search_after=t2_id2   -> ...
stop:    when a page comes back empty (or max_records is reached)
```

This is keyset pagination: instead of "skip N rows" it says "give me everything after this record", so the depth of the result set does not matter.

## Project structure

```
cfpb-complaints-pipeline/
├── scripts/
│   └── cfpb_ingest.py      # incremental ingestion (bronze)
├── include/
│   ├── data/bronze/        # raw JSON files (generated, not committed)
│   └── state/              # watermark file (generated, not committed)
├── dags/                   # Airflow DAGs (planned)
├── dbt/                    # dbt models for silver/gold (planned)
├── config/
├── docker-compose.yml      # Postgres service (in progress)
├── requirements.txt
└── README.md
```

Generated data and state files are excluded with `.gitignore`.

## Tech stack

- **Python 3.11**: `requests`, `urllib3` (retry), `argparse`, `logging`, `pathlib`
- **Planned:** DuckDB / Postgres, dbt, Apache Airflow, Docker, pytest

## Getting started

### 1. Clone and install

```bash
git clone https://github.com/Shehzad07/cfpb-complaints-pipeline.git
cd cfpb-complaints-pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Run the ingestion

Run from the repository root, because paths are relative:

```bash
# Normal incremental run (first run loads yesterday + today)
python scripts/cfpb_ingest.py

# Reload history from a specific date
python scripts/cfpb_ingest.py --backfill-from 2026-11-10

# Change the lookback window or record limit
python scripts/cfpb_ingest.py --lookback-days 3 --max-records 200000
```

### 3. Check the result

- Raw data: `include/data/bronze/ingest_date=YYYY-MM-DD/run_<timestamp>.json`
- State: `include/state/cfpb_state.json`

Run it a second time. It should create a **new** bronze file, re-fetch only the lookback window plus anything newer, and never move the watermark backwards.

### Example output

First run, then an immediate second run:

```
First run. loading last 1 days
Fetched 1000 records so far...
...
Fetched 4378 records so far...
Saved 4378 records to include/data/bronze/ingest_date=2026-11-18/run_20261118T144629.json
Watermark moved to 2026-11-18

Incremental run. Watermark=2026-11-18, lookback=1 days
Fetched 4378 records so far...
Saved 4378 records to include/data/bronze/ingest_date=2026-11-18/run_20261118T145106.json
```

The second run fetches the same records again, which is intended. Bronze keeps every run, and the silver layer will deduplicate by `complaint_id`.

## Things I learned while building this

- **Read the API docs for every parameter.** My first requests returned `{"detail": "Not found."}`. The docs say that sending a `format` parameter turns the request into an export and ignores `frm`/`size`, so I removed it.
- **Test the live API, not just the docs.** The published definition says `frm` can go up to 100,000 and `size` up to 100. In practice the API rejected `frm` above 10,000 and accepted `size=1000`.
- **Estimate your data volume before choosing a design.** Third-party reports suggested about 20,000 complaints per day. A real count for one day was 48,662, which ruled out offset paging entirely.
- **Offset paging has a ceiling. Cursor paging does not.** Each result carries a `sort` value that can be passed back as `search_after`.
- **`date_received_max` is exclusive**, so to include today's complaints the end date must be tomorrow.
- **Order of operations matters.** Saving the data first and updating the watermark second is what makes a failed run safe to repeat.
- **Mocked tests can hide real API problems.** Testing against the live API is a separate step from unit testing the logic.

## Known limitations

- **Recent days may be incomplete in the public database.** Yesterday plus part of today returned only 4,378 records, while a completed day held 48,662. I suspect complaints appear a few days after they are received, but I have not confirmed this. If it is true, a 1-day lookback can miss late-published records, and the lookback needs to be longer. I plan to measure this by comparing counts per `date_received` across several days.
- `size=1000` is not in the API's published definition, so it could stop working without notice.
- The whole run is held in memory and written as one JSON file. This is fine for tens of thousands of rows but would need streaming to disk for much larger loads.
- A corrupted state file currently raises an error instead of being treated as a first run. This is deliberate for now (fail loudly), and I may change it.
- There is no scheduler yet. Runs are started manually.
- There are no automated tests yet.

## Roadmap

- [x] Incremental ingestion with watermark, lookback, retries, timeouts
- [x] Cursor-based pagination (`search_after`) for full daily volume
- [x] Append-only bronze layer with atomic writes
- [x] Backfill option
- [ ] Measure publication lag and set the lookback window from real data
- [ ] Silver layer: clean, type, and deduplicate by `complaint_id` using an upsert
- [ ] Gold layer: complaints by company/month, top issues, timely-response rate
- [ ] Unit tests with `pytest` and a mocked API
- [ ] Airflow DAG with a daily schedule
- [ ] Docker Compose setup for Postgres and Airflow
- [ ] Data quality checks

## Data source

[CFPB Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/), published by the U.S. Consumer Financial Protection Bureau. The data is public and licensed CC0.

## Author

**Shehzad** · [GitHub](https://github.com/Shehzad07)
