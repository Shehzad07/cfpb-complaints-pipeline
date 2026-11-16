# CFPB Consumer Complaints Pipeline

An **incremental batch data pipeline** that pulls consumer complaints from the public [CFPB Consumer Complaint Database API](https://cfpb.github.io/ccdb5-api/) and loads only what is new on every run.

I built this project to learn how production data pipelines behave over time: what happens when they run daily, when an API fails, when records arrive late, or when the same job runs twice.

> **Status:** work in progress. The bronze (raw) ingestion layer is built. Silver, gold, and orchestration are planned. See the [Roadmap](#roadmap).

---

## The problem

A simple script that downloads "the last N days" every time has three problems:

- It **re-downloads** the same records on every run, creating duplicates.
- It **loses data** if a run fails halfway and nothing remembers where it stopped.
- It **misses updates**, because complaints can arrive late or change after they are first published.

This pipeline is designed so that it can run on a schedule, fail and be re-run safely, and always keep the newest data on top.

## How it works

```
CFPB API
   |
   |  1. read watermark -> decide start date
   |  2. fetch pages (retry + timeout) until a page is empty
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
| **Lookback window** | Each run re-fetches 7 days before the watermark | Catches late or updated complaints |
| **Pagination** | Loop through pages of 100 until a page is empty or `max_records` is reached | The API returns a limited number of rows per request |
| **Retries with backoff** | Retries on 429/500/502/503/504, waiting longer each time | Temporary API problems do not crash the run |
| **Timeouts + `raise_for_status()`** | 30 second timeout, errors raised clearly | A hung request never blocks the pipeline forever |
| **Oldest-first sorting** | `sort=created_date_asc` | If the record limit is hit, the watermark can never skip data |
| **Append-only bronze** | Each run writes a new `run_<timestamp>.json` | Raw data is never edited, so it works as an audit trail |
| **Atomic writes** | Write to a `.tmp` file, then rename | A crash cannot leave a half-written file |
| **Safe watermark update** | Moved only after the bronze file is saved, and never backwards | A failed run cannot silently skip data |
| **Backfill** | `--backfill-from YYYY-MM-DD` ignores the watermark | Lets me reload history without changing code |

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
git clone https://github.com/<your-username>/cfpb-complaints-pipeline.git
cd cfpb-complaints-pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Run the ingestion

Run from the repository root, because paths are relative:

```bash
# Normal incremental run (first run loads the last 30 days)
python scripts/cfpb_ingest.py

# Reload history from a specific date
python scripts/cfpb_ingest.py --backfill-from 2026-01-01

# Change the lookback window or record limit
python scripts/cfpb_ingest.py --lookback-days 3 --max-records 1000
```

### 3. Check the result

- Raw data: `include/data/bronze/ingest_date=YYYY-MM-DD/run_<timestamp>.json`
- State: `include/state/cfpb_state.json`

Run it a second time. It should create a **new** bronze file, fetch only the lookback window plus anything newer, and never move the watermark backwards.

## Things I learned while building this

- **Read the API docs for every parameter.** My first requests returned `{"detail": "Not found."}`. The docs say that sending a `format` parameter turns the request into an export and ignores `frm`/`size`, so I removed it.
- **`date_received_max` is exclusive**, so to include today's complaints the end date must be tomorrow.
- **Order of operations matters.** Saving the data first and updating the watermark second is what makes a failed run safe to repeat.
- **Mocked tests can hide real API problems.** Testing against the live API is a separate step from unit testing the logic.

## Known limitations

- The API limits how deep you can page with `frm`. A very large backfill should be split into smaller date ranges.
- A corrupted state file currently raises an error instead of being treated as a first run. This is deliberate for now (fail loudly), and I may change it.
- There are no automated tests yet.

## Roadmap

- [x] Incremental ingestion with watermark, lookback, pagination, retries
- [x] Append-only bronze layer with atomic writes
- [x] Backfill option
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
