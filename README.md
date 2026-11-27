# CFPB Consumer Complaints Pipeline

An **incremental batch data pipeline** that pulls consumer complaints from the public [CFPB Consumer Complaint Database API](https://cfpb.github.io/ccdb5-api/) and loads only what is new on every run.

I built this project to learn how production data pipelines behave over time: what happens when they run daily, when an API fails, when records arrive late, when one day holds tens of thousands of rows, or when the same job runs twice.

> **Status:** work in progress. Bronze (raw ingestion), silver (cleaned and deduplicated), and gold (summary tables) are built and run end to end. Tests and orchestration are next. See the [Roadmap](#roadmap).

---

## The problem

A simple script that downloads "the last N days" every time has four problems:

- It **re-downloads** the same records on every run, creating duplicates.
- It **loses data** if a run fails halfway and nothing remembers where it stopped.
- It **misses updates**, because complaints are published with a delay and can change after they first appear.
- It **breaks on volume**. This dataset is large: a single day can hold tens of thousands of complaints (I measured 48,662 for 2026-11-10), and the API will not let you page that deep with a normal offset.

This pipeline is designed so that it can run on a schedule, fail and be re-run safely, and always keep the newest data on top.

## How it works

```
CFPB API
   |
   |  1. read watermark -> decide start date (watermark minus lookback)
   |  2. fetch pages with a cursor (search_after) until a page is empty
   |     (retry + timeout on every request)
   v
Bronze   raw JSON, one NEW file per run (append-only)
   |
   |  3. update watermark ONLY after the file is safely written
   v

Silver 4. cleaned, typed, one row per complaint_id (upsert)
   |
   v
Gold   5. aggregates for analysis
```

### Key design decisions

| Concept | What I did | Why |
|---|---|---|
| **Watermark** | The newest `date_received` loaded is saved in `include/state/cfpb_state.json` | The next run only asks for newer data |
| **Lookback window** | Each run re-fetches 5 days before the watermark (`--lookback-days`) | Complaints are published with a delay of about two days (see [Known limitations](#known-limitations)), so recent days must be fetched again |
| **Cursor pagination** | Each hit carries a `sort` value. I pass the last one back as `search_after` to get the next page | The API rejects `frm` above 10,000, so offset paging cannot reach a full day |
| **Large pages** | `size=1000` per request | Fewer requests. The live API accepts it even though its published definition says 100 |
| **Retries with backoff** | Retries on 429/500/502/503/504, waiting longer each time | Temporary API problems do not crash the run |
| **Timeouts + `raise_for_status()`** | 30 second timeout, errors raised clearly | A hung request never blocks the pipeline forever |
| **Oldest-first sorting** | `sort=created_date_asc` | If the record limit is hit, the watermark can never skip data |
| **Record cap** | `--max-records` (default 300,000) | A safety limit so one run cannot download unlimited data, sized for a multi-day window |
| **Append-only bronze** | Each run writes a new `run_<timestamp>.json` | Raw data is never edited, so it works as an audit trail |
| **Atomic writes** | Write to a `.tmp` file, then rename | A crash cannot leave a half-written file |
| **Safe watermark update** | Moved only after the bronze file is saved, and never backwards | A failed run cannot silently skip data |
| **Backfill** | `--backfill-from YYYY-MM-DD` ignores the watermark | Lets me reload history without changing code |
| **Silver upsert** | `INSERT OR REPLACE` on a `complaint_id` primary key, keeping the newest bronze version of each complaint | Re-runs never create duplicates, and updated complaints replace old ones |
| **Gold rebuild** | Each gold table is rebuilt in full from silver (`CREATE OR REPLACE`) | Safe to repeat, and gold never drifts from silver |

### How paging works

```
page 1:  request with no cursor            -> hits, last hit has sort = [t1, id1]
page 2:  request with search_after=t1_id1   -> hits, last hit has sort = [t2, id2]
page 3:  request with search_after=t2_id2   -> ...
stop:    when a page comes back empty (or max_records is reached)
```

This is keyset pagination: instead of "skip N rows" it says "give me everything after this record", so the depth of the result set does not matter.

### Gold tables

| Table | What it answers |
|---|---|
| `gold.complaints_per_day` | Complaints received per `date_received` |
| `gold.top_companies` | Which companies receive the most complaints |
| `gold.top_issues` | The most common product and issue combinations |
| `gold.timely_response_rate` | Per company, the share of timely responses (companies with at least 100 complaints) |

## Project structure

```
cfpb-complaints-pipeline/
├── scripts/
│   ├── cfpb_ingest.py         # incremental ingestion (bronze)
│   ├── silver.py              # clean + deduplicate (silver)
│   └── gold.py                # summary tables (gold)
├── include/
│   ├── data/bronze/           # raw JSON files (generated, not committed)
│   ├── data/warehouse.duckdb  # silver + gold tables (generated, not committed)
│   └── state/                 # watermark file (generated, not committed)
├── tests/                     # pytest tests (planned)
├── dags/                      # Airflow DAGs (planned)
├── dbt/                       # dbt models (planned)
├── config/
├── docker-compose.yml         # Postgres service (in progress)
├── requirements.txt
└── README.md
```

Generated data and state files are excluded with `.gitignore`.

## Tech stack

- **Python 3.14** (run in a GitHub Codespace): `requests`, `urllib3` (retry), `argparse`, `logging`, `pathlib`
- **DuckDB**: silver and gold tables
- **Planned:** pytest, Apache Airflow, Docker, dbt or Postgres

## Getting started

### 1. Clone and install

```bash
git clone https://github.com/Shehzad07/cfpb-complaints-pipeline.git
cd cfpb-complaints-pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Run the full pipeline

Run everything from the repository root, because paths are relative:

```bash
python scripts/cfpb_ingest.py   # bronze: fetch new complaints
python scripts/silver.py        # silver: clean + deduplicate
python scripts/gold.py          # gold: summary tables
```

Ingestion options:

```bash
# Reload history from a specific date
python scripts/cfpb_ingest.py --backfill-from 2026-11-10

# Change the lookback window or record limit
python scripts/cfpb_ingest.py --lookback-days 3 --max-records 200000
```

On the very first run there is no watermark, so the script loads yesterday and today only. Use `--backfill-from` for older data.

### 3. Check the result

- Raw data: `include/data/bronze/ingest_date=YYYY-MM-DD/run_<timestamp>.json`
- State: `include/state/cfpb_state.json`
- Tables: `include/data/warehouse.duckdb` (`silver.complaints` and the `gold.*` tables)

Run the pipeline a second time. Ingestion creates a **new** bronze file and never moves the watermark backwards. Silver and gold must report no new rows and the same counts.

### Example output

Ingestion, first run and an immediate second run (earlier version with a 1-day lookback):

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

The second run fetches the same records again, which is intended. Bronze keeps every run.

Silver, run twice on the real data:

```
Silver rows: before=0 after=52817 (new=52817)
Silver rows: before=52817 after=52817 (new=0)    <- the second run changes nothing
```

Bronze held 85,037 rows, of which 52,817 were unique complaints. Silver matches that unique count exactly.

Gold, `complaints_per_day` for the first days of the data:

```
date_received  complaints
2026-11-14          9,346
2026-11-15          4,373
2026-11-16         15,602
2026-11-17         18,442
2026-11-18          5,053
2026-11-19              1
```

The smallest counts are at the end, because the newest days are not fully published yet (see below). The 14th and 15th were a weekend, so their lower counts are probably real quiet days.

## Things I learned while building this

- **Read the API docs for every parameter.** My first requests returned `{"detail": "Not found."}`. The docs say that sending a `format` parameter turns the request into an export and ignores `frm`/`size`, so I removed it.
- **Test the live API, not just the docs.** The published definition says `frm` can go up to 100,000 and `size` up to 100. In practice the API rejected `frm` above 10,000 and accepted `size=1000`.
- **Estimate your data volume before choosing a design.** Third-party reports suggested about 20,000 complaints per day. A real count for one day was 48,662, which ruled out offset paging entirely.
- **Offset paging has a ceiling. Cursor paging does not.** Each result carries a `sort` value that can be passed back as `search_after`.
- **A moved watermark does not mean that day is complete.** The watermark advances as soon as one complaint for a new date exists, while the rest of that day is still being published. Completeness comes from the lookback, not from the watermark.
- **`date_received_max` is exclusive**, so to include today's complaints the end date must be tomorrow.
- **Order of operations matters.** Saving the data first and updating the watermark second is what makes a failed run safe to repeat.
- **Paths are relative, so scripts must run from the repo root.** Running a script from inside `scripts/` created a second `include/` folder.
- **Mocked tests can hide real API problems.** Testing against the live API is a separate step from unit testing the logic.

## Known limitations

- **Publication lag (measured).** Complaints appear in the API with a delay. For 2026-11-17, 4,373 records were visible the next day, 18,426 about a day later, and 18,442 in the final count, so a day is about 99.9% complete after roughly two days. I use a 5-day lookback for margin. This is based on one fully observed day, and I will re-check it as more runs accumulate.
- **Counts can shift.** A first run returned 10,000 records for 2026-11-14, while silver now shows 9,346 for that day. I do not know why, so I treat the counts as close, not exact.
- **Silver only inserts and updates.** If the API ever removes a complaint, it would remain in silver.
- **Silver reads every bronze file on every run.** It will slow down as bronze grows. Processing only new files is a planned improvement.
- **Bronze grows quickly**, because each run re-fetches the lookback window and stores it again.
- `size=1000` is not in the API's published definition, so it could stop working without notice.
- The whole ingestion run is held in memory and written as one JSON file. This is fine for hundreds of thousands of rows but would need streaming to disk for much larger loads.
- A corrupted state file currently raises an error instead of being treated as a first run. This is deliberate for now (fail loudly), and I may change it.
- There is no scheduler yet, and a GitHub Codespace cannot act as one because it stops when idle. Runs are started manually.
- There are no automated tests yet.

## Roadmap

- [x] Incremental ingestion with watermark, lookback, retries, timeouts
- [x] Cursor-based pagination (`search_after`) for full daily volume
- [x] Append-only bronze layer with atomic writes
- [x] Backfill option
- [x] Silver layer: clean, type, and deduplicate by `complaint_id` using an upsert — 85,037 bronze rows reduced to 52,817 unique complaints.
- [x] Measure publication lag and set the lookback window from real data
- [x] Gold layer: daily volume, top companies, top issues, timely-response rate
- [ ] Unit tests with `pytest` (watermark, paging with a fake API, silver idempotency, gold)
- [ ] Airflow DAG running ingest, silver, and gold daily
- [ ] Silver: process only new bronze files
- [ ] Bronze retention or Parquet compression
- [ ] Docker Compose setup for Postgres and Airflow
- [ ] Data quality checks

## Data source

[CFPB Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/), published by the U.S. Consumer Financial Protection Bureau. The data is public and licensed CC0.

## Author

**Shehzad** · [GitHub](https://github.com/Shehzad07)
