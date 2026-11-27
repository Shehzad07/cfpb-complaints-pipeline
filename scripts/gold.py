"""
gold.py - Build the GOLD layer (business-ready tables) from SILVER using DuckDB.

Silver = one clean row per complaint.
Gold   = small summary tables that answer questions directly.

Each gold table is rebuilt in full from silver on every run
(CREATE OR REPLACE), so running it many times gives the same result.

Run from the repo root:
    python scripts/gold.py
"""

import logging
from pathlib import Path

import duckdb

# ---------- Config ----------
DB_PATH = Path("include/data/warehouse.duckdb")
MIN_COMPLAINTS = 100  # ignore tiny companies when ranking rates

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# table name -> the SELECT that builds it
GOLD_TABLES = {
    # How many complaints were received each day.
    # Recent days look small because complaints are published with a delay.
    "complaints_per_day": """
        SELECT date_received, count(*) AS complaints
        FROM silver.complaints
        WHERE date_received IS NOT NULL
        GROUP BY date_received
        ORDER BY date_received
    """,
    # Which companies receive the most complaints.
    "top_companies": """
        SELECT company, count(*) AS complaints
        FROM silver.complaints
        WHERE company IS NOT NULL
        GROUP BY company
        ORDER BY complaints DESC
    """,
    # Most common issues.
    "top_issues": """
        SELECT product, issue, count(*) AS complaints
        FROM silver.complaints
        WHERE product IS NOT NULL AND issue is NOT NULL
        GROUP BY product, issue
        ORDER BY complaints DESC
    """,
    # Share of complaints where the company responded on time.
    "timely_response_rate": f"""
        SELECT
            company,
            count(*) AS complaints,
            count(*) FILTER (WHERE timely = 'Yes') AS timely_yes,
            round(100.0 * count(*) FILTER (WHERE timely = 'Yes') / count(*), 2) AS timely_pct
        FROM silver.complaints
        WHERE company is NOT NULL AND timely IS NOT NULL
        GROUP BY company
        HAVING count(*) >= {MIN_COMPLAINTS}
        ORDER BY complaints DESC
    """,
}


def build_gold() -> None:
    if not DB_PATH.exists():
        logger.error("%s not found. run silver.py first", DB_PATH)
        return

    con = duckdb.connect(str(DB_PATH))
    try:
        try:
            silver_rows = con.execute(
                "SELECT count(*) from silver.complaints"
            ).fetchone()[0]
        except duckdb.CatalogException:
            logger.error("Table silver.complaints does not exist, Run silver.py first")
            return

        if silver_rows == 0:
            logger.warning("silver.complaints is empty. Nothing to build")
            return

        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        for name, query in GOLD_TABLES.items():
            con.execute(f"CREATE OR REPLACE TABLE gold.{name} AS {query}")
            rows = con.execute(f"SELECT count(*) FROM gold.{name}").fetchone()[0]
            logger.info("gold.%s built: %d rows", name, rows)
    finally:
        con.close()


if __name__ == "__main__":
    build_gold()
