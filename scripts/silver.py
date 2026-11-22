"""
silver.py - Build the SILVER layer from BRONZE using DuckDB.

Bronze = raw, append-only JSON files (may contain duplicates / repeated records).
Silver = cleaned, typed, ONE row per complaint_id, always holding the latest version.

Safe to run any number of times: the upsert makes it idempotent.

Run:
    python silver.py
"""

import logging
from pathlib import Path

import duckdb

# ----------------------Config-------------------
BRONZE_DIR = Path("include/data/bronze")
DB_PATH = Path("include/data/warehouse.duckdb")

# column name -> target type. Only these columns go to silver.
COLUMNS = {
    "complaint_id": "BIGINT",
    "date_received": "DATE",
    "date_sent_to_company": "DATE",
    "product": "VARCHAR",
    "sub_product": "VARCHAR",
    "issue": "VARCHAR",
    "sub_issue": "VARCHAR",
    "company": "VARCHAR",
    "state": "VARCHAR",
    "zip_code": "VARCHAR",
    "submitted_via": "VARCHAR",
    "company_response": "VARCHAR",
    "timely": "VARCHAR",
    "consumer_disputed": "VARCHAR",
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def clean_expression(col: str, sql_type: str, available: set[str]) -> str:
    """Build the SQL that cleans one column. Missing columns become NULL."""
    if col not in available:
        return f"CAST(NULL AS {sql_type}) as {col}"
    if sql_type == "BIGINT":
        return f"TRY_CAST({col} AS BIGINT) as {col}"
    if sql_type == "DATE":
        return f"TRY_CAST(substr(CAST({COL} AS VARCHAR), 1, 10) AS DATE) AS {col}"
    return f"NULLIF(trim(CAST({col} as AS VARCHAR)), "") AS {col}"


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXIST silver")
    columns_defs = ",\n    ".join(
        
    )
