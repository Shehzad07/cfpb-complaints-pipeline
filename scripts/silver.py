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
    return f"NULLIF(trim(CAST({col} as AS VARCHAR)), " ") AS {col}"


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXIST silver")
    columns_defs = ",\n    ".join(
        f"{name}{typ}{' PRIMARY KEY' if name == 'complaint_id' else ''}"
        for name, typ in COLUMNS.items()
    )
    con.execute(f"""
        CREATE TABLE IF NOT EXIST silver.complaints (
            {columns_defs},
            loaded_at TIMESTAMP
        )
    """)


def build_silver() -> None:
    if not list(BRONZE_DIR.glob("*/*.json")):
        logger.warning("No bronze files found in %s. Nothing to do", BRONZE_DIR)
        return

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    create_tables(con)

    source = (
        f"read_json_auto('{BRONZE_DIR.as_posix()}/*/*.json', "
        "union_by_name=true, file_name=true)"
    )

    available = {
        row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    }
    missing = set(COLUMNS) - availble
    if missing:
        logger.warning("Column missing in bronze (will be Null): %s", sorted(missing))
    if "complaint_id" not in available:
        raise RuntimeError(
            "Bronze data has no complaint_id column. Cannot build silver."
        )
