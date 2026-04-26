from pathlib import Path

import duckdb
import pandas as pd


DDL = """
CREATE TABLE IF NOT EXISTS fact_adverse_events (
    event_id          VARCHAR PRIMARY KEY,
    report_date       DATE,
    year              SMALLINT,
    month             SMALLINT,
    serious           BOOLEAN,
    death             BOOLEAN,
    hospitalization   BOOLEAN,
    life_threatening  BOOLEAN,
    country           VARCHAR,
    sex               VARCHAR,
    age_years         FLOAT,
    suspect_drug      VARCHAR,
    suspect_brand     VARCHAR,
    all_suspect_drugs VARCHAR,
    reactions         VARCHAR,
    num_reactions     SMALLINT,
    num_suspect_drugs SMALLINT
);

CREATE TABLE IF NOT EXISTS fact_recalls (
    recall_number          VARCHAR PRIMARY KEY,
    report_date            DATE,
    recall_initiation_date DATE,
    year                   SMALLINT,
    month                  SMALLINT,
    classification         VARCHAR,
    class_num              SMALLINT,
    status                 VARCHAR,
    recall_type            VARCHAR,
    recalling_firm         VARCHAR,
    product_description    VARCHAR,
    reason_for_recall      VARCHAR,
    distribution_pattern   VARCHAR,
    state                  VARCHAR,
    country                VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_drugs (
    product_ndc          VARCHAR PRIMARY KEY,
    generic_name         VARCHAR,
    brand_name           VARCHAR,
    labeler_name         VARCHAR,
    dosage_form          VARCHAR,
    route                VARCHAR,
    pharm_class          VARCHAR,
    dea_schedule         VARCHAR,
    marketing_start_date DATE,
    product_type         VARCHAR,
    application_number   VARCHAR
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    endpoint         VARCHAR,
    ingested_at      TIMESTAMP DEFAULT now(),
    records_raw      INTEGER,
    records_loaded   INTEGER
);
"""


def init(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.executemany("", [])  # no-op to check connection
    for statement in DDL.split(";"):
        stmt = statement.strip()
        if stmt:
            conn.execute(stmt)
    return conn


def upsert(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    # Use INSERT OR REPLACE pattern via temp table
    tmp = f"_tmp_{table}"
    conn.register(tmp, df)
    conn.execute(f"DELETE FROM {table} WHERE {_pk(table)} IN (SELECT {_pk(table)} FROM {tmp})")
    conn.execute(f"INSERT INTO {table} SELECT * FROM {tmp}")
    conn.unregister(tmp)
    return len(df)


def _pk(table: str) -> str:
    pks = {
        "fact_adverse_events": "event_id",
        "fact_recalls": "recall_number",
        "dim_drugs": "product_ndc",
    }
    return pks.get(table, "rowid")


def log_ingestion(conn: duckdb.DuckDBPyConnection, endpoint: str, raw: int, loaded: int) -> None:
    conn.execute(
        "INSERT INTO ingestion_log (endpoint, records_raw, records_loaded) VALUES (?, ?, ?)",
        [endpoint, raw, loaded],
    )


def query(conn: duckdb.DuckDBPyConnection, sql: str, params=None) -> pd.DataFrame:
    if params:
        return conn.execute(sql, params).df()
    return conn.execute(sql).df()
