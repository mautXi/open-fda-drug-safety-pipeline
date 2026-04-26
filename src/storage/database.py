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

_PK = {
    "fact_adverse_events": "event_id",
    "fact_recalls": "recall_number",
    "dim_drugs": "product_ndc",
}


def init(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute(DDL)
    return conn


def upsert(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    pk = _PK[table]
    conn.register("_tmp", df)
    conn.execute(f"DELETE FROM {table} WHERE {pk} IN (SELECT {pk} FROM _tmp)")
    conn.execute(f"INSERT INTO {table} SELECT * FROM _tmp")
    conn.unregister("_tmp")
    return len(df)


def log_ingestion(conn: duckdb.DuckDBPyConnection, endpoint: str, raw: int, loaded: int) -> None:
    conn.execute(
        "INSERT INTO ingestion_log (endpoint, records_raw, records_loaded) VALUES (?, ?, ?)",
        [endpoint, raw, loaded],
    )


def query(conn: duckdb.DuckDBPyConnection, sql: str, params=None) -> pd.DataFrame:
    return conn.execute(sql, params).df()
