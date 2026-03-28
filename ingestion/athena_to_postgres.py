"""
athena_to_postgres.py
---------------------
Ingests processed tables from Athena (ecommerce_catalog) into the
PostgreSQL staging schema.

Usage:
    python ingestion/athena_to_postgres.py                  # load all 4 tables
    python ingestion/athena_to_postgres.py --table orders   # load a single table

Required .env variables:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    ATHENA_OUTPUT_S3  (e.g. s3://my-bucket/athena-results/)
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ATHENA_DATABASE = "ecommerce_catalog"
STAGING_SCHEMA = "staging"
ALL_TABLES = ["orders", "customers", "products", "order_items"]

POST_LOAD_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON staging.orders(customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON staging.order_items(order_id)",
    "CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON staging.order_items(product_id)",
]

ATHENA_PAGE_SIZE = 1000
ATHENA_POLL_INTERVAL = 2.0
ATHENA_TIMEOUT = 900.0

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
LOGGER = logging.getLogger("athena_to_postgres")


def _log_separator(label: str = "") -> None:
    line = "-" * 60
    if label:
        LOGGER.info("%s [ %s ]", line[: max(0, 30 - len(label) // 2)], label)
    else:
        LOGGER.info(line)

# ---------------------------------------------------------------------------
# Athena helpers  (reuse patterns from athena_validation.py)
# ---------------------------------------------------------------------------


def _build_athena_client(region: Optional[str]) -> Any:
    resolved_region = region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    key_id = os.environ["AWS_ACCESS_KEY_ID"]
    LOGGER.info("Building Athena client — region=%s key_id=%s...%s",
                resolved_region, key_id[:4], key_id[-4:])
    session = boto3.session.Session(
        aws_access_key_id=key_id,
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=resolved_region,
    )
    client = session.client("athena")
    LOGGER.info("Athena client ready.")
    return client


def _start_query(
    athena: Any,
    sql: str,
    output_location: str,
    workgroup: Optional[str],
) -> str:
    LOGGER.info("Submitting Athena query ...")
    LOGGER.info("  SQL            : %s", sql)
    LOGGER.info("  Database       : %s", ATHENA_DATABASE)
    LOGGER.info("  Output S3      : %s", output_location)
    LOGGER.info("  Workgroup      : %s", workgroup or "(primary/default)")
    request: Dict[str, Any] = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": ATHENA_DATABASE},
        "ResultConfiguration": {"OutputLocation": output_location},
    }
    if workgroup:
        request["WorkGroup"] = workgroup
    execution_id = athena.start_query_execution(**request)["QueryExecutionId"]
    LOGGER.info("  Execution ID   : %s", execution_id)
    return execution_id


def _wait_for_query(athena: Any, query_execution_id: str) -> None:
    """Poll until the query reaches a terminal state."""
    deadline = time.monotonic() + ATHENA_TIMEOUT
    poll_start = time.monotonic()
    last_state: Optional[str] = None
    poll_count = 0

    LOGGER.info("Polling Athena query %s (timeout=%.0fs, interval=%.1fs) ...",
                query_execution_id, ATHENA_TIMEOUT, ATHENA_POLL_INTERVAL)

    while True:
        poll_count += 1
        resp = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = resp["QueryExecution"]["Status"]
        state: str = status["State"]
        elapsed = time.monotonic() - poll_start

        if state != last_state:
            LOGGER.info("  [poll #%d | %.1fs elapsed] State: %s → %s",
                        poll_count, elapsed, last_state or "SUBMITTED", state)
            last_state = state
        else:
            LOGGER.debug("  [poll #%d | %.1fs elapsed] State: %s (unchanged)",
                         poll_count, elapsed, state)

        if state == "SUCCEEDED":
            LOGGER.info("Athena query SUCCEEDED in %.1fs (polls=%d).", elapsed, poll_count)
            return
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "unknown")
            LOGGER.error("Athena query %s ended with state=%s after %.1fs. Reason: %s",
                         query_execution_id, state, elapsed, reason)
            raise RuntimeError(
                f"Athena query {query_execution_id} ended with state={state}: {reason}"
            )
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Athena query {query_execution_id} timed out after {ATHENA_TIMEOUT:.0f}s"
            )
        time.sleep(ATHENA_POLL_INTERVAL)


def _fetch_paginated(
    athena: Any, query_execution_id: str
) -> Tuple[List[str], List[List[Optional[str]]]]:
    """Retrieve all result pages, handling the header row on page 1."""
    columns: List[str] = []
    rows: List[List[Optional[str]]] = []
    next_token: Optional[str] = None
    first_page = True
    page_num = 0
    fetch_start = time.monotonic()

    LOGGER.info("Fetching results for query %s (page_size=%d) ...",
                query_execution_id, ATHENA_PAGE_SIZE)

    while True:
        page_num += 1
        kwargs: Dict[str, Any] = {
            "QueryExecutionId": query_execution_id,
            "MaxResults": ATHENA_PAGE_SIZE,
        }
        if next_token:
            kwargs["NextToken"] = next_token

        resp = athena.get_query_results(**kwargs)
        result_set = resp["ResultSet"]

        if not columns:
            columns = [
                c["Name"]
                for c in result_set.get("ResultSetMetadata", {}).get("ColumnInfo", [])
            ]
            LOGGER.info("  Columns (%d): %s", len(columns), ", ".join(columns))

        page_rows = result_set.get("Rows", [])
        if first_page and page_rows:
            page_rows = page_rows[1:]  # drop header row
            first_page = False

        page_data_count = len(page_rows)
        for r in page_rows:
            data = r.get("Data", [])
            values: List[Optional[str]] = []
            for i in range(len(columns)):
                cell = data[i] if i < len(data) else {}
                values.append(cell.get("VarCharValue"))
            rows.append(values)

        next_token = resp.get("NextToken")
        LOGGER.info("  Page %d: fetched %d rows (total so far: %d) %s",
                    page_num, page_data_count, len(rows),
                    "— more pages incoming ..." if next_token else "— last page.")
        if not next_token:
            break

    fetch_elapsed = time.monotonic() - fetch_start
    LOGGER.info("Pagination complete: %d pages, %d total rows, %.2fs.",
                page_num, len(rows), fetch_elapsed)
    return columns, rows


def query_athena_to_df(
    athena: Any,
    table_name: str,
    output_location: str,
    workgroup: Optional[str],
) -> pd.DataFrame:
    """Execute SELECT * on an Athena table and return a DataFrame."""
    sql = f'SELECT * FROM "{ATHENA_DATABASE}"."{table_name}"'
    t_start = time.monotonic()

    LOGGER.info("[%s] Starting Athena extraction.", table_name)
    LOGGER.info("[%s] Query : %s", table_name, sql)

    query_execution_id = _start_query(athena, sql, output_location, workgroup)
    _wait_for_query(athena, query_execution_id)
    columns, rows = _fetch_paginated(athena, query_execution_id)

    if not columns:
        LOGGER.warning("[%s] Athena returned no columns — empty result set.", table_name)
        return pd.DataFrame()

    LOGGER.info("[%s] Building DataFrame from %d rows x %d columns ...",
                table_name, len(rows), len(columns))
    df = pd.DataFrame(rows, columns=columns)

    LOGGER.info("[%s] DataFrame ready — shape=%s memory=%.2f MB dtypes: %s",
                table_name, df.shape,
                df.memory_usage(deep=True).sum() / 1024 / 1024,
                dict(df.dtypes.astype(str)))
    LOGGER.info("[%s] Athena extraction complete in %.2fs.", table_name, time.monotonic() - t_start)
    return df


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------


def _pg_connection_string() -> str:
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5433")
    database = os.getenv("PG_DATABASE", "ecommerce")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "yourpassword")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def _raw_pg_dsn() -> str:
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5433")
    database = os.getenv("PG_DATABASE", "ecommerce")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "yourpassword")
    return f"host={host} port={port} dbname={database} user={user} password={password}"


def ensure_staging_schema(engine: Any) -> None:
    LOGGER.info("Ensuring schema '%s' exists in PostgreSQL ...", STAGING_SCHEMA)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}"))
    LOGGER.info("Schema '%s' is ready.", STAGING_SCHEMA)


def load_df_to_postgres(engine: Any, df: pd.DataFrame, table_name: str) -> int:
    """Write DataFrame to staging.<table_name>, replacing any existing data."""
    target = f"{STAGING_SCHEMA}.{table_name}"
    if df.empty:
        LOGGER.warning("[%s] DataFrame is empty — skipping write.", table_name)
        return 0

    row_count = len(df)
    chunksize = 500
    estimated_chunks = max(1, -(-row_count // chunksize))  # ceiling division
    LOGGER.info("[%s] Writing %d rows to %s (chunksize=%d, ~%d chunks, if_exists=replace) ...",
                table_name, row_count, target, chunksize, estimated_chunks)

    pg_start = time.monotonic()
    df.to_sql(
        name=table_name,
        schema=STAGING_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=chunksize,
    )
    pg_elapsed = time.monotonic() - pg_start
    rate = row_count / pg_elapsed if pg_elapsed > 0 else 0

    LOGGER.info("[%s] Write complete — %d rows in %.2fs (%.0f rows/s).",
                table_name, row_count, pg_elapsed, rate)
    print(f"Loaded {row_count} rows to {target}")
    return row_count


def create_indexes(conn: Any) -> None:
    """Create FK indexes idempotently (IF NOT EXISTS)."""
    LOGGER.info("Creating %d FK index(es) ...", len(POST_LOAD_INDEXES))
    with conn.cursor() as cur:
        for i, ddl in enumerate(POST_LOAD_INDEXES, start=1):
            target = ddl.split("ON")[1].strip().rstrip(";")
            LOGGER.info("  [%d/%d] %s", i, len(POST_LOAD_INDEXES), ddl)
            try:
                t0 = time.monotonic()
                cur.execute(ddl)
                LOGGER.info("         -> OK (%.2fs) on %s", time.monotonic() - t0, target)
            except psycopg2.Error as exc:
                LOGGER.warning("         -> SKIPPED: %s", exc)
                conn.rollback()
    conn.commit()
    LOGGER.info("Index creation committed.")


def vacuum_analyze(conn: Any, tables: List[str]) -> None:
    """VACUUM ANALYZE each staging table — must run outside a transaction."""
    LOGGER.info("Running VACUUM ANALYZE on %d table(s) ...", len(tables))
    old_autocommit = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for i, table_name in enumerate(tables, start=1):
                qualified = f"{STAGING_SCHEMA}.{table_name}"
                LOGGER.info("  [%d/%d] VACUUM ANALYZE %s ...", i, len(tables), qualified)
                t0 = time.monotonic()
                cur.execute(f"VACUUM ANALYZE {qualified}")
                LOGGER.info("         -> done (%.2fs).", time.monotonic() - t0)
    finally:
        conn.autocommit = old_autocommit
    LOGGER.info("VACUUM ANALYZE complete for all tables.")


# ---------------------------------------------------------------------------
# Per-table orchestration
# ---------------------------------------------------------------------------


def load_table(
    *,
    table_name: str,
    athena: Any,
    engine: Any,
    output_location: str,
    workgroup: Optional[str],
) -> int:
    """Load a single table from Athena into staging. Returns row count."""
    try:
        df = query_athena_to_df(athena, table_name, output_location, workgroup)
    except Exception as exc:
        LOGGER.error("[%s] Athena query failed: %s", table_name, exc)
        raise

    try:
        row_count = load_df_to_postgres(engine, df, table_name)
        return row_count
    except Exception as exc:
        LOGGER.error("[%s] PostgreSQL write failed: %s", table_name, exc)
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest Athena processed tables into PostgreSQL staging schema."
    )
    p.add_argument(
        "--table",
        choices=ALL_TABLES,
        default=None,
        help="Load only this table instead of all four.",
    )
    p.add_argument(
        "--athena-output",
        default=None,
        help="S3 output location for Athena results (overrides ATHENA_OUTPUT_S3 env var).",
    )
    p.add_argument(
        "--workgroup",
        default=None,
        help="Athena workgroup (overrides ATHENA_WORKGROUP env var).",
    )
    p.add_argument(
        "--database",
        default=None,
        help="Athena database name (default: ecommerce_catalog).",
    )
    p.add_argument("--verbose", action="store_true", help="Enable DEBUG logging.")
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> None:
    load_dotenv()

    args = parse_args(argv)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Resolve which tables to process
    tables_to_load: List[str] = [args.table] if args.table else ALL_TABLES

    # Resolve Athena configuration
    output_location: Optional[str] = args.athena_output or os.getenv("ATHENA_OUTPUT_S3")
    if not output_location:
        s3_bucket = os.getenv("S3_BUCKET")
        if s3_bucket:
            output_location = f"s3://{s3_bucket}/athena-results/ingestion/"
    if not output_location:
        raise RuntimeError(
            "Missing Athena S3 output location. "
            "Set ATHENA_OUTPUT_S3 in .env or pass --athena-output."
        )

    workgroup: Optional[str] = args.workgroup or os.getenv("ATHENA_WORKGROUP")
    region: Optional[str] = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")

    LOGGER.info("=== athena_to_postgres ===")
    LOGGER.info("  tables        : %s", tables_to_load)
    LOGGER.info("  athena db     : %s", ATHENA_DATABASE)
    LOGGER.info("  output s3     : %s", output_location)
    LOGGER.info("  workgroup     : %s", workgroup or "(default)")
    LOGGER.info("  pg target     : %s:%s/%s", os.getenv("PG_HOST", "localhost"),
                os.getenv("PG_PORT", "5433"), os.getenv("PG_DATABASE", "ecommerce"))

    # Build clients
    try:
        athena = _build_athena_client(region)
    except KeyError as exc:
        raise RuntimeError(
            f"Missing AWS credential env var: {exc}. "
            "Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in .env."
        ) from exc

    LOGGER.info("Connecting to PostgreSQL — host=%s port=%s db=%s user=%s ...",
                os.getenv("PG_HOST", "localhost"), os.getenv("PG_PORT", "5433"),
                os.getenv("PG_DATABASE", "ecommerce"), os.getenv("PG_USER", "postgres"))
    try:
        engine = create_engine(_pg_connection_string(), pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        LOGGER.info("SQLAlchemy engine connected successfully.")
    except Exception as exc:
        LOGGER.error("PostgreSQL connection failed: %s", exc)
        raise

    # Raw psycopg2 connection for DDL that cannot run inside a transaction
    # (VACUUM ANALYZE) and for index creation.
    try:
        raw_conn = psycopg2.connect(_raw_pg_dsn())
        LOGGER.info("psycopg2 raw connection established.")
    except psycopg2.Error as exc:
        LOGGER.error("psycopg2 connection failed: %s", exc)
        raise

    try:
        ensure_staging_schema(engine)

        overall_start = time.monotonic()
        total_rows = 0
        failed_tables: List[str] = []
        loaded_tables: List[str] = []

        for idx, table_name in enumerate(tables_to_load, start=1):
            _log_separator(f"{idx}/{len(tables_to_load)}: {table_name}")
            t0 = time.monotonic()
            try:
                row_count = load_table(
                    table_name=table_name,
                    athena=athena,
                    engine=engine,
                    output_location=output_location,
                    workgroup=workgroup,
                )
                elapsed = time.monotonic() - t0
                total_rows += row_count
                loaded_tables.append(table_name)
                LOGGER.info("[%s] ✔ SUCCESS — %d rows loaded in %.1fs.",
                            table_name, row_count, elapsed)
            except Exception as exc:
                elapsed = time.monotonic() - t0
                LOGGER.error("[%s] ✘ FAILED after %.1fs: %s", table_name, elapsed, exc)
                failed_tables.append(table_name)
                # Continue with remaining tables rather than aborting entire run.

        _log_separator("POST-LOAD")

        # Create FK indexes only for tables that loaded successfully
        if loaded_tables:
            create_indexes(raw_conn)

        # VACUUM ANALYZE successfully loaded tables
        if loaded_tables:
            vacuum_analyze(raw_conn, loaded_tables)

        total_elapsed = time.monotonic() - overall_start
        _log_separator("SUMMARY")
        LOGGER.info("  Total time    : %.1fs", total_elapsed)
        LOGGER.info("  Total rows    : %d", total_rows)
        LOGGER.info("  Tables OK     : %s", loaded_tables)
        LOGGER.info("  Tables FAILED : %s", failed_tables if failed_tables else "none")
        print(
            f"\nTotal time: {total_elapsed:.1f}s | "
            f"Rows loaded: {total_rows} | "
            f"Tables OK: {loaded_tables} | "
            f"Tables FAILED: {failed_tables}"
        )

        if failed_tables:
            raise SystemExit(
                f"Ingestion completed with errors. Failed tables: {failed_tables}"
            )

    finally:
        raw_conn.close()
        engine.dispose()


if __name__ == "__main__":
    main()
