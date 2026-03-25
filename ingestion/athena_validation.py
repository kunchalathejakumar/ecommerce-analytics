import argparse
import contextlib
import logging
import os
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple


LOGGER = logging.getLogger(__name__)

# Default raw / processed / quarantine Glue table names (same Athena database).
# Raw tables are usually created by a Glue crawler on s3://.../raw/; names must match these defaults
# (not the processed table names) so row-count checks can run.
ENTITY_PIPELINE_TABLES: Dict[str, Tuple[str, str, str]] = {
    "customers": ("customers_raw", "customers", "customers_quarantine"),
    "products": ("products_raw", "products", "products_quarantine"),
    "orders": ("orders_raw", "orders", "orders_quarantine"),
    "order_items": ("order_items_raw", "order_items", "order_items_quarantine"),
}


@dataclass(frozen=True)
class AthenaQueryResult:
    query_execution_id: str
    columns: List[str]
    rows: List[List[Optional[str]]]


class _TeeStream:
    """Write stream output to multiple file-like targets."""

    def __init__(self, *streams: Any) -> None:
        self._streams = streams

    def write(self, data: str) -> int:
        for s in self._streams:
            s.write(data)
        return len(data)

    def flush(self) -> None:
        for s in self._streams:
            s.flush()

    @property
    def encoding(self) -> str:
        first = self._streams[0]
        return getattr(first, "encoding", "utf-8")


def _parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {s3_uri}")
    remainder = s3_uri[5:]
    if "/" in remainder:
        bucket, key = remainder.split("/", 1)
    else:
        bucket, key = remainder, ""
    if not bucket:
        raise ValueError(f"Invalid S3 URI with empty bucket: {s3_uri}")
    return bucket, key


def _upload_validation_log_to_s3(
    *,
    log_file_path: str,
    run_timestamp: str,
    run_status: str,
    region_name: Optional[str],
) -> Optional[str]:
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        LOGGER.warning("S3_BUCKET is not set; skipping Athena validation log upload.")
        return None

    # Keep all validation run logs in one location for historical inspection.
    key = f"logs/athena-validation-log/athena-validation-{run_timestamp}-{run_status}.log"
    import boto3

    session_kwargs: Dict[str, Any] = {}
    if region_name:
        session_kwargs["region_name"] = region_name
    session = boto3.session.Session(**session_kwargs)
    s3 = session.client("s3")
    s3.upload_file(log_file_path, bucket, key)
    return f"s3://{bucket}/{key}"


def _quote_ident(identifier: str) -> str:
    # Athena/Trino identifier quoting uses double-quotes.
    return '"' + identifier.replace('"', '""') + '"'


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _athena_client(region_name: Optional[str]) -> Any:
    import boto3

    session_kwargs: Dict[str, Any] = {}
    if region_name:
        session_kwargs["region_name"] = region_name
    session = boto3.session.Session(**session_kwargs)
    return session.client("athena")


def _start_query_execution(
    athena: Any,
    sql: str,
    database: str,
    output_location: str,
    workgroup: Optional[str],
) -> str:
    request: Dict[str, Any] = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": database},
        "ResultConfiguration": {"OutputLocation": output_location},
    }
    if workgroup:
        request["WorkGroup"] = workgroup
    resp = athena.start_query_execution(**request)
    return resp["QueryExecutionId"]


def _wait_for_query(
    athena: Any,
    query_execution_id: str,
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> Dict[str, Any]:
    start = time.time()
    last_state: Optional[str] = None

    while True:
        resp = athena.get_query_execution(QueryExecutionId=query_execution_id)
        qe = resp.get("QueryExecution", {})
        status = qe.get("Status", {})
        state = status.get("State")

        if state != last_state:
            LOGGER.info("Athena query state: %s (id=%s)", state, query_execution_id)
            last_state = state

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if state != "SUCCEEDED":
                reason = status.get("StateChangeReason") or "Unknown"
                raise RuntimeError(
                    f"Athena query {query_execution_id} did not succeed: state={state}, reason={reason}"
                )
            return qe

        if (time.time() - start) > timeout_seconds:
            raise TimeoutError(
                f"Athena query {query_execution_id} timed out after {timeout_seconds:.0f}s"
            )

        time.sleep(poll_interval_seconds)


def _fetch_all_rows(athena: Any, query_execution_id: str) -> Tuple[List[str], List[List[Optional[str]]]]:
    columns: List[str] = []
    rows: List[List[Optional[str]]] = []

    next_token: Optional[str] = None
    is_first_page = True
    while True:
        kwargs: Dict[str, Any] = {"QueryExecutionId": query_execution_id, "MaxResults": 1000}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = athena.get_query_results(**kwargs)

        result_set = resp.get("ResultSet", {})
        meta = result_set.get("ResultSetMetadata", {})
        col_info = meta.get("ColumnInfo", [])
        if col_info and not columns:
            columns = [c.get("Name", "") for c in col_info]

        page_rows = result_set.get("Rows", [])
        if is_first_page and page_rows:
            # First row is header row.
            page_rows = page_rows[1:]
            is_first_page = False

        for r in page_rows:
            data = r.get("Data", [])
            values: List[Optional[str]] = []
            for i in range(len(columns)):
                cell = data[i] if i < len(data) else {}
                values.append(cell.get("VarCharValue"))
            rows.append(values)

        next_token = resp.get("NextToken")
        if not next_token:
            break

    return columns, rows


def run_athena_query(
    athena: Any,
    *,
    sql: str,
    database: str,
    output_location: str,
    workgroup: Optional[str],
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> AthenaQueryResult:
    query_execution_id = _start_query_execution(
        athena=athena,
        sql=sql,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
    )
    _wait_for_query(
        athena=athena,
        query_execution_id=query_execution_id,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )
    columns, rows = _fetch_all_rows(athena=athena, query_execution_id=query_execution_id)
    return AthenaQueryResult(query_execution_id=query_execution_id, columns=columns, rows=rows)


def _print_result(title: str, result: AthenaQueryResult) -> None:
    from tabulate import tabulate

    print()
    print(f"== {title} ==")
    if not result.columns:
        print("(no columns returned)")
        return
    if not result.rows:
        print("(no rows)")
        return
    print(tabulate(result.rows, headers=result.columns, tablefmt="github"))


def _get_table_columns(
    athena: Any,
    *,
    database: str,
    table: str,
    output_location: str,
    workgroup: Optional[str],
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> List[str]:
    sql = (
        "SELECT column_name "
        "FROM information_schema.columns "
        f"WHERE table_schema = '{database}' AND table_name = '{table}' "
        "ORDER BY ordinal_position"
    )
    result = run_athena_query(
        athena,
        sql=sql,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )
    return [r[0] for r in result.rows if r and r[0]]


def _build_null_check_sql(database: str, table: str, columns: Sequence[str]) -> str:
    if not columns:
        raise RuntimeError(f"No columns found for {database}.{table}")
    exprs = [f"SUM(CASE WHEN {_quote_ident(c)} IS NULL THEN 1 ELSE 0 END) AS {_quote_ident(c)}" for c in columns]
    return f"SELECT {', '.join(exprs)} FROM {_quote_ident(database)}.{_quote_ident(table)}"


def _parse_single_row_counts(result: AthenaQueryResult) -> Dict[str, int]:
    if not result.rows or len(result.rows) < 1:
        raise RuntimeError(f"Expected at least one row, got none (query_id={result.query_execution_id})")
    if len(result.columns) != len(result.rows[0]):
        raise RuntimeError("Unexpected Athena result shape for counts query.")

    skip_as_int = frozenset({"quarantine_rate", "processed_plus_quarantine"})
    out: Dict[str, int] = {}
    row = result.rows[0]
    for col, val in zip(result.columns, row):
        if col in skip_as_int:
            continue
        if val is None or val == "":
            out[col] = 0
        else:
            out[col] = int(float(val))
    return out


def _entity_count_comparison_sql(
    database: str,
    raw_table: str,
    processed_table: str,
    quarantine_table: str,
) -> str:
    return f"""
    WITH
      raw_ct AS (SELECT COUNT(*) AS raw_count FROM {_quote_ident(database)}.{_quote_ident(raw_table)}),
      proc_ct AS (SELECT COUNT(*) AS processed_count FROM {_quote_ident(database)}.{_quote_ident(processed_table)}),
      q_ct AS (SELECT COUNT(*) AS quarantine_count FROM {_quote_ident(database)}.{_quote_ident(quarantine_table)})
    SELECT
      raw_count,
      processed_count,
      quarantine_count,
      (processed_count + quarantine_count) AS processed_plus_quarantine,
      (raw_count - (processed_count + quarantine_count)) AS delta,
      CASE WHEN raw_count = 0 THEN 0.0 ELSE (CAST(quarantine_count AS DOUBLE) / CAST(raw_count AS DOUBLE)) END AS quarantine_rate
    FROM raw_ct CROSS JOIN proc_ct CROSS JOIN q_ct
    """.strip()


def _parse_entities_arg(raw: str) -> List[str]:
    keys = [e.strip() for e in raw.split(",") if e.strip()]
    unknown = [k for k in keys if k not in ENTITY_PIPELINE_TABLES]
    if unknown:
        raise ValueError(
            f"Unknown --entities value(s): {unknown}. "
            f"Expected a subset of: {', '.join(sorted(ENTITY_PIPELINE_TABLES))}."
        )
    return keys


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run post-Glue data quality validations in Athena."
    )
    p.add_argument("--database", default="ecommerce_catalog", help="Athena database name.")
    p.add_argument("--orders-table", default="orders", help="Processed orders table.")
    p.add_argument("--raw-orders-table", default="orders_raw", help="Raw CSV-backed orders table.")
    p.add_argument("--quarantine-table", default="orders_quarantine", help="Orders quarantine table.")
    p.add_argument(
        "--entities",
        default="customers,products,orders,order_items",
        help="Comma-separated pipelines to check raw vs processed vs quarantine counts for "
        f"(subset of: {', '.join(sorted(ENTITY_PIPELINE_TABLES))}).",
    )
    p.add_argument(
        "--athena-output",
        default=None,
        help="S3 output location for Athena results (overrides ATHENA_OUTPUT_S3).",
    )
    p.add_argument(
        "--workgroup",
        default=None,
        help="Athena workgroup (overrides ATHENA_WORKGROUP).",
    )
    p.add_argument("--region", default=None, help="AWS region (defaults to AWS_REGION/AWS_DEFAULT_REGION).")
    p.add_argument("--poll-interval", type=float, default=2.0, help="Poll interval in seconds (default: 2).")
    p.add_argument("--timeout", type=float, default=900.0, help="Query timeout in seconds (default: 900).")
    p.add_argument(
        "--max-quarantine-rate",
        type=float,
        default=0.05,
        help="Fail if quarantine_count/raw_count exceeds this threshold (default: 0.05).",
    )
    p.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    # Load environment variables from .env if python-dotenv is installed.
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ModuleNotFoundError:
        pass
    _configure_logging(args.verbose)

    region = args.region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    database = args.database
    orders_table = args.orders_table
    raw_orders_table = args.raw_orders_table
    quarantine_table = args.quarantine_table

    try:
        entity_keys = _parse_entities_arg(args.entities)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    workgroup = args.workgroup or os.getenv("ATHENA_WORKGROUP")
    output_location = (
        args.athena_output
        or os.getenv("ATHENA_OUTPUT_S3")
        or (
            f"s3://{os.getenv('S3_BUCKET')}/athena-results/validation/"
            if os.getenv("S3_BUCKET")
            else None
        )
    )
    if not output_location:
        raise RuntimeError(
            "Missing Athena output location. Set ATHENA_OUTPUT_S3 in .env or pass --athena-output."
        )

    if not output_location.startswith("s3://"):
        raise ValueError(f"Athena output location must be an s3:// URI, got: {output_location}")

    LOGGER.info("Running Athena validations")
    LOGGER.info("  region      : %s", region or "(default)")
    LOGGER.info("  database    : %s", database)
    LOGGER.info("  entities    : %s", ", ".join(entity_keys))
    LOGGER.info("  orders      : %s (processed)", orders_table)
    LOGGER.info("  raw_orders  : %s", raw_orders_table)
    LOGGER.info("  quarantine  : %s (orders)", quarantine_table)
    LOGGER.info("  workgroup   : %s", workgroup or "(default)")
    LOGGER.info("  output_s3   : %s", output_location)

    athena = _athena_client(region_name=region)

    poll_interval_seconds = float(args.poll_interval)
    timeout_seconds = float(args.timeout)
    max_quarantine_rate = float(args.max_quarantine_rate)
    if not (0.0 <= max_quarantine_rate <= 1.0):
        raise ValueError(f"--max-quarantine-rate must be in [0.0, 1.0], got: {max_quarantine_rate}")

    # 1) Row count comparison (raw vs processed vs quarantine) per entity
    for entity in entity_keys:
        raw_t, proc_t, q_t = ENTITY_PIPELINE_TABLES[entity]
        if entity == "orders":
            raw_t, proc_t, q_t = raw_orders_table, orders_table, quarantine_table

        counts_sql = _entity_count_comparison_sql(database, raw_t, proc_t, q_t)
        counts_result = run_athena_query(
            athena,
            sql=counts_sql,
            database=database,
            output_location=output_location,
            workgroup=workgroup,
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=timeout_seconds,
        )
        _print_result(f"Row counts ({entity}: {raw_t} vs {proc_t} vs {q_t})", counts_result)
        counts = _parse_single_row_counts(counts_result)
        raw_count = int(counts.get("raw_count", 0))
        quarantine_count = int(counts.get("quarantine_count", 0))
        processed_count = int(counts.get("processed_count", 0))
        delta = int(counts.get("delta", 0))

        quarantine_rate = (float(quarantine_count) / float(raw_count)) if raw_count > 0 else 0.0
        if delta != 0:
            LOGGER.warning(
                "[%s] Row count mismatch: raw=%d processed=%d quarantine=%d delta=%d",
                entity,
                raw_count,
                processed_count,
                quarantine_count,
                delta,
            )
        if quarantine_rate > max_quarantine_rate:
            raise RuntimeError(
                f"[{entity}] Quarantine rate too high: {quarantine_rate:.2%} "
                f"(quarantine={quarantine_count}, raw={raw_count})"
            )

    # 2) Null check: count nulls in every column of orders
    columns = _get_table_columns(
        athena,
        database=database,
        table=orders_table,
        output_location=output_location,
        workgroup=workgroup,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )
    nulls_sql = _build_null_check_sql(database=database, table=orders_table, columns=columns)
    nulls_result = run_athena_query(
        athena,
        sql=nulls_sql,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )
    _print_result("Null checks (null counts per column for orders)", nulls_result)

    # 3) Status distribution
    status_sql = (
        f"SELECT status, COUNT(*) AS cnt "
        f"FROM {_quote_ident(database)}.{_quote_ident(orders_table)} "
        "GROUP BY status ORDER BY 2 DESC"
    )
    status_result = run_athena_query(
        athena,
        sql=status_sql,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )
    _print_result("Status distribution (orders)", status_result)

    # 4) Date range check (and fail if future-dated)
    date_sql = (
        f"SELECT MIN(order_date) AS min_order_date, MAX(order_date) AS max_order_date "
        f"FROM {_quote_ident(database)}.{_quote_ident(orders_table)}"
    )
    date_result = run_athena_query(
        athena,
        sql=date_sql,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )
    _print_result("Date range check (orders.order_date)", date_result)
    if date_result.rows and date_result.rows[0] and len(date_result.rows[0]) >= 2:
        max_dt = date_result.rows[0][1]
        if max_dt:
            try:
                max_order_date = date.fromisoformat(max_dt)
                if max_order_date > date.today():
                    raise RuntimeError(
                        f"Found future-dated orders: max(order_date)={max_order_date.isoformat()} > today={date.today().isoformat()}"
                    )
            except ValueError:
                LOGGER.warning("Could not parse max(order_date) value as ISO date: %r", max_dt)

    # 5) Quarantine breakdown
    quarantine_sql = (
        "SELECT failure_reason, COUNT(*) AS cnt "
        f"FROM {_quote_ident(database)}.{_quote_ident(quarantine_table)} "
        "GROUP BY failure_reason ORDER BY 2 DESC"
    )
    quarantine_result = run_athena_query(
        athena,
        sql=quarantine_sql,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )
    _print_result("Quarantine breakdown (orders_quarantine)", quarantine_result)

    LOGGER.info("Athena validations completed successfully.")


if __name__ == "__main__":
    run_timestamp = time.strftime("%Y%m%d-%H%M%S")
    run_status = "failed"
    region_name = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    log_file_path = os.path.join(
        os.getcwd(), f"athena-validation-run-{run_timestamp}.log"
    )

    with open(log_file_path, mode="w", encoding="utf-8") as log_file:
        tee_out = _TeeStream(sys.stdout, log_file)
        tee_err = _TeeStream(sys.stderr, log_file)
        with contextlib.redirect_stdout(tee_out), contextlib.redirect_stderr(tee_err):
            try:
                main()
                run_status = "success"
            except Exception as exc:
                # Persist traceback details into the captured log file.
                traceback.print_exc()
                # Import lazily so `--help` works even without deps installed.
                try:
                    from botocore.exceptions import BotoCoreError, ClientError

                    if isinstance(exc, (ClientError, BotoCoreError)):
                        raise RuntimeError(f"AWS error while running Athena validations: {exc}") from exc
                except Exception:
                    pass
                raise
            finally:
                try:
                    uploaded_uri = _upload_validation_log_to_s3(
                        log_file_path=log_file_path,
                        run_timestamp=run_timestamp,
                        run_status=run_status,
                        region_name=region_name,
                    )
                    if uploaded_uri:
                        print(f"Athena validation log uploaded to: {uploaded_uri}")
                except Exception:
                    traceback.print_exc()
                    print("WARNING: Failed to upload Athena validation log to S3.", file=sys.stderr)

