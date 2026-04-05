import sys, os
sys.path.insert(0, "/opt/airflow/project")

import logging
from datetime import timedelta
from typing import List, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Optional runtime overrides (Admin → Variables, or AIRFLOW_VAR_<KEY> in the worker env).
# If unset, ingestion scripts use their CLI defaults (--output-dir data/generated, --max-quarantine-rate 0.05).
VAR_GENERATE_DATA_OUTPUT_DIR = "ecommerce_pipeline_generate_data_output_dir"
# Optional integer; passed as --run-id so each batch gets non-overlapping surrogate keys.
VAR_GENERATE_DATA_RUN_ID = "ecommerce_pipeline_generate_data_run_id"
VAR_ATHENA_VALIDATION_MAX_QUARANTINE_RATE = (
    "ecommerce_pipeline_athena_validation_max_quarantine_rate"
)


def _optional_airflow_var(key: str) -> Optional[str]:
    raw = Variable.get(key, default_var=None)
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


def build_generate_data_argv() -> List[str]:
    argv: List[str] = []
    out_dir = _optional_airflow_var(VAR_GENERATE_DATA_OUTPUT_DIR)
    if out_dir is not None:
        argv.extend(["--output-dir", out_dir])
    run_id_raw = _optional_airflow_var(VAR_GENERATE_DATA_RUN_ID)
    if run_id_raw is not None:
        argv.extend(["--run-id", str(int(run_id_raw))])
    return argv


def build_athena_validation_argv() -> List[str]:
    argv: List[str] = []
    rate = _optional_airflow_var(VAR_ATHENA_VALIDATION_MAX_QUARANTINE_RATE)
    if rate is not None:
        argv.extend(["--max-quarantine-rate", str(float(rate))])
    return argv

# default (commented): ecommerce-customers-transform
GLUE_JOB_NAME_CUSTOMERS = os.environ["GLUE_JOB_NAME_CUSTOMERS"]
# default (commented): ecommerce-products-transform
GLUE_JOB_NAME_PRODUCTS = os.environ["GLUE_JOB_NAME_PRODUCTS"]
# default (commented): ecommerce-orders-transform
GLUE_JOB_NAME_ORDERS = os.environ["GLUE_JOB_NAME_ORDERS"]
# default (commented): ecommerce-orders-items-transform
GLUE_JOB_NAME_ORDER_ITEMS = os.environ["GLUE_JOB_NAME_ORDER_ITEMS"]

# default (commented): ecommerce-raw-crawler
GLUE_CRAWLER_NAME_RAW = os.environ["GLUE_CRAWLER_NAME_RAW"]
# default (commented): ecommerce-processed-crawler
GLUE_CRAWLER_NAME_PROCESSED = os.environ["GLUE_CRAWLER_NAME_PROCESSED"]

# dbt (mounted path in Airflow containers; profile name matches dbt_project.yml)
DBT_PROJECT_DIR = "/opt/airflow/project/dbt_project"
DBT_PROFILE_NAME = "ecommerce"


def run_generate_data() -> None:
    from ingestion.generate_data import main

    argv = build_generate_data_argv()
    if argv:
        logging.info("generate_data argv overrides: %s", argv)
    main(argv)


def run_upload_to_s3() -> None:
    from ingestion.upload_to_s3 import main

    main()


def run_glue_crawler_task(crawler_name: str) -> None:
    from ingestion.run_glue_crawler import run_glue_crawler

    run_glue_crawler(crawler_name=crawler_name, poll_interval_seconds=30)


def run_athena_validation() -> None:
    from ingestion.athena_validation import main

    argv = build_athena_validation_argv()
    if argv:
        logging.info("athena_validation argv overrides: %s", argv)
    main(argv)


def run_athena_to_postgres() -> None:
    from ingestion.athena_to_postgres import main

    main()


def log_pipeline_summary() -> None:
    s3_bucket = os.environ.get("S3_BUCKET", "")
    athena_out = os.environ.get("ATHENA_OUTPUT_S3", "")
    logging.info(
        "Ecommerce pipeline completed.\n"
        "  Glue ETL jobs: customers=%s, products=%s, orders=%s, order_items=%s\n"
        "  Glue crawlers: raw=%s, processed=%s\n"
        "  Athena / S3: bucket=%s, athena_output_s3=%s\n"
        "  dbt: project=%s, profile=%s (staging → intermediate → marts, then tests)",
        GLUE_JOB_NAME_CUSTOMERS,
        GLUE_JOB_NAME_PRODUCTS,
        GLUE_JOB_NAME_ORDERS,
        GLUE_JOB_NAME_ORDER_ITEMS,
        GLUE_CRAWLER_NAME_RAW,
        GLUE_CRAWLER_NAME_PROCESSED,
        s3_bucket,
        athena_out,
        DBT_PROJECT_DIR,
        DBT_PROFILE_NAME,
    )


def build_glue_poll_command(job_name: str) -> str:
    return f"""
set -euo pipefail
echo "Starting Glue job: {job_name}"
run_id=$(aws glue start-job-run --job-name "{job_name}" --query 'JobRunId' --output text)
echo "Glue job started: {job_name}, run_id=$run_id"

while true; do
  state=$(aws glue get-job-run --job-name "{job_name}" --run-id "$run_id" --query 'JobRun.JobRunState' --output text)
  echo "Glue job {job_name} ($run_id) state: $state"

  if [ "$state" = "SUCCEEDED" ]; then
    echo "Glue job {job_name} succeeded."
    break
  fi

  if [ "$state" = "FAILED" ] || [ "$state" = "STOPPED" ] || [ "$state" = "TIMEOUT" ]; then
    echo "Glue job {job_name} failed with state: $state"
    exit 1
  fi

  sleep 30
done
""".strip()


BASH_ENV = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "AWS_REGION": os.environ.get("AWS_REGION", ""),
    "S3_BUCKET": os.environ.get("S3_BUCKET", ""),
    "ATHENA_OUTPUT_S3": os.environ.get("ATHENA_OUTPUT_S3", ""),
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
    "POSTGRES_DB": os.environ.get("POSTGRES_DB", ""),
    "POSTGRES_USER": os.environ.get("POSTGRES_USER", ""),
    "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
}


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ecommerce_daily_pipeline",
    default_args=default_args,
    # schedule_interval="0 6 * * *",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=run_generate_data,
    )

    ingest_to_s3 = PythonOperator(
        task_id="ingest_to_s3",
        python_callable=run_upload_to_s3,
    )

    run_glue_customers = BashOperator(
        task_id="run_glue_customers",
        bash_command=build_glue_poll_command(GLUE_JOB_NAME_CUSTOMERS),
        env=BASH_ENV,
        append_env=True,
    )

    run_glue_products = BashOperator(
        task_id="run_glue_products",
        bash_command=build_glue_poll_command(GLUE_JOB_NAME_PRODUCTS),
        env=BASH_ENV,
        append_env=True,
    )

    run_glue_orders = BashOperator(
        task_id="run_glue_orders",
        bash_command=build_glue_poll_command(GLUE_JOB_NAME_ORDERS),
        env=BASH_ENV,
        append_env=True,
    )

    run_glue_order_items = BashOperator(
        task_id="run_glue_order_items",
        bash_command=build_glue_poll_command(GLUE_JOB_NAME_ORDER_ITEMS),
        env=BASH_ENV,
        append_env=True,
    )

    run_glue_raw_crawler = PythonOperator(
        task_id="run_glue_raw_crawler",
        python_callable=run_glue_crawler_task,
        op_kwargs={"crawler_name": GLUE_CRAWLER_NAME_RAW},
    )

    run_glue_processed_crawler = PythonOperator(
        task_id="run_glue_processed_crawler",
        python_callable=run_glue_crawler_task,
        op_kwargs={"crawler_name": GLUE_CRAWLER_NAME_PROCESSED},
    )

    validate_with_athena = PythonOperator(
        task_id="validate_with_athena",
        python_callable=run_athena_validation,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_athena_to_postgres,
    )

    # profiles.yml lives in the mounted dbt_project; there is no .../project/docker/airflow in the image.
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt run --select staging --profiles-dir {DBT_PROJECT_DIR}"
        ),
        env=BASH_ENV,
        append_env=True,
    )

    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt run --select intermediate --profiles-dir {DBT_PROJECT_DIR}"
        ),
        env=BASH_ENV,
        append_env=True,
    )

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt run --select marts --profiles-dir {DBT_PROJECT_DIR}"
        ),
        env=BASH_ENV,
        append_env=True,
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt test --profiles-dir {DBT_PROJECT_DIR}"
        ),
        env=BASH_ENV,
        append_env=True,
    )

    notify_complete = PythonOperator(
        task_id="notify_complete",
        python_callable=log_pipeline_summary,
    )

    generate_data >> ingest_to_s3
    ingest_to_s3 >> run_glue_raw_crawler
    run_glue_raw_crawler >> [run_glue_customers, run_glue_products, run_glue_orders]
    [run_glue_customers, run_glue_products, run_glue_orders] >> run_glue_order_items
    run_glue_order_items >> run_glue_processed_crawler
    run_glue_processed_crawler >> validate_with_athena
    validate_with_athena >> load_to_postgres
    load_to_postgres >> run_dbt_staging
    run_dbt_staging >> run_dbt_intermediate >> run_dbt_marts >> run_dbt_tests >> notify_complete
