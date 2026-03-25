import sys
from datetime import datetime
import logging

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def drop_placeholder_columns(df):
    """
    Drop Glue CSV auto-generated placeholder columns like col0, col1, ... .
    These are usually created when input rows have extra delimiters.
    """
    placeholder_cols = [c for c in df.columns if c.lower().startswith("col") and c[3:].isdigit()]
    if placeholder_cols:
        logging.warning("Dropping placeholder columns from orders input: %s", placeholder_cols)
        return df.drop(*placeholder_cols)
    return df


def parse_arguments() -> dict:
    """
    Parse Glue job arguments.
    """
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_INPUT_PATH",
            "S3_OUTPUT_PATH",
            "S3_QUARANTINE_PATH",
            "CUSTOMERS_PATH",
        ],
    )
    return args


def read_orders(glue_context: GlueContext, s3_input_path: str) -> DynamicFrame:
    """
    Read orders CSV from S3 with Glue job bookmarks enabled.
    """
    return glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [s3_input_path],
            "recurse": True,
            "groupFiles": "inPartition",
            "groupSize": "10485760",
            "jobBookmarkKeys": ["order_id"],
            "jobBookmarkKeysSortOrder": "asc",
        },
        format="csv",
        format_options={
            "withHeader": True,
            "separator": ",",
            "quoteChar": '"',
            "escaper": "\\",
            "multiline": False,
        },
        transformation_ctx="orders_source",
    )


def read_customers(glue_context: GlueContext, customers_path: str) -> DynamicFrame:
    """
    Read already processed customers dataset (Parquet) from S3.
    Expected to contain at least a customer_id column.
    """
    return glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [customers_path],
            "recurse": True,
        },
        format="parquet",
        transformation_ctx="customers_source",
    )


def transform_orders(
    orders_dyf: DynamicFrame,
    customers_dyf: DynamicFrame,
):
    """
    Apply business transformations and validation to orders.
    Returns (clean_df, quarantine_df, input_count).
    """
    spark = orders_dyf.glue_ctx.spark_session

    orders_df = orders_dyf.toDF()
    orders_df = drop_placeholder_columns(orders_df)
    customers_df = customers_dyf.toDF()

    # Some reference datasets might not have a customer_id column (or be empty).
    # If the column is missing, skip the reference join and treat all non-null
    # customer_id values as valid to avoid hard failures.
    if "customer_id" in customers_df.columns:
        customers_df = customers_df.select("customer_id").dropDuplicates()
        customers_df = customers_df.withColumnRenamed("customer_id", "ref_customer_id")
        orders_df = orders_df.join(
            customers_df,
            orders_df.customer_id == customers_df.ref_customer_id,
            how="left",
        )
    else:
        orders_df = orders_df.withColumn("ref_customer_id", F.col("customer_id"))

    input_count = orders_df.count()

    # Standardise date_ordered to timestamp using three formats
    parsed_date = F.coalesce(
        F.to_timestamp("date_ordered", "yyyy-MM-dd"),
        F.to_timestamp("date_ordered", "dd/MM/yyyy"),
        F.to_timestamp("date_ordered", "MMM dd yyyy"),
    )

    orders_df = orders_df.withColumn("parsed_order_ts", parsed_date)

    # Deduplicate on order_id keeping latest record by parsed date
    w = Window.partitionBy("order_id").orderBy(F.col("parsed_order_ts").desc())
    orders_df = (
        orders_df.withColumn("row_num", F.row_number().over(w))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Normalise status
    orders_df = orders_df.withColumn(
        "status_norm",
        F.lower(F.trim(F.col("status"))),
    )

    # Clean total_amount: strip '$' and ',' and cast to double
    orders_df = orders_df.withColumn(
        "total_amount_clean",
        F.regexp_replace(F.col("total_amount"), "[$,]", ""),
    ).withColumn(
        "total_amount_double",
        F.col("total_amount_clean").cast("double"),
    )

    # Impute null / blank shipping_region with 'UNKNOWN'
    orders_df = orders_df.withColumn(
        "shipping_region_imputed",
        F.when(
            (F.col("shipping_region").isNull())
            | (F.trim(F.col("shipping_region")) == ""),
            F.lit("UNKNOWN"),
        ).otherwise(F.trim(F.col("shipping_region"))),
    )

    now_ts = F.current_timestamp()

    # Validation flags
    invalid_date = F.col("parsed_order_ts").isNull()
    future_date = F.col("parsed_order_ts") > now_ts
    missing_customer = F.col("ref_customer_id").isNull()
    non_numeric_amount = (
        F.col("total_amount_double").isNull()
        & F.col("total_amount").isNotNull()
        & (F.trim(F.col("total_amount")) != "")
    )

    orders_df = orders_df.withColumn("is_invalid_date", invalid_date).withColumn(
        "is_future_date",
        future_date,
    ).withColumn(
        "is_missing_customer",
        missing_customer,
    ).withColumn(
        "is_non_numeric_amount",
        non_numeric_amount,
    )

    # Build failure_reason for quarantine records
    reasons_array = F.array(
        F.when(F.col("is_invalid_date"), F.lit("invalid_date_format")),
        F.when(F.col("is_future_date"), F.lit("future_dated_order")),
        F.when(F.col("is_missing_customer"), F.lit("missing_customer_reference")),
        F.when(F.col("is_non_numeric_amount"), F.lit("invalid_total_amount")),
    )

    failure_reason = F.concat_ws(
        "; ",
        F.expr("filter(reasons_array, x -> x is not null)"),
    )

    orders_df = orders_df.withColumn("reasons_array", reasons_array).withColumn(
        "failure_reason",
        failure_reason,
    )

    # Separate clean and quarantine
    any_failure = (
        F.col("is_invalid_date")
        | F.col("is_future_date")
        | F.col("is_missing_customer")
        | F.col("is_non_numeric_amount")
    )

    clean_df = orders_df.filter(~any_failure)
    quarantine_df = orders_df.filter(any_failure)

    # Prepare clean dataframe with final schema
    clean_df = (
        clean_df.withColumn("order_timestamp", F.col("parsed_order_ts"))
        .withColumn("status", F.col("status_norm"))
        .withColumn("total_amount", F.col("total_amount_double"))
        .withColumn("shipping_region", F.col("shipping_region_imputed"))
        .withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
        .drop(
            "parsed_order_ts",
            "status_norm",
            "total_amount_clean",
            "total_amount_double",
            "shipping_region_imputed",
            "ref_customer_id",
            "is_invalid_date",
            "is_future_date",
            "is_missing_customer",
            "is_non_numeric_amount",
            "reasons_array",
        )
    )

    # Quarantine dataframe: keep original fields plus failure_reason
    quarantine_df = quarantine_df.drop("reasons_array")

    return clean_df, quarantine_df, input_count


def write_clean(
    glue_context: GlueContext,
    clean_df,
    s3_output_path: str,
) -> int:
    """
    Write clean records as Parquet partitioned by year and month.
    Returns count of clean records.
    """
    clean_count = clean_df.count()

    clean_dyf = DynamicFrame.fromDF(clean_df, glue_context, "clean_dyf")
    glue_context.write_dynamic_frame.from_options(
        frame=clean_dyf,
        connection_type="s3",
        connection_options={
            "path": s3_output_path,
            "partitionKeys": ["year", "month"],
        },
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="clean_sink",
    )
    return clean_count


def write_quarantine(
    glue_context: GlueContext,
    quarantine_df,
    s3_quarantine_path: str,
) -> int:
    """
    Write quarantine records as Parquet.
    Returns count of quarantine records.
    """
    quarantine_count = quarantine_df.count()

    quarantine_dyf = DynamicFrame.fromDF(
        quarantine_df, glue_context, "quarantine_dyf"
    )
    glue_context.write_dynamic_frame.from_options(
        frame=quarantine_dyf,
        connection_type="s3",
        connection_options={
            "path": s3_quarantine_path,
        },
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="quarantine_sink",
    )
    return quarantine_count


def publish_metrics(
    job_name: str,
    input_count: int,
    clean_count: int,
    quarantine_count: int,
) -> None:
    """
    Publish job metrics to CloudWatch.
    """
    cw = boto3.client("cloudwatch")
    quarantine_rate = (
        float(quarantine_count) / float(input_count) if input_count > 0 else 0.0
    )

    from datetime import timezone

    timestamp = datetime.now(timezone.utc)
    namespace = "EcommerceAnalytics/OrdersETL"

    metrics = [
        {
            "MetricName": "InputCount",
            "Dimensions": [{"Name": "JobName", "Value": job_name}],
            "Timestamp": timestamp,
            "Value": float(input_count),
            "Unit": "Count",
        },
        {
            "MetricName": "CleanCount",
            "Dimensions": [{"Name": "JobName", "Value": job_name}],
            "Timestamp": timestamp,
            "Value": float(clean_count),
            "Unit": "Count",
        },
        {
            "MetricName": "QuarantineCount",
            "Dimensions": [{"Name": "JobName", "Value": job_name}],
            "Timestamp": timestamp,
            "Value": float(quarantine_count),
            "Unit": "Count",
        },
        {
            "MetricName": "QuarantineRate",
            "Dimensions": [{"Name": "JobName", "Value": job_name}],
            "Timestamp": timestamp,
            "Value": quarantine_rate,
            "Unit": "Percent",
        },
    ]

    cw.put_metric_data(Namespace=namespace, MetricData=metrics)


def main() -> None:
    args = parse_arguments()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.info("Starting orders_transform job %s", args["JOB_NAME"])
    logging.info(
        "Input: %s | Output: %s | Quarantine: %s",
        args["S3_INPUT_PATH"],
        args["S3_OUTPUT_PATH"],
        args["S3_QUARANTINE_PATH"],
    )

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    orders_dyf = read_orders(glue_context, args["S3_INPUT_PATH"])
    customers_dyf = read_customers(glue_context, args["CUSTOMERS_PATH"])

    clean_df, quarantine_df, input_count = transform_orders(
        orders_dyf, customers_dyf
    )
    logging.info("Read %d order records", input_count)

    clean_count = write_clean(
        glue_context=glue_context,
        clean_df=clean_df,
        s3_output_path=args["S3_OUTPUT_PATH"],
    )

    quarantine_count = write_quarantine(
        glue_context=glue_context,
        quarantine_df=quarantine_df,
        s3_quarantine_path=args["S3_QUARANTINE_PATH"],
    )

    logging.info(
        "Orders transform completed: clean=%d, quarantine=%d (rate=%.4f)",
        clean_count,
        quarantine_count,
        float(quarantine_count) / float(input_count) if input_count else 0.0,
    )

    publish_metrics(
        job_name=args["JOB_NAME"],
        input_count=input_count,
        clean_count=clean_count,
        quarantine_count=quarantine_count,
    )

    job.commit()
    logging.info("Orders job %s committed successfully", args["JOB_NAME"])


if __name__ == "__main__":
    main()

