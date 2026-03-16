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
            "ORDERS_PATH",
            "PRODUCTS_PATH",
        ],
    )
    return args


def read_order_items(glue_context: GlueContext, s3_input_path: str) -> DynamicFrame:
    """
    Read order_items CSV from S3 with Glue job bookmarks enabled.
    """
    return glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [s3_input_path],
            "recurse": True,
            "groupFiles": "inPartition",
            "groupSize": "10485760",
            "jobBookmarkKeys": ["item_id"],
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
        transformation_ctx="order_items_source",
    )


def read_orders(glue_context: GlueContext, orders_path: str) -> DynamicFrame:
    """
    Read processed orders dataset (Parquet) from S3.
    Expected to contain at least: order_id, year, month (and optionally order_date/timestamp).
    """
    return glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [orders_path], "recurse": True},
        format="parquet",
        transformation_ctx="orders_ref_source",
    )


def read_products(glue_context: GlueContext, products_path: str) -> DynamicFrame:
    """
    Read processed products dataset (Parquet) from S3.
    Expected to contain at least: product_id.
    """
    return glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [products_path], "recurse": True},
        format="parquet",
        transformation_ctx="products_ref_source",
    )


def transform_order_items(
    order_items_dyf: DynamicFrame,
    orders_ref_dyf: DynamicFrame,
    products_ref_dyf: DynamicFrame,
):
    """
    Apply business transformations and validation to order_items.
    Returns (clean_df, quarantine_df, input_count).
    """
    order_items_df = order_items_dyf.toDF()
    orders_ref_df = orders_ref_dyf.toDF()
    products_ref_df = products_ref_dyf.toDF()

    input_count = order_items_df.count()

    # Trim string columns where present
    for col in ["item_id", "order_id", "product_id", "quantity", "unit_price", "discount"]:
        if col in order_items_df.columns:
            order_items_df = order_items_df.withColumn(col, F.trim(F.col(col)))

    # Cast to appropriate types
    if "item_id" in order_items_df.columns:
        order_items_df = order_items_df.withColumn("item_id", F.col("item_id").cast("long"))
    if "order_id" in order_items_df.columns:
        order_items_df = order_items_df.withColumn("order_id", F.col("order_id").cast("long"))
    if "product_id" in order_items_df.columns:
        order_items_df = order_items_df.withColumn("product_id", F.col("product_id").cast("long"))

    if "quantity" in order_items_df.columns:
        order_items_df = order_items_df.withColumn(
            "quantity_double", F.col("quantity").cast("double")
        )
    else:
        order_items_df = order_items_df.withColumn("quantity_double", F.lit(None).cast("double"))

    if "unit_price" in order_items_df.columns:
        order_items_df = order_items_df.withColumn(
            "unit_price_double", F.col("unit_price").cast("double")
        )
    else:
        order_items_df = order_items_df.withColumn("unit_price_double", F.lit(None).cast("double"))

    if "discount" in order_items_df.columns:
        order_items_df = order_items_df.withColumn(
            "discount_double", F.col("discount").cast("double")
        )
    else:
        order_items_df = order_items_df.withColumn("discount_double", F.lit(None).cast("double"))

    # Deduplicate on (order_id, item_id)
    if {"order_id", "item_id"}.issubset(set(order_items_df.columns)):
        order_items_df = order_items_df.dropDuplicates(["order_id", "item_id"])

    # Join with reference orders to validate foreign key and get partition fields
    orders_ref_df = orders_ref_df.select(
        "order_id",
        "year",
        "month",
    ).dropDuplicates(["order_id"])

    order_items_df = order_items_df.join(
        orders_ref_df,
        on="order_id",
        how="left",
    )

    # Join with reference products to validate product_id
    products_ref_df = products_ref_df.select("product_id").dropDuplicates(["product_id"])
    products_ref_df = products_ref_df.withColumnRenamed("product_id", "ref_product_id")
    order_items_df = order_items_df.join(
        products_ref_df,
        order_items_df.product_id == products_ref_df.ref_product_id,
        how="left",
    )

    # Validation flags
    invalid_item_id = order_items_df.item_id.isNull()
    invalid_order_id = order_items_df.order_id.isNull()
    invalid_product_id = order_items_df.product_id.isNull()

    invalid_unit_price = (order_items_df.unit_price_double.isNull()) | (
        order_items_df.unit_price_double <= 0
    )
    invalid_discount = (
        order_items_df.discount_double.isNull()
        | (order_items_df.discount_double < 0.0)
        | (order_items_df.discount_double > 1.0)
    )

    # Quantity: cast to integer and ensure > 0
    order_items_df = order_items_df.withColumn(
        "quantity_int",
        F.col("quantity_double").cast("int"),
    )
    invalid_quantity = (
        order_items_df.quantity_int.isNull() | (order_items_df.quantity_int <= 0)
    )

    # Foreign key validations: order and product must exist in reference datasets
    missing_order_fk = order_items_df.year.isNull() & order_items_df.month.isNull()
    missing_product_fk = F.col("ref_product_id").isNull()

    order_items_df = (
        order_items_df.withColumn("is_invalid_item_id", invalid_item_id)
        .withColumn("is_invalid_order_id", invalid_order_id)
        .withColumn("is_invalid_product_id", invalid_product_id)
        .withColumn("is_invalid_unit_price", invalid_unit_price)
        .withColumn("is_invalid_discount", invalid_discount)
        .withColumn("is_invalid_quantity", invalid_quantity)
        .withColumn("is_missing_order_fk", missing_order_fk)
        .withColumn("is_missing_product_fk", missing_product_fk)
    )

    # Build failure_reason for quarantine records
    reasons_array = F.array(
        F.when(F.col("is_invalid_item_id"), F.lit("missing_or_invalid_item_id")),
        F.when(F.col("is_invalid_order_id"), F.lit("missing_or_invalid_order_id")),
        F.when(F.col("is_invalid_product_id"), F.lit("missing_or_invalid_product_id")),
        F.when(F.col("is_invalid_unit_price"), F.lit("invalid_unit_price")),
        F.when(F.col("is_invalid_discount"), F.lit("invalid_discount")),
        F.when(F.col("is_invalid_quantity"), F.lit("invalid_quantity")),
        F.when(F.col("is_missing_order_fk"), F.lit("missing_order_reference")),
        F.when(F.col("is_missing_product_fk"), F.lit("missing_product_reference")),
    )

    order_items_df = order_items_df.withColumn("reasons_array", reasons_array)

    failure_reason = F.concat_ws(
        "; ",
        F.expr("filter(reasons_array, x -> x is not null)"),
    )

    order_items_df = order_items_df.withColumn("failure_reason", failure_reason)

    # Separate clean and quarantine
    any_failure = (
        F.col("is_invalid_item_id")
        | F.col("is_invalid_order_id")
        | F.col("is_invalid_product_id")
        | F.col("is_invalid_unit_price")
        | F.col("is_invalid_discount")
        | F.col("is_invalid_quantity")
        | F.col("is_missing_order_fk")
        | F.col("is_missing_product_fk")
    )

    clean_df = order_items_df.filter(~any_failure)
    quarantine_df = order_items_df.filter(any_failure)

    # For clean records, ensure partition fields exist (from orders_ref_df)
    clean_df = clean_df.withColumn("year", F.col("year")).withColumn(
        "month", F.col("month")
    )

    # Final clean schema
    clean_df = (
        clean_df.withColumn("quantity", F.col("quantity_int"))
        .withColumn("unit_price", F.col("unit_price_double"))
        .withColumn("discount", F.col("discount_double"))
        .drop(
            "quantity_double",
            "quantity_int",
            "unit_price_double",
            "discount_double",
            "ref_product_id",
            "is_invalid_item_id",
            "is_invalid_order_id",
            "is_invalid_product_id",
            "is_invalid_unit_price",
            "is_invalid_discount",
            "is_invalid_quantity",
            "is_missing_order_fk",
            "is_missing_product_fk",
            "reasons_array",
            "failure_reason",
        )
    )

    # Quarantine dataframe: keep everything plus failure_reason, drop helper array
    quarantine_df = quarantine_df.drop("reasons_array")

    return clean_df, quarantine_df, input_count


def write_clean(
    glue_context: GlueContext,
    clean_df,
    s3_output_path: str,
) -> int:
    """
    Write clean order_items records as Parquet partitioned by year and month.
    Returns count of clean records.
    """
    clean_count = clean_df.count()

    clean_dyf = DynamicFrame.fromDF(clean_df, glue_context, "clean_order_items_dyf")
    glue_context.write_dynamic_frame.from_options(
        frame=clean_dyf,
        connection_type="s3",
        connection_options={
            "path": s3_output_path,
            "partitionKeys": ["year", "month"],
        },
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="clean_order_items_sink",
    )
    return clean_count


def write_quarantine(
    glue_context: GlueContext,
    quarantine_df,
    s3_quarantine_path: str,
) -> int:
    """
    Write quarantine order_items records as Parquet.
    Returns count of quarantine records.
    """
    quarantine_count = quarantine_df.count()

    quarantine_dyf = DynamicFrame.fromDF(
        quarantine_df, glue_context, "quarantine_order_items_dyf"
    )
    glue_context.write_dynamic_frame.from_options(
        frame=quarantine_dyf,
        connection_type="s3",
        connection_options={
            "path": s3_quarantine_path,
        },
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="quarantine_order_items_sink",
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
    namespace = "EcommerceAnalytics/OrderItemsETL"

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
    logging.info("Starting order_items_transform job %s", args["JOB_NAME"])
    logging.info(
        "Input: %s | Output: %s | Quarantine: %s | Orders ref: %s | Products ref: %s",
        args["S3_INPUT_PATH"],
        args["S3_OUTPUT_PATH"],
        args["S3_QUARANTINE_PATH"],
        args["ORDERS_PATH"],
        args["PRODUCTS_PATH"],
    )

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    order_items_dyf = read_order_items(glue_context, args["S3_INPUT_PATH"])
    orders_ref_dyf = read_orders(glue_context, args["ORDERS_PATH"])
    products_ref_dyf = read_products(glue_context, args["PRODUCTS_PATH"])

    clean_df, quarantine_df, input_count = transform_order_items(
        order_items_dyf=order_items_dyf,
        orders_ref_dyf=orders_ref_dyf,
        products_ref_dyf=products_ref_dyf,
    )
    logging.info("Read %d order_items records", input_count)

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
        "Order_items transform completed: clean=%d, quarantine=%d (rate=%.4f)",
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
    logging.info("Order_items job %s committed successfully", args["JOB_NAME"])


if __name__ == "__main__":
    main()

