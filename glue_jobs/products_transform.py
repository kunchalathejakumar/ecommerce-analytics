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
        ],
    )
    return args


def read_products(glue_context: GlueContext, s3_input_path: str) -> DynamicFrame:
    """
    Read products CSV from S3 with Glue job bookmarks enabled.
    """
    return glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [s3_input_path],
            "recurse": True,
            "groupFiles": "inPartition",
            "groupSize": "10485760",
            "jobBookmarkKeys": ["product_id"],
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
        transformation_ctx="products_source",
    )


def transform_products(products_dyf: DynamicFrame):
    """
    Apply business transformations and validation to products.
    Returns (clean_df, quarantine_df, input_count).
    """
    products_df = products_dyf.toDF()

    input_count = products_df.count()

    # Trim basic string columns
    for col in ["product_id", "name", "category"]:
        if col in products_df.columns:
            products_df = products_df.withColumn(col, F.trim(F.col(col)))

    # Cast numeric columns
    if "product_id" in products_df.columns:
        products_df = products_df.withColumn(
            "product_id", F.col("product_id").cast("long")
        )

    if "list_price" in products_df.columns:
        products_df = products_df.withColumn(
            "list_price", F.col("list_price").cast("double")
        )

    if "cost_price" in products_df.columns:
        products_df = products_df.withColumn(
            "cost_price", F.col("cost_price").cast("double")
        )

    # Deduplicate on product_id (keep first)
    if "product_id" in products_df.columns:
        products_df = products_df.dropDuplicates(["product_id"])

    # Fix category typos
    if "category" in products_df.columns:
        cat_lower = F.lower(F.col("category"))
        products_df = products_df.withColumn(
            "category_clean",
            F.when(
                cat_lower.isin("electornics", "electroncs"),
                F.lit("Electronics"),
            ).otherwise(F.col("category")),
        )
    else:
        products_df = products_df.withColumn("category_clean", F.lit(None))

    # Impute null cost_price with category median
    # First, compute median cost_price per category_clean using approx_percentile
    if "cost_price" in products_df.columns:
        median_df = (
            products_df.groupBy("category_clean")
            .agg(
                F.expr(
                    "percentile_approx(cost_price, 0.5) as median_cost_price"
                )
            )
        )

        products_df = products_df.join(
            median_df,
            on="category_clean",
            how="left",
        )

        products_df = products_df.withColumn(
            "cost_price_imputed",
            F.when(
                F.col("cost_price").isNull(),
                F.col("median_cost_price"),
            ).otherwise(F.col("cost_price")),
        )
    else:
        products_df = products_df.withColumn("cost_price_imputed", F.lit(None))

    # Validation flags
    invalid_product_id = F.col("product_id").isNull()
    invalid_cost_price = (F.col("cost_price_imputed").isNull()) | (
        F.col("cost_price_imputed") <= 0
    )
    invalid_list_price = F.col("list_price").isNull() | (F.col("list_price") <= 0)
    price_below_cost = F.col("list_price") < F.col("cost_price_imputed")

    products_df = (
        products_df.withColumn("is_invalid_product_id", invalid_product_id)
        .withColumn("is_invalid_cost_price", invalid_cost_price)
        .withColumn("is_invalid_list_price", invalid_list_price)
        .withColumn("is_price_below_cost", price_below_cost)
    )

    # Build failure_reason for quarantine records
    reasons_array = F.array(
        F.when(F.col("is_invalid_product_id"), F.lit("missing_or_invalid_product_id")),
        F.when(F.col("is_invalid_cost_price"), F.lit("invalid_cost_price")),
        F.when(F.col("is_invalid_list_price"), F.lit("invalid_list_price")),
        F.when(F.col("is_price_below_cost"), F.lit("price_below_cost_price")),
    )

    products_df = products_df.withColumn("reasons_array", reasons_array)

    failure_reason = F.concat_ws(
        "; ",
        F.expr("filter(reasons_array, x -> x is not null)"),
    )

    products_df = products_df.withColumn("failure_reason", failure_reason)

    # Separate clean and quarantine
    any_failure = (
        F.col("is_invalid_product_id")
        | F.col("is_invalid_cost_price")
        | F.col("is_invalid_list_price")
        | F.col("is_price_below_cost")
    )

    clean_df = products_df.filter(~any_failure)
    quarantine_df = products_df.filter(any_failure)

    # Final clean schema
    clean_df = (
        clean_df.withColumn("category", F.col("category_clean"))
        .withColumn("cost_price", F.col("cost_price_imputed"))
        .drop(
            "category_clean",
            "median_cost_price",
            "cost_price_imputed",
            "is_invalid_product_id",
            "is_invalid_cost_price",
            "is_invalid_list_price",
            "is_price_below_cost",
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
    Write clean product records as Parquet.
    Returns count of clean records.
    """
    clean_count = clean_df.count()

    clean_dyf = DynamicFrame.fromDF(clean_df, glue_context, "clean_products_dyf")
    glue_context.write_dynamic_frame.from_options(
        frame=clean_dyf,
        connection_type="s3",
        connection_options={
            "path": s3_output_path,
        },
        format="glueparquet",
        format_options={"compression": "snappy"},
        transformation_ctx="clean_products_sink",
    )
    return clean_count


def write_quarantine(
    glue_context: GlueContext,
    quarantine_df,
    s3_quarantine_path: str,
) -> int:
    """
    Write quarantine product records as Parquet.
    Returns count of quarantine records.
    """
    quarantine_count = quarantine_df.count()

    quarantine_dyf = DynamicFrame.fromDF(
        quarantine_df, glue_context, "quarantine_products_dyf"
    )
    glue_context.write_dynamic_frame.from_options(
        frame=quarantine_dyf,
        connection_type="s3",
        connection_options={
            "path": s3_quarantine_path,
        },
        format="glueparquet",
        format_options={"compression": "snappy"},
        transformation_ctx="quarantine_products_sink",
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
    namespace = "EcommerceAnalytics/ProductsETL"

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
    logging.info("Starting products_transform job %s", args["JOB_NAME"])
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

    products_dyf = read_products(glue_context, args["S3_INPUT_PATH"])

    clean_df, quarantine_df, input_count = transform_products(products_dyf)
    logging.info("Read %d product records", input_count)

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
        "Products transform completed: clean=%d, quarantine=%d (rate=%.4f)",
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
    logging.info("Products job %s committed successfully", args["JOB_NAME"])


if __name__ == "__main__":
    main()

