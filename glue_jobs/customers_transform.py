import sys
from datetime import datetime

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
        ],
    )
    return args


def read_customers(glue_context: GlueContext, s3_input_path: str) -> DynamicFrame:
    """
    Read customers CSV from S3 with Glue job bookmarks enabled.
    """
    return glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [s3_input_path],
            "recurse": True,
            "groupFiles": "inPartition",
            "groupSize": "10485760",
            "jobBookmarkKeys": ["customer_id"],
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
        transformation_ctx="customers_source",
    )


def transform_customers(customers_dyf: DynamicFrame):
    """
    Apply business transformations and validation to customers.
    Returns (clean_df, quarantine_df, input_count).
    """
    customers_df = customers_dyf.toDF()

    input_count = customers_df.count()

    # Basic cleanup: trim strings
    for col in ["customer_id", "name", "email", "segment", "country"]:
        if col in customers_df.columns:
            customers_df = customers_df.withColumn(
                col, F.trim(F.col(col))
            )

    # Deduplicate on customer_id (keep first)
    if "customer_id" in customers_df.columns:
        customers_df = customers_df.dropDuplicates(["customer_id"])

    # Deduplicate on email (keep first)
    if "email" in customers_df.columns:
        customers_df = customers_df.dropDuplicates(["email"])

    # Transform name: title-case after trimming
    if "name" in customers_df.columns:
        customers_df = customers_df.withColumn(
            "name",
            F.initcap(F.col("name")),
        )

    # Standardize country names (US variants -> United States, etc.)
    if "country" in customers_df.columns:
        country_upper = F.upper(F.col("country"))
        customers_df = customers_df.withColumn(
            "country_standardized",
            F.when(
                country_upper.isin("US", "USA", "UNITED STATES"),
                F.lit("United States"),
            )
            .when(
                country_upper.isin("UK", "UNITED KINGDOM", "GB", "GREAT BRITAIN"),
                F.lit("United Kingdom"),
            )
            .otherwise(F.col("country")),
        )
    else:
        customers_df = customers_df.withColumn("country_standardized", F.lit(None))

    # Impute null / blank segment with 'UNKNOWN'
    if "segment" in customers_df.columns:
        customers_df = customers_df.withColumn(
            "segment_imputed",
            F.when(
                (F.col("segment").isNull()) | (F.trim(F.col("segment")) == ""),
                F.lit("UNKNOWN"),
            ).otherwise(F.col("segment")),
        )
    else:
        customers_df = customers_df.withColumn("segment_imputed", F.lit("UNKNOWN"))

    # Email format validation (very simple regex)
    if "email" in customers_df.columns:
        email_valid = F.col("email").rlike(
            r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        )
    else:
        email_valid = F.lit(False)

    # Other validation flags
    missing_customer_id = ~F.col("customer_id").cast("string").rlike(r"^\d+$")
    empty_name = (F.col("name").isNull()) | (F.trim(F.col("name")) == "")

    customers_df = (
        customers_df.withColumn("is_invalid_email", ~email_valid)
        .withColumn("is_missing_customer_id", missing_customer_id)
        .withColumn("is_empty_name", empty_name)
    )

    # Build failure_reason for quarantine records
    reasons_array = F.array(
        F.when(F.col("is_invalid_email"), F.lit("invalid_email_format")),
        F.when(F.col("is_missing_customer_id"), F.lit("missing_or_invalid_customer_id")),
        F.when(F.col("is_empty_name"), F.lit("missing_name")),
    )

    customers_df = customers_df.withColumn("reasons_array", reasons_array)

    failure_reason = F.concat_ws(
        "; ",
        F.expr("filter(reasons_array, x -> x is not null)"),
    )

    customers_df = customers_df.withColumn("failure_reason", failure_reason)

    # Separate clean and quarantine
    any_failure = (
        F.col("is_invalid_email")
        | F.col("is_missing_customer_id")
        | F.col("is_empty_name")
    )

    clean_df = customers_df.filter(~any_failure)
    quarantine_df = customers_df.filter(any_failure)

    # Final clean schema
    clean_df = (
        clean_df.withColumn("segment", F.col("segment_imputed"))
        .withColumn("country", F.col("country_standardized"))
        .drop(
            "segment_imputed",
            "country_standardized",
            "is_invalid_email",
            "is_missing_customer_id",
            "is_empty_name",
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
    Write clean customer records as Parquet.
    Returns count of clean records.
    """
    clean_count = clean_df.count()

    clean_dyf = DynamicFrame.fromDF(clean_df, glue_context, "clean_customers_dyf")
    glue_context.write_dynamic_frame.from_options(
        frame=clean_dyf,
        connection_type="s3",
        connection_options={
            "path": s3_output_path,
        },
        format="glueparquet",
        format_options={"compression": "snappy"},
        transformation_ctx="clean_customers_sink",
    )
    return clean_count


def write_quarantine(
    glue_context: GlueContext,
    quarantine_df,
    s3_quarantine_path: str,
) -> int:
    """
    Write quarantine customer records as Parquet.
    Returns count of quarantine records.
    """
    quarantine_count = quarantine_df.count()

    quarantine_dyf = DynamicFrame.fromDF(
        quarantine_df, glue_context, "quarantine_customers_dyf"
    )
    glue_context.write_dynamic_frame.from_options(
        frame=quarantine_dyf,
        connection_type="s3",
        connection_options={
            "path": s3_quarantine_path,
        },
        format="glueparquet",
        format_options={"compression": "snappy"},
        transformation_ctx="quarantine_customers_sink",
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
    namespace = "EcommerceAnalytics/CustomersETL"

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

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    customers_dyf = read_customers(glue_context, args["S3_INPUT_PATH"])

    clean_df, quarantine_df, input_count = transform_customers(customers_dyf)

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

    publish_metrics(
        job_name=args["JOB_NAME"],
        input_count=input_count,
        clean_count=clean_count,
        quarantine_count=quarantine_count,
    )

    job.commit()


if __name__ == "__main__":
    main()

