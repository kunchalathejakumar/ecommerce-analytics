import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from ingestion.upload_logs import upload_logs_to_s3


LOGGER = logging.getLogger(__name__)


def _get_glue_client(region_name: Optional[str] = None):
    """
    Return a boto3 Glue client.

    Region is taken from:
    - explicit argument, or
    - AWS_REGION / AWS_DEFAULT_REGION env vars.
    """
    if region_name is None:
        region_name = (
            os.getenv("AWS_REGION")
            or os.getenv("AWS_DEFAULT_REGION")
        )
    return boto3.client("glue", region_name=region_name)


def run_glue_crawler(
    crawler_name: str = "ecommerce-processed-crawler",
    poll_interval_seconds: int = 15,
) -> None:
    """
    Start an AWS Glue crawler and wait until it finishes.

    - Loads credentials/region from .env (if present).
    - Starts the crawler.
    - Polls every `poll_interval_seconds` until state is READY or FAILED.
    - Logs start/end times, state, last crawl metrics.
    - Raises an exception if the crawler fails so Airflow can mark the task failed.
    """

    # Load environment variables (AWS creds, region, etc.) from .env if present.
    load_dotenv()

    # Basic logging configuration in case caller hasn't configured logging.
    if not LOGGER.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        )

    glue = _get_glue_client()

    LOGGER.info("Starting Glue crawler '%s'...", crawler_name)
    start_time = datetime.now(timezone.utc)

    try:
        glue.start_crawler(Name=crawler_name)
    except ClientError as e:
        # If it's already running, log and continue polling.
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "CrawlerRunningException":
            LOGGER.warning(
                "Crawler '%s' is already running, will wait for completion.",
                crawler_name,
            )
        else:
            LOGGER.error("Failed to start crawler '%s': %s", crawler_name, e, exc_info=True)
            raise
    except BotoCoreError as e:
        LOGGER.error("BotoCore error while starting crawler '%s': %s", crawler_name, e, exc_info=True)
        raise

    # Poll until the crawler is READY or FAILED
    last_state = None
    while True:
        try:
            response = glue.get_crawler(Name=crawler_name)
        except (ClientError, BotoCoreError) as e:
            LOGGER.error("Error fetching crawler '%s' status: %s", crawler_name, e, exc_info=True)
            raise

        crawler = response.get("Crawler", {})
        state = crawler.get("State")
        last_crawl = crawler.get("LastCrawl", {})

        if state != last_state:
            LOGGER.info(
                "Crawler '%s' state changed: %s",
                crawler_name,
                state,
            )
            last_state = state

        # When state is READY, we can inspect LastCrawl for outcome
        if state == "READY":
            status = last_crawl.get("Status")
            error_message = last_crawl.get("ErrorMessage")
            log_group = last_crawl.get("LogGroup")
            log_stream = last_crawl.get("LogStream")

            end_time = last_crawl.get("CompletedOn") or datetime.now(timezone.utc)

            LOGGER.info(
                "Crawler '%s' finished. Status=%s StartTime=%s EndTime=%s",
                crawler_name,
                status,
                last_crawl.get("StartTime"),
                end_time,
            )

            # Tables created/updated are not directly returned by get_crawler.
            # We log catalog info that can be used to inspect results in Glue.
            database_name = crawler.get("DatabaseName")
            LOGGER.info(
                "Crawler '%s' is associated with Glue database '%s'. "
                "Inspect the Glue Data Catalog for created/updated tables.",
                crawler_name,
                database_name,
            )

            if log_group and log_stream:
                LOGGER.info(
                    "Crawler logs available in CloudWatch Logs: logGroup=%s, logStream=%s",
                    log_group,
                    log_stream,
                )

            if status == "SUCCEEDED":
                # Successful completion
                if error_message:
                    LOGGER.warning(
                        "Crawler '%s' completed with warnings: %s",
                        crawler_name,
                        error_message,
                    )
                return

            # Any non-SUCCEEDED status is considered a failure for Airflow
            message = (
                f"Glue crawler '{crawler_name}' failed with status={status}, "
                f"error={error_message}"
            )
            LOGGER.error(message)
            raise RuntimeError(message)

        if state == "FAILED":
            error_message = last_crawl.get("ErrorMessage")
            message = (
                f"Glue crawler '{crawler_name}' is in FAILED state. "
                f"LastCrawlStatus={last_crawl.get('Status')}, error={error_message}"
            )
            LOGGER.error(message)
            raise RuntimeError(message)

        # Still running or stopping; wait and poll again
        LOGGER.debug(
            "Crawler '%s' state=%s, waiting %s seconds before next check.",
            crawler_name,
            state,
            poll_interval_seconds,
        )
        time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    # Allow running as a standalone script for manual testing.
    try:
        run_glue_crawler()
    finally:
        # Upload logs generated by this ingestion script to S3.
        upload_logs_to_s3()

