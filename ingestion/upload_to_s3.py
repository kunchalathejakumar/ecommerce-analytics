import argparse
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


DATE_FORMAT = "%Y-%m-%d"


@dataclass
class FileUploadResult:
    filename: str
    table_name: str
    local_path: str
    s3_path: str
    file_size_mb: float
    row_count: int
    duration_seconds: float
    status: str
    error: Optional[str] = None


def load_environment() -> None:
    """
    Load environment variables from .env if present.
    """
    load_dotenv()


def get_bucket_name(explicit_bucket: Optional[str] = None) -> str:
    bucket = explicit_bucket or os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError(
            "S3 bucket name not provided. "
            "Set S3_BUCKET in your .env or use --bucket."
        )
    return bucket


def iter_csv_files(raw_dir: Path) -> List[Path]:
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory does not exist: {raw_dir}")
    return sorted(p for p in raw_dir.glob("*.csv") if p.is_file())


def table_name_from_filename(path: Path) -> str:
    return path.stem


def compute_file_stats(path: Path) -> Dict[str, Any]:
    size_mb = path.stat().st_size / (1024 * 1024)
    # Count rows excluding header
    row_count = 0
    with path.open("r", encoding="utf-8") as f:
        for i, _ in enumerate(f, start=1):
            pass
    if i == 0:
        row_count = 0
    else:
        row_count = max(i - 1, 0)
    return {"file_size_mb": size_mb, "row_count": row_count}


def object_exists(s3_client: Any, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_with_retries(
    s3_client: Any,
    bucket: str,
    key: str,
    local_path: Path,
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
) -> None:
    attempt = 0
    while True:
        attempt += 1
        try:
            s3_client.upload_file(str(local_path), bucket, key)
            return
        except (ClientError, BotoCoreError) as exc:
            if attempt >= max_attempts:
                raise exc
            sleep_for = delay_seconds * (2 ** (attempt - 1))
            logging.warning(
                "Upload attempt %s failed for %s, retrying in %.1fs: %s",
                attempt,
                local_path,
                sleep_for,
                exc,
            )
            time.sleep(sleep_for)


def upload_file(
    s3_client: Any,
    bucket: str,
    file_path: Path,
    base_prefix: str,
    run_date: str,
) -> FileUploadResult:
    table_name = table_name_from_filename(file_path)
    key = f"{base_prefix}/{table_name}/{run_date}/{file_path.name}"
    s3_path = f"s3://{bucket}/{key}"

    stats = compute_file_stats(file_path)
    file_size_mb = float(stats["file_size_mb"])
    row_count = int(stats["row_count"])

    logging.info("Processing %s -> %s", file_path, s3_path)

    start_time = time.time()
    status = "uploaded"
    error: Optional[str] = None

    try:
        if object_exists(s3_client, bucket, key):
            status = "skipped_exists"
            logging.info("Skipping existing object for today: %s", s3_path)
        else:
            upload_with_retries(s3_client, bucket, key, file_path)
            logging.info("Uploaded %s to %s", file_path, s3_path)
    except (ClientError, BotoCoreError) as exc:
        status = "failed"
        error = str(exc)
        logging.error("Failed to upload %s: %s", file_path, exc)

    duration = time.time() - start_time

    return FileUploadResult(
        filename=file_path.name,
        table_name=table_name,
        local_path=str(file_path),
        s3_path=s3_path,
        file_size_mb=round(file_size_mb, 4),
        row_count=row_count,
        duration_seconds=round(duration, 3),
        status=status,
        error=error,
    )


def write_manifest(
    s3_client: Any,
    bucket: str,
    run_date: str,
    results: List[FileUploadResult],
) -> str:
    manifest_key = f"logs/ingestion/{run_date}_manifest.json"
    manifest_s3_path = f"s3://{bucket}/{manifest_key}"

    summary = {
        "date": run_date,
        "bucket": bucket,
        "total_files": len(results),
        "uploaded": sum(1 for r in results if r.status == "uploaded"),
        "skipped": sum(1 for r in results if r.status.startswith("skipped")),
        "failed": sum(1 for r in results if r.status == "failed"),
        "files": [asdict(r) for r in results],
    }

    body = json.dumps(summary, indent=2)
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    logging.info("Wrote manifest to %s", manifest_s3_path)
    return manifest_s3_path


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload raw CSV files to S3 in partitioned layout."
    )
    parser.add_argument(
        "--raw-dir",
        default="data/raw",
        help="Local directory containing raw CSV files (default: data/raw).",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Target S3 bucket name (overrides S3_BUCKET environment variable).",
    )
    parser.add_argument(
        "--prefix",
        default="raw",
        help="Base S3 prefix under the bucket (default: raw).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Partition date in YYYY-MM-DD format (default: today).",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="AWS region name (default: from AWS_REGION or boto3 config).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    load_environment()
    args = parse_args(argv)
    configure_logging(args.verbose)

    bucket = get_bucket_name(args.bucket)
    raw_dir = Path(args.raw_dir)
    base_prefix = args.prefix.rstrip("/")
    run_date = (
        args.date
        if args.date is not None
        else datetime.utcnow().strftime(DATE_FORMAT)
    )

    logging.info("Starting upload of raw CSV files to S3")
    logging.info("  Bucket   : %s", bucket)
    logging.info("  Raw dir  : %s", raw_dir.resolve())
    logging.info("  Prefix   : %s", base_prefix)
    logging.info("  Date     : %s", run_date)

    try:
        csv_files = iter_csv_files(raw_dir)
    except FileNotFoundError as exc:
        logging.error("%s", exc)
        return

    if not csv_files:
        logging.info("No CSV files found in %s, nothing to upload.", raw_dir)
        return

    session_kwargs: Dict[str, Any] = {}
    if args.region:
        session_kwargs["region_name"] = args.region

    session = boto3.session.Session(**session_kwargs)
    s3_client = session.client("s3")

    results: List[FileUploadResult] = []
    for file_path in csv_files:
        result = upload_file(
            s3_client=s3_client,
            bucket=bucket,
            file_path=file_path,
            base_prefix=base_prefix,
            run_date=run_date,
        )
        results.append(result)

        logging.info(
            "Result for %s - status=%s size_mb=%.4f rows=%d s3_path=%s duration=%.3fs",
            result.filename,
            result.status,
            result.file_size_mb,
            result.row_count,
            result.s3_path,
            result.duration_seconds,
        )

    try:
        manifest_path = write_manifest(
            s3_client=s3_client,
            bucket=bucket,
            run_date=run_date,
            results=results,
        )
        logging.info("Manifest written to %s", manifest_path)
    except (ClientError, BotoCoreError) as exc:
        logging.error("Failed to write manifest: %s", exc)


if __name__ == "__main__":
    main()

