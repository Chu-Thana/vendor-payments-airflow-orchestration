from __future__ import annotations

from pathlib import Path
import os
import logging

import boto3
import pandas as pd

BASE_PATH = os.getenv("AIRFLOW_DATA_PATH", "/opt/airflow")

INPUT_FILE = (
    Path(BASE_PATH)
    / "data/processed/vendor_payments_streaming_extracted.csv"
)

OUTPUT_FILE = (
    Path(BASE_PATH)
    / "data/processed/vendor_payments_streaming_cleaned.csv"
)

SILVER_S3_BUCKET = "sales-analytics-lakehouse-thana"
SILVER_S3_KEY = (
    "silver/vendor_payments_streaming_cleaned.csv"
)

logger = logging.getLogger(__name__)


def upload_to_s3(local_path: str, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)
    logger.info(f"Uploaded {local_path} to s3://{bucket}/{key}")


def transform_vendor_payments_staging() -> str:
    """
    Transform extracted staging data into silver layer.

    Steps:
    1. Validate schema
    2. Cast data types
    3. Drop invalid rows
    4. Filter duplicate rows flagged by Kafka consumer
    5. Apply secondary deduplication by event_id in Airflow
    6. Write cleaned silver dataset
    7. Upload silver dataset to S3
    """

    logger.info("Start transform_staging_sales")

    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)
    raw_count = len(df)
    logger.info(f"Loaded {raw_count} rows from extracted staging file")

    required_columns = {
        "event_id",
        "event_type",
        "event_timestamp",
        "source_system",
        "source_row_hash",
        "business_composite_key",
        "fiscal_year",
        "supplier_name",
        "department",
        "payment_amount",
        "dedup_status",
    }

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        logger.error(
            f"Schema mismatch. Missing columns: "
            f"{sorted(missing_columns)}"
        )
        raise ValueError(
            f"Schema mismatch. Missing columns: "
            f"{sorted(missing_columns)}"
        )

    # Type casting
    df["payment_amount"] = pd.to_numeric(
        df["payment_amount"],
        errors="coerce",
    )

    df["fiscal_year"] = pd.to_numeric(
        df["fiscal_year"],
        errors="coerce",
    )

    df["event_timestamp"] = pd.to_datetime(
        df["event_timestamp"],
        errors="coerce",
        utc=True,
    )

    # 1) Drop invalid rows
    before_invalid_filter = len(df)

    df = df.dropna(
        subset=[
            "event_id",
            "event_type",
            "event_timestamp",
            "source_system",
            "source_row_hash",
            "business_composite_key",
            "fiscal_year",
            "supplier_name",
            "department",
            "payment_amount",
            "dedup_status",
        ]
    )

    invalid_dropped = before_invalid_filter - len(df)

    # 2) Keep records accepted by Kafka consumer
    before_flag_filter = len(df)

    df = df[df["dedup_status"] == "accepted"]

    flagged_duplicate_dropped = before_flag_filter - len(df)

    logger.info(
        "Applying secondary deduplication by event_id in Airflow"
    )

    # 3) Secondary deduplication in Airflow
    before_secondary_dedup = len(df)

    df = df.drop_duplicates(
        subset=["event_id"],
        keep="first",
    )

    secondary_duplicate_dropped = (
            before_secondary_dedup - len(df)
    )

    final_count = len(df)

    remaining_duplicates = df["event_id"].duplicated().sum()
    if remaining_duplicates > 0:
        raise ValueError(f"Final duplicate check failed: {remaining_duplicates} duplicates remain")

    logger.info(f"Raw rows before cleaning: {raw_count}")
    logger.info(f"Invalid rows dropped: {invalid_dropped}")
    logger.info(f"Non-accepted records dropped by dedup status: {flagged_duplicate_dropped}")
    logger.info(f"Duplicates dropped by Airflow secondary dedup: {secondary_duplicate_dropped}")
    logger.info(f"Final clean rows after transform: {final_count}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"Cleaned file written to: {OUTPUT_FILE}")

    upload_to_s3(
        str(OUTPUT_FILE),
        SILVER_S3_BUCKET,
        SILVER_S3_KEY,
    )

    logger.info("Transform completed successfully")
    return str(OUTPUT_FILE)


if __name__ == "__main__":
    transform_vendor_payments_staging()