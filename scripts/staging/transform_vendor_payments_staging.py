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
    Transform extracted vendor payment staging data into a cleaned silver dataset.

    Steps:
    1. Validate the required schema
    2. Cast columns to the expected data types
    3. Remove invalid rows
    4. Write the cleaned silver dataset
    5. Upload the silver dataset to Amazon S3

    Returns:
        The local output file path as a string.
    """

    logger.info("Starting vendor payments staging transformation")

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
        ]
    )

    invalid_dropped = before_invalid_filter - len(df)
    final_count = len(df)

    logger.info(f"Raw rows before cleaning: {raw_count}")
    logger.info(f"Invalid rows dropped: {invalid_dropped}")
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