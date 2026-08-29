from __future__ import annotations

from pathlib import Path
import os
import logging

import boto3
import pandas as pd

BASE_PATH = os.getenv("AIRFLOW_DATA_PATH", "/opt/airflow")

SILVER_S3_BUCKET = "sales-analytics-lakehouse-thana"

logger = logging.getLogger(__name__)


def upload_to_s3(local_path: str, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)
    logger.info(f"Uploaded {local_path} to s3://{bucket}/{key}")


def transform_vendor_payments_staging(
    input_file: str,
    window_id: str,
) -> str:
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
    input_path = Path(input_file)

    output_file = (
            Path(BASE_PATH)
            / "data"
            / "processed"
            / window_id
            / "vendor_payments_streaming_cleaned.csv"
    )

    silver_s3_key = (
        f"silver/streaming/{window_id}/"
        "vendor_payments_streaming_cleaned.csv"
    )

    logger.info("Starting vendor payments staging transformation")

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    df = pd.read_csv(input_path)

    raw_count = len(df)
    logger.info(f"Loaded {raw_count} rows from extracted staging file")

    required_columns = {
        "event_id",
        "event_type",
        "event_timestamp",
        "source_system",
        "window_id",
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

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_csv(
        output_file,
        index=False,
    )

    logger.info(f"Cleaned file written to: {output_file}")

    upload_to_s3(
        str(output_file),
        SILVER_S3_BUCKET,
        silver_s3_key,
    )

    logger.info("Transform completed successfully")
    return str(output_file)
