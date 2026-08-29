from __future__ import annotations

from pathlib import Path
import logging
import os

import boto3
import pandas as pd


BASE_PATH = os.getenv(
    "AIRFLOW_DATA_PATH",
    "/opt/airflow",
)

logger = logging.getLogger(__name__)


def load_vendor_payments_summary(
    input_file: str,
    window_id: str,
) -> str:
    """
    Aggregate cleaned vendor payment events into a
    warehouse-ready department summary.
    """

    input_path = Path(input_file)

    output_file = (
            Path(BASE_PATH)
            / "data"
            / "warehouse"
            / window_id
            / "vendor_payments_summary_by_department.csv"
    )

    s3_bucket = "sales-analytics-lakehouse-thana"

    s3_key = (
        f"gold/streaming/{window_id}/"
        "vendor_payments_summary_by_department.csv"
    )

    logger.info(
        "Starting vendor payments summary load"
    )

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    df = pd.read_csv(input_path)

    required_columns = {
        "window_id",
        "department",
        "payment_amount",
        "business_composite_key",
        "event_id",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            "Schema mismatch. Missing columns: "
            f"{sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError(
            "No clean rows available for summary."
        )

    if df["department"].isna().any():
        raise ValueError(
            "Missing department detected "
            "in cleaned input"
        )

    summary = (
        df.groupby(
            "department",
            as_index=False,
        )
        .agg(
            total_payment_amount=(
                "payment_amount",
                "sum",
            ),
            total_business_records=(
                "business_composite_key",
                "nunique",
            ),
            total_events=(
                "event_id",
                "count",
            ),
        )
        .sort_values(
            by="total_payment_amount",
            ascending=False,
        )
    )

    logger.info(
        f"Input clean rows: {len(df)}"
    )
    logger.info(
        f"Output summary rows: {len(summary)}"
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary.to_csv(
        output_file,
        index=False,
    )

    logger.info(
        f"Loaded {len(summary)} summary rows into warehouse"
    )

    s3_bucket = (
        "sales-analytics-lakehouse-thana"
    )

    s3_key = (
        "gold/vendor_payments_summary_by_department.csv"
    )

    upload_to_s3(
        str(output_file),
        s3_bucket,
        s3_key,
    )

    return str(output_file)


def upload_to_s3(
    local_path: str,
    bucket: str,
    key: str,
) -> None:
    s3 = boto3.client("s3")

    s3.upload_file(
        local_path,
        bucket,
        key,
    )

    logger.info(
        f"Uploaded {local_path} "
        f"to s3://{bucket}/{key}"
    )
