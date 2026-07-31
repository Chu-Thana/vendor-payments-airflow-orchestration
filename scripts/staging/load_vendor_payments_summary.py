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

INPUT_FILE = (
    Path(BASE_PATH)
    / "data/processed/vendor_payments_streaming_cleaned.csv"
)

OUTPUT_FILE = (
    Path(BASE_PATH)
    / "data/warehouse/vendor_payments_summary_by_department.csv"
)

logger = logging.getLogger(__name__)


def load_vendor_payments_summary() -> str:
    """
    Aggregate cleaned vendor payment events into a
    warehouse-ready department summary.
    """

    logger.info(
        "Start load_staging_sales_summary"
    )

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    df = pd.read_csv(INPUT_FILE)

    expected_columns = {
        "department",
        "payment_amount",
        "business_composite_key",
        "event_id",
    }

    missing_expected = (
        expected_columns - set(df.columns)
    )

    if missing_expected:
        raise ValueError(
            "Schema mismatch. Missing columns: "
            f"{sorted(missing_expected)}"
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

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary.to_csv(
        OUTPUT_FILE,
        index=False,
    )

    logger.info(
        f"Loaded {len(df)} rows into warehouse"
    )

    s3_bucket = (
        "sales-analytics-lakehouse-thana"
    )

    s3_key = (
        "gold/vendor_payments_summary_by_department.csv"
    )

    upload_to_s3(
        str(OUTPUT_FILE),
        s3_bucket,
        s3_key,
    )

    return str(OUTPUT_FILE)


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


if __name__ == "__main__":
    load_vendor_payments_summary()