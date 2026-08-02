from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pandas as pd

BASE_PATH = os.getenv("AIRFLOW_DATA_PATH", "/opt/airflow")
STAGING_FILE = (
    Path(BASE_PATH)
    / "staging/vendor_payments_streaming_staging.jsonl"
)

OUTPUT_FILE = (
    Path(BASE_PATH)
    / "data/processed/vendor_payments_streaming_extracted.csv"
)

logger = logging.getLogger(__name__)


def extract_vendor_payments_staging() -> str:
    """
    Read raw JSONL events from staging and convert them to CSV for downstream tasks.
    Returns output file path.
    """
    logger.info(f"Reading staging file from {STAGING_FILE}")

    if not STAGING_FILE.exists():
        raise FileNotFoundError(f"Staging file not found: {STAGING_FILE}")

    rows = []
    bad_rows = 0

    with open(STAGING_FILE, "r", encoding="utf-8-sig", errors="replace") as f:
        for raw_line in f:

            line = raw_line.strip()
            if not line:
                continue

            # กัน BOM / replacement char / hidden char หน้าไฟล์
            line = line.lstrip("\ufeff").replace("\ufffd", "")

            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                bad_rows += 1
                logger.warning(f"Skip bad JSON line: {line[:200]}")
                logger.warning(f"JSON error: {e}")

    logger.info(f"Extracted {len(rows)} valid rows, skipped {bad_rows} bad rows")

    if not rows:
        raise ValueError("Staging file exists but contains no records.")

    df = pd.DataFrame(rows)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    logger.info(f"Saved extracted CSV to {OUTPUT_FILE}")
    logger.info(f"Extracted {len(df)} rows")

    return str(OUTPUT_FILE)


if __name__ == "__main__":
    extract_vendor_payments_staging()