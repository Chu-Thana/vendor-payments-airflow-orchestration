import json

import pytest

from dags import vendor_payments_data_platform_orchestration as orchestration


def build_valid_summary() -> dict:
    return {
        "execution": {
            "runtime_seconds": 12.5,
            "status": "PASS",
        },
        "redshift": {
            "region": "ap-southeast-1",
            "workgroup": "default-workgroup",
            "database": "dev",
            "architecture": {
                "landing_schema": "landing",
                "analytics_schema": "analytics",
            },
        },
        "batch": {
            "landing_table_count": 5,
            "landing_total_rows": 100,
            "landing_tables": [],
            "analytics_view_count": 5,
            "validation_status": "PASS",
        },
        "streaming": {
            "landing_table_count": 1,
            "landing_metrics": {
                "total_rows": 10,
                "rows_with_event_id": 10,
                "distinct_event_ids": 10,
                "duplicate_event_ids": 0,
                "missing_event_ids": 0,
            },
            "analytics_view_count": 4,
            "analytics_metrics": {
                "fiscal_year_rows": 2,
                "total_events": 10,
                "total_distinct_events": 10,
                "total_payment_amount": "1000.00",
            },
            "validation_status": "PASS",
        },
        "validation": {
            "status": "PASS",
        },
    }


def write_summary(tmp_path, summary: dict):
    summary_file = tmp_path / "redshift_execution_summary.json"
    summary_file.write_text(
        json.dumps(summary),
        encoding="utf-8",
    )
    return summary_file


def test_validate_redshift_summary_passes(
    tmp_path,
    monkeypatch,
) -> None:
    summary_file = write_summary(
        tmp_path,
        build_valid_summary(),
    )

    monkeypatch.setattr(
        orchestration,
        "PROJECT5_REDSHIFT_SUMMARY",
        summary_file,
    )

    result = orchestration.validate_redshift_execution_summary()

    assert result["available"] is True
    assert result["execution_status"] == "PASS"
    assert result["validation_status"] == "PASS"
    assert result["batch"]["landing_table_count"] == 5
    assert result["streaming"]["analytics_view_count"] == 4


def test_validate_redshift_summary_detects_duplicates(
    tmp_path,
    monkeypatch,
) -> None:
    summary = build_valid_summary()
    summary["streaming"]["landing_metrics"][
        "duplicate_event_ids"
    ] = 1

    summary_file = write_summary(tmp_path, summary)

    monkeypatch.setattr(
        orchestration,
        "PROJECT5_REDSHIFT_SUMMARY",
        summary_file,
    )

    with pytest.raises(
        ValueError,
        match="Duplicate event IDs were detected",
    ):
        orchestration.validate_redshift_execution_summary()


def test_validate_redshift_summary_detects_missing_event_ids(
    tmp_path,
    monkeypatch,
) -> None:
    summary = build_valid_summary()
    summary["streaming"]["landing_metrics"][
        "missing_event_ids"
    ] = 1

    summary_file = write_summary(tmp_path, summary)

    monkeypatch.setattr(
        orchestration,
        "PROJECT5_REDSHIFT_SUMMARY",
        summary_file,
    )

    with pytest.raises(
        ValueError,
        match="Missing event IDs were detected",
    ):
        orchestration.validate_redshift_execution_summary()


def test_validate_redshift_summary_detects_view_count_error(
    tmp_path,
    monkeypatch,
) -> None:
    summary = build_valid_summary()
    summary["batch"]["analytics_view_count"] = 4

    summary_file = write_summary(tmp_path, summary)

    monkeypatch.setattr(
        orchestration,
        "PROJECT5_REDSHIFT_SUMMARY",
        summary_file,
    )

    with pytest.raises(
        ValueError,
        match="Expected 5 Batch analytics views",
    ):
        orchestration.validate_redshift_execution_summary()


def test_validate_redshift_summary_requires_file(
    tmp_path,
    monkeypatch,
) -> None:
    missing_file = tmp_path / "missing_summary.json"

    monkeypatch.setattr(
        orchestration,
        "PROJECT5_REDSHIFT_SUMMARY",
        missing_file,
    )

    with pytest.raises(
        FileNotFoundError,
        match="Redshift execution summary not found",
    ):
        orchestration.validate_redshift_execution_summary()