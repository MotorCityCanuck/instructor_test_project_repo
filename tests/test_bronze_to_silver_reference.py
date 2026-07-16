"""Tests for Bronze-to-Silver reference table builders."""

from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context
from napa_pipeline.bronze_to_silver.reference import (
    build_monthly_batches,
    build_regions,
)


def _context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    return config, create_pipeline_context(config, environment, pipeline_run_id="run-123")


def test_build_monthly_batches_accepts_valid_row_and_derives_calendar_fields() -> None:
    config, context = _context()
    result = build_monthly_batches(
        [
            {
                "id": "batch-2026-06",
                "batch_sequence": "6",
                "batch_date": "2026-06-30",
                "status": "final",
            }
        ],
        config,
        context,
    )

    assert result.reconciliation.status == "PASSED"
    assert len(result.accepted_rows) == 1
    row = result.accepted_rows[0]
    assert row["batch_id"] == "batch-2026-06"
    assert row["batch_year"] == 2026
    assert row["batch_month"] == 6
    assert row["batch_quarter"] == 2
    assert row["batch_status"] == "FINAL"
    assert row["_source_table"] == "monthly_batches"


def test_build_monthly_batches_rejects_invalid_date() -> None:
    config, context = _context()
    result = build_monthly_batches(
        [
            {
                "id": "batch-2026-06",
                "batch_date": "2026-99-99",
            }
        ],
        config,
        context,
    )

    assert len(result.accepted_rows) == 0
    assert len(result.rejected_rows) == 1
    assert result.rejected_rows[0]["reject_reason"] == "INVALID_DATE"
    assert result.reconciliation.status == "PASSED"


def test_build_monthly_batches_resolves_duplicate_keys_by_completeness() -> None:
    config, context = _context()
    result = build_monthly_batches(
        [
            {"id": "batch-1", "batch_date": "2026-01-31"},
            {
                "id": "batch-1",
                "batch_sequence": "1",
                "batch_date": "2026-01-31",
                "batch_type": "monthly",
            },
        ],
        config,
        context,
    )

    assert len(result.accepted_rows) == 1
    assert len(result.rejected_rows) == 1
    assert result.business_key_duplicate_count == 1
    assert result.rejected_rows[0]["reject_reason"] == "DUPLICATE_BUSINESS_KEY"


def test_build_regions_accepts_valid_row_and_normalizes_country() -> None:
    config, context = _context()
    result = build_regions(
        [
            {
                "id": "region-1",
                "name": "Ontario West",
                "province": " on ",
                "country": "Canada",
                "status": "active",
            }
        ],
        config,
        context,
    )

    assert result.reconciliation.status == "PASSED"
    assert len(result.accepted_rows) == 1
    row = result.accepted_rows[0]
    assert row["region_id"] == "region-1"
    assert row["region_name"] == "Ontario West"
    assert row["province_state"] == "ON"
    assert row["country_code"] == "CAN"
    assert row["active_flag"] is True
    assert row["_source_table"] == "regions"


def test_build_regions_rejects_invalid_country() -> None:
    config, context = _context()
    result = build_regions(
        [
            {
                "id": "region-1",
                "name": "Atlantis",
                "country": "Atlantis",
            }
        ],
        config,
        context,
    )

    assert len(result.accepted_rows) == 0
    assert len(result.rejected_rows) == 1
    assert result.rejected_rows[0]["reject_reason"] == "INVALID_DOMAIN_VALUE"


def test_build_regions_removes_exact_duplicates_before_processing() -> None:
    config, context = _context()
    source_rows = [
        {
            "id": "region-1",
            "name": "Ontario West",
            "country": "Canada",
        },
        {
            "id": "region-1",
            "name": "Ontario West",
            "country": "Canada",
        },
    ]

    result = build_regions(source_rows, config, context)

    assert len(result.accepted_rows) == 1
    assert result.exact_duplicate_count == 1
    assert result.business_key_duplicate_count == 0
    assert result.reconciliation.status == "PASSED"
