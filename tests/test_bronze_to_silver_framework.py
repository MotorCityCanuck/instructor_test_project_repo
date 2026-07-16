"""Tests for Bronze-to-Silver shared framework helpers."""

from datetime import date, datetime

from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.io import (
    get_bronze_source_table_fqn,
    get_enabled_sources_by_name,
    get_silver_reject_table_fqn,
    get_silver_target_table_fqn,
)
from napa_pipeline.bronze_to_silver.metadata import (
    STANDARD_METADATA_COLUMNS,
    build_metadata_payload,
    build_record_hash,
    build_source_record_json,
)
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context
from napa_pipeline.bronze_to_silver.orchestration import get_silver_tables_by_stage
from napa_pipeline.bronze_to_silver.quality import (
    build_reject_record,
    calculate_quality_score,
    resolve_quality_status,
)
from napa_pipeline.bronze_to_silver.reconciliation import reconcile_table_counts
from napa_pipeline.bronze_to_silver.transforms import (
    normalize_domain_value,
    safe_cast_date,
    safe_cast_decimal,
    safe_cast_float,
    safe_cast_int,
    safe_cast_timestamp,
    standardize_string,
    to_snake_case,
)
from napa_pipeline.bronze_to_silver.validation import (
    calculate_failure_pct,
    validate_required_fields,
    validate_source_contract,
)


def _context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    return config, environment, create_pipeline_context(
        config,
        environment,
        pipeline_run_id="run-123",
    )


def test_io_helpers_build_release_specific_table_names() -> None:
    config, environment, _ = _context()
    source_config = config.enabled_sources["regions"]
    table_config = config.data["silver_tables"]["regions"]

    assert get_bronze_source_table_fqn(environment, source_config) == (
        "workspace.instructor_5k_bronze.regions"
    )
    assert get_silver_target_table_fqn(environment, table_config) == (
        "workspace.instructor_5k_silver.regions"
    )
    assert get_silver_reject_table_fqn(environment, table_config) == (
        "workspace.instructor_5k_silver_reject.regions_exceptions"
    )


def test_enabled_sources_and_stage_grouping_follow_configuration() -> None:
    config, _, _ = _context()

    enabled_sources = get_enabled_sources_by_name(config)
    grouped_tables = get_silver_tables_by_stage(config)

    assert len(enabled_sources) == 13
    assert grouped_tables["reference"][0]["table_name"] == "monthly_batches"
    assert grouped_tables["competition"][-1]["table_name"] == "match_games"


def test_metadata_helpers_build_stable_hash_and_payload() -> None:
    _, _, context = _context()
    record = {"player_id": "P1", "player_name": "Jane Doe"}
    record_hash = build_record_hash(record, ["player_id", "player_name"])
    payload = build_metadata_payload(context, "player_master", record_hash)

    assert set(STANDARD_METADATA_COLUMNS).issubset(payload.keys())
    assert payload["_record_hash"] == record_hash
    assert build_record_hash(record, ["player_id", "player_name"]) == record_hash


def test_build_source_record_json_is_stable() -> None:
    json_value = build_source_record_json({"b": 2, "a": 1})

    assert json_value == '{"a":1,"b":2}'


def test_string_and_domain_standardization_helpers() -> None:
    config, _, _ = _context()

    assert to_snake_case("PlayerID") == "player_id"
    assert standardize_string("  usa \r\n") == "usa"
    assert standardize_string("  usa \r\n", uppercase=True) == "USA"
    assert (
        normalize_domain_value("United States", config.data["domains"]["country_code"])
        == "USA"
    )
    assert normalize_domain_value("Unknown", config.data["domains"]["country_code"]) is None


def test_safe_cast_helpers_support_common_scalar_types() -> None:
    assert safe_cast_int("12") == 12
    assert safe_cast_float("12.5") == 12.5
    assert str(safe_cast_decimal("12.50")) == "12.50"
    assert safe_cast_date("2026-07-15") == date(2026, 7, 15)
    assert safe_cast_timestamp("2026-07-15T13:45:00") == datetime(2026, 7, 15, 13, 45, 0)


def test_validation_helpers_report_contract_and_required_field_gaps() -> None:
    result = validate_source_contract(
        source_columns=["id", "name", "extra"],
        required_columns=["id", "region_id"],
        expected_columns=["id", "name"],
    )

    assert result.missing_required_columns == ("region_id",)
    assert result.unexpected_columns == ("extra",)
    assert result.status == "FAILED"
    assert validate_required_fields({"id": "1", "name": ""}, ["id", "name"]) == ["name"]
    assert calculate_failure_pct(100, 3) == 3.0


def test_quality_helpers_build_rejects_and_scores() -> None:
    _, _, context = _context()
    reject_record = build_reject_record(
        context,
        source_table="player_master",
        target_table="players",
        source_business_key="P1",
        reject_reason="INVALID_DOMAIN_VALUE",
        rule_id="PLAYER_003",
        rule_severity="ERROR",
        source_record={"player_id": "P1", "country_code": "X"},
    )

    assert resolve_quality_status("WARNING") == "WARNING"
    assert resolve_quality_status("ERROR") == "REJECTED"
    assert reject_record["source_table"] == "player_master"
    assert reject_record["rule_id"] == "PLAYER_003"
    assert calculate_quality_score(3, 5) == 85


def test_reconciliation_helper_balances_counts() -> None:
    summary = reconcile_table_counts(
        bronze_row_count=100,
        exact_duplicate_count=1,
        business_key_loser_count=2,
        rejected_row_count=3,
        accepted_row_count=94,
    )

    assert summary.reconciliation_difference == 0
    assert summary.status == "PASSED"
