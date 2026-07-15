"""Tests for Raw-to-Bronze operations helpers."""

from datetime import datetime, timezone

from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.operations import (
    PIPELINE_RUNS_TABLE,
    RUN_MESSAGES_TABLE,
    SCHEMA_SNAPSHOTS_TABLE,
    TABLE_RUNS_TABLE,
    RECONCILIATION_RESULTS_TABLE,
    build_pipeline_run_end_record,
    build_pipeline_run_start_record,
    build_reconciliation_record,
    build_run_message_record,
    build_schema_snapshot_records,
    build_table_run_end_record,
    build_table_run_start_record,
    calculate_schema_hash,
    create_pipeline_context,
    ensure_operations_tables,
)


class FakeSparkSession:
    """Capture SQL statements executed by operations helpers."""

    def __init__(self):
        self.executed_queries: list[str] = []

    def sql(self, query: str):
        self.executed_queries.append(query)
        return None


def _context():
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    return create_pipeline_context(config, environment, pipeline_run_id="run-123")


def test_ensure_operations_tables_emits_all_expected_ddls() -> None:
    context = _context()
    spark = FakeSparkSession()

    ensure_operations_tables(spark, context)

    joined = "\n".join(spark.executed_queries)
    assert f".{PIPELINE_RUNS_TABLE}" in joined
    assert f".{TABLE_RUNS_TABLE}" in joined
    assert f".{SCHEMA_SNAPSHOTS_TABLE}" in joined
    assert f".{RECONCILIATION_RESULTS_TABLE}" in joined
    assert f".{RUN_MESSAGES_TABLE}" in joined


def test_build_pipeline_run_records_include_duration_and_status() -> None:
    context = _context()
    started = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
    completed = datetime(2026, 7, 15, 12, 5, tzinfo=timezone.utc)

    start_record = build_pipeline_run_start_record(context, started_ts=started)
    end_record = build_pipeline_run_end_record(
        context,
        started_ts=started,
        completed_ts=completed,
        status="SUCCEEDED",
    )

    assert start_record["status"] == "RUNNING"
    assert end_record["status"] == "SUCCEEDED"
    assert end_record["duration_seconds"] == 300.0


def test_build_table_run_end_record_calculates_row_difference() -> None:
    context = _context()
    started = datetime(2026, 7, 15, 13, 0, tzinfo=timezone.utc)
    completed = datetime(2026, 7, 15, 13, 2, 30, tzinfo=timezone.utc)

    record = build_table_run_end_record(
        context,
        source_file_name="regions.parquet",
        source_table="regions",
        target_table="workspace.instructor_5k_bronze.regions",
        started_ts=started,
        completed_ts=completed,
        status="SUCCEEDED",
        source_row_count=10,
        bronze_row_count=10,
    )

    assert record["row_count_difference"] == 0
    assert record["duration_seconds"] == 150.0


def test_build_schema_snapshot_records_share_schema_hash() -> None:
    context = _context()
    fields = [
        {"column_name": "id", "data_type": "string", "nullable": False},
        {"column_name": "name", "data_type": "string", "nullable": True},
    ]

    records = build_schema_snapshot_records(
        context,
        layer_name="raw",
        object_name="regions.parquet",
        schema_fields=fields,
    )

    assert len(records) == 2
    assert records[0]["schema_hash"] == records[1]["schema_hash"]
    assert records[0]["ordinal_position"] == 1
    assert records[1]["ordinal_position"] == 2


def test_calculate_schema_hash_is_deterministic() -> None:
    fields = [
        {"column_name": "id", "data_type": "string", "nullable": False},
        {"column_name": "name", "data_type": "string", "nullable": True},
    ]

    assert calculate_schema_hash(fields) == calculate_schema_hash(fields)


def test_build_reconciliation_and_message_records() -> None:
    context = _context()

    reconciliation = build_reconciliation_record(
        context,
        source_file_name="regions.parquet",
        bronze_table="regions",
        raw_row_count=100,
        bronze_row_count=98,
        raw_business_column_count=5,
        bronze_business_column_count=5,
        metadata_column_count=10,
    )
    message = build_run_message_record(
        context,
        message_level="ERROR",
        message_code="ROW_COUNT_MISMATCH",
        message_text="Bronze row count does not match Raw row count.",
        source_name="regions",
    )

    assert reconciliation["status"] == "MISMATCH"
    assert reconciliation["row_count_difference"] == -2
    assert message["source_name"] == "regions"
    assert message["message_code"] == "ROW_COUNT_MISMATCH"


def test_build_table_run_start_record_sets_running_status() -> None:
    context = _context()
    record = build_table_run_start_record(
        context,
        source_file_name="clubs.parquet",
        source_table="clubs",
        target_table="workspace.instructor_5k_bronze.clubs",
    )

    assert record["status"] == "RUNNING"
    assert record["source_file_name"] == "clubs.parquet"
