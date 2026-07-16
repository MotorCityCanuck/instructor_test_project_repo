"""Tests for Bronze-to-Silver operations helpers."""

from datetime import datetime, timezone

from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.operations import (
    PIPELINE_RUNS_TABLE,
    QUALITY_RESULTS_TABLE,
    RECONCILIATION_RESULTS_TABLE,
    RUN_MESSAGES_TABLE,
    SCHEMA_SNAPSHOTS_TABLE,
    TABLE_RUNS_TABLE,
    append_records,
    build_pipeline_run_end_record,
    build_pipeline_run_start_record,
    build_quality_result_record,
    build_reconciliation_record,
    build_run_message_record,
    build_schema_snapshot_records,
    build_table_run_end_record,
    build_table_run_start_record,
    calculate_schema_hash,
    create_pipeline_context,
    ensure_operations_tables,
)
import pytest


class FakeSparkSession:
    """Capture SQL statements executed by operations helpers."""

    def __init__(self):
        self.executed_queries: list[str] = []
        self.table_requests: list[str] = []
        self.created_records: list[dict] | None = None
        self.created_schema = None

    def sql(self, query: str):
        self.executed_queries.append(query)
        return None

    def table(self, table_name: str):
        self.table_requests.append(table_name)
        return type("FakeTable", (), {"schema": "schema-from-table"})()

    def createDataFrame(self, records, schema=None):
        self.created_records = list(records)
        self.created_schema = schema
        return FakeWriteDataFrame()


class FakeWriteBuilder:
    """Minimal DataFrameWriter stub."""

    def __init__(self):
        self.calls: list[tuple[str, str | None]] = []

    def format(self, value: str):
        self.calls.append(("format", value))
        return self

    def mode(self, value: str):
        self.calls.append(("mode", value))
        return self

    def saveAsTable(self, value: str):
        self.calls.append(("saveAsTable", value))
        return None


class FakeWriteDataFrame:
    """Minimal DataFrame stub with writer."""

    def __init__(self):
        self.write = FakeWriteBuilder()


class FakeField:
    def __init__(self, name: str, nullable: bool):
        self.name = name
        self.nullable = nullable


class FakeSchema:
    def __init__(self, fields):
        self.fields = fields


def _context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    return create_pipeline_context(config, environment, pipeline_run_id="run-123")


def test_ensure_operations_tables_emits_all_expected_ddls() -> None:
    context = _context()
    spark = FakeSparkSession()

    ensure_operations_tables(spark, context)

    joined = "\n".join(spark.executed_queries)
    assert f".{PIPELINE_RUNS_TABLE}" in joined
    assert f".{TABLE_RUNS_TABLE}" in joined
    assert f".{QUALITY_RESULTS_TABLE}" in joined
    assert f".{RECONCILIATION_RESULTS_TABLE}" in joined
    assert f".{SCHEMA_SNAPSHOTS_TABLE}" in joined
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

    assert start_record["status"] == "STARTED"
    assert end_record["status"] == "SUCCEEDED"
    assert end_record["duration_seconds"] == 300.0


def test_build_table_run_end_record_captures_counts() -> None:
    context = _context()
    started = datetime(2026, 7, 15, 13, 0, tzinfo=timezone.utc)
    completed = datetime(2026, 7, 15, 13, 2, 30, tzinfo=timezone.utc)

    record = build_table_run_end_record(
        context,
        source_table="regions",
        target_table="workspace.instructor_5k_silver.regions",
        build_stage="reference",
        build_order=20,
        started_ts=started,
        completed_ts=completed,
        status="SUCCEEDED",
        source_row_count=10,
        accepted_row_count=10,
        published_row_count=10,
    )

    assert record["build_stage"] == "reference"
    assert record["build_order"] == 20
    assert record["published_row_count"] == 10
    assert record["duration_seconds"] == 150.0


def test_build_pipeline_run_end_record_accepts_naive_started_timestamp() -> None:
    context = _context()
    started = datetime(2026, 7, 15, 12, 0)
    completed = datetime(2026, 7, 15, 12, 5, tzinfo=timezone.utc)

    record = build_pipeline_run_end_record(
        context,
        started_ts=started,
        completed_ts=completed,
        status="SUCCEEDED",
    )

    assert record["started_ts"].tzinfo == timezone.utc
    assert record["completed_ts"].tzinfo == timezone.utc
    assert record["duration_seconds"] == 300.0


def test_build_quality_and_reconciliation_records() -> None:
    context = _context()

    quality_record = build_quality_result_record(
        context,
        target_table="players",
        rule_id="PLAYER_001",
        rule_type="not_null",
        severity="CRITICAL",
        status="FAILED",
        evaluated_row_count=100,
        failed_row_count=3,
        failure_pct=3.0,
        sample_business_keys=["1", "2"],
    )
    reconciliation_record = build_reconciliation_record(
        context,
        source_table="player_master",
        target_table="players",
        bronze_row_count=100,
        exact_duplicate_count=1,
        business_key_loser_count=2,
        rejected_row_count=3,
        accepted_row_count=94,
    )

    assert quality_record["rule_id"] == "PLAYER_001"
    assert quality_record["sample_business_keys"] == ["1", "2"]
    assert reconciliation_record["reconciliation_difference"] == 0
    assert reconciliation_record["status"] == "PASSED"


def test_build_schema_snapshot_records_share_schema_hash() -> None:
    context = _context()
    fields = [
        {"column_name": "id", "data_type": "string", "nullable": False},
        {"column_name": "name", "data_type": "string", "nullable": True},
    ]

    records = build_schema_snapshot_records(
        context,
        layer_name="silver",
        table_name="regions",
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


def test_build_run_message_record_uses_target_table() -> None:
    context = _context()
    message = build_run_message_record(
        context,
        message_level="ERROR",
        message_code="QUALITY_RULE_FAILED",
        message_text="Player rule failed.",
        target_table="players",
    )

    assert message["target_table"] == "players"
    assert message["message_code"] == "QUALITY_RULE_FAILED"


def test_append_records_uses_existing_table_schema() -> None:
    spark = FakeSparkSession()
    records = [
        {
            "pipeline_run_id": "run-123",
            "release_name": "napa_5k",
            "target_table": "players",
        }
    ]

    append_records(spark, "workspace.instructor_ops.run_messages", records)

    assert spark.table_requests == ["workspace.instructor_ops.run_messages"]
    assert spark.created_records == records
    assert spark.created_schema == "schema-from-table"


def test_append_records_raises_clear_error_for_missing_required_field() -> None:
    class StrictFakeSparkSession(FakeSparkSession):
        def table(self, table_name: str):
            self.table_requests.append(table_name)
            return type(
                "FakeTable",
                (),
                {
                    "schema": FakeSchema(
                        [
                            FakeField("pipeline_run_id", False),
                            FakeField("release_name", False),
                            FakeField("target_table", False),
                        ]
                    )
                },
            )()

    spark = StrictFakeSparkSession()
    records = [
        {
            "pipeline_run_id": "run-123",
            "release_name": None,
            "target_table": "players",
        }
    ]

    with pytest.raises(ValueError, match="required field 'release_name' is null or missing"):
        append_records(spark, "workspace.instructor_ops.run_messages", records)
