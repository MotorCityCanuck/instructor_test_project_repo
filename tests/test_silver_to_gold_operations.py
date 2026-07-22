"""Tests for Silver-to-Gold operations helpers."""

from datetime import date, datetime, timezone

import pytest

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import build_runtime_context, resolve_release_environment
from napa_pipeline.silver_to_gold.operations import (
    MODEL_METRICS_TABLE,
    MODEL_RUNS_TABLE,
    PIPELINE_RUNS_TABLE,
    QUALITY_RESULTS_TABLE,
    RECOMMENDATION_RUNS_TABLE,
    RECONCILIATION_RESULTS_TABLE,
    TABLE_RUNS_TABLE,
    append_records,
    build_model_metric_record,
    build_model_run_end_record,
    build_model_run_start_record,
    build_pipeline_run_end_record,
    build_pipeline_run_start_record,
    build_quality_result_record,
    build_recommendation_run_record,
    build_reconciliation_record,
    build_table_run_end_record,
    build_table_run_start_record,
    calculate_record_hash,
    complete_pipeline_run,
    create_pipeline_context,
    ensure_operations_tables,
)


class FakeSparkSession:
    """Capture SQL statements executed by operations helpers."""

    def __init__(self):
        self.executed_queries: list[str] = []
        self.table_requests: list[str] = []
        self.created_records: list[dict] | None = None
        self.created_schema = None
        self.tables: dict[str, object] = {}
        self.catalog = type(
            "FakeCatalog",
            (),
            {"dropTempView": lambda self, _name: None},
        )()

    def sql(self, query: str):
        self.executed_queries.append(query)
        return None

    def table(self, table_name: str):
        self.table_requests.append(table_name)
        if table_name in self.tables:
            return self.tables[table_name]
        return type(
            "FakeTable",
            (),
            {
                "schema": "schema-from-table",
                "toLocalIterator": lambda self: iter(()),
            },
        )()

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

    def createOrReplaceTempView(self, _name: str):
        return None


class FakeField:
    def __init__(self, name: str, nullable: bool):
        self.name = name
        self.nullable = nullable


class FakeSchema:
    def __init__(self, fields):
        self.fields = fields


class FakeExistingRow:
    def __init__(self, mapping):
        self._mapping = mapping

    def asDict(self, recursive: bool = True):
        return dict(self._mapping)


class FakePipelineTable:
    def __init__(self, schema, rows):
        self.schema = schema
        self._rows = rows

    def toLocalIterator(self):
        return iter(self._rows)


def _runtime_context():
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    return build_runtime_context(
        config,
        environment,
        upstream_silver_run_id="upstream-run-123",
        match_rows=[{"match_date": "2026-06-25", "completed_flag": True}],
    )


def _context():
    return create_pipeline_context(_runtime_context(), pipeline_run_id="run-123")


def test_ensure_operations_tables_emits_all_expected_ddls() -> None:
    context = _context()
    spark = FakeSparkSession()

    ensure_operations_tables(spark, context)

    joined = "\n".join(spark.executed_queries)
    assert f".{PIPELINE_RUNS_TABLE}" in joined
    assert f".{TABLE_RUNS_TABLE}" in joined
    assert f".{QUALITY_RESULTS_TABLE}" in joined
    assert f".{RECONCILIATION_RESULTS_TABLE}" in joined
    assert f".{MODEL_RUNS_TABLE}" in joined
    assert f".{MODEL_METRICS_TABLE}" in joined
    assert f".{RECOMMENDATION_RUNS_TABLE}" in joined


def test_build_pipeline_run_records_include_gold_specific_fields() -> None:
    context = _context()
    started = datetime(2026, 7, 22, 12, 0, tzinfo=timezone.utc)
    completed = datetime(2026, 7, 22, 12, 5, tzinfo=timezone.utc)

    start_record = build_pipeline_run_start_record(context, started_ts=started)
    end_record = build_pipeline_run_end_record(
        context,
        started_ts=started,
        completed_ts=completed,
        status="SUCCEEDED",
    )

    assert start_record["status"] == "RUNNING"
    assert start_record["analysis_as_of_date"] == date(2026, 6, 25)
    assert start_record["upstream_pipeline_run_id"] == "upstream-run-123"
    assert end_record["status"] == "SUCCEEDED"
    assert end_record["duration_seconds"] == 300.0


def test_build_table_run_end_record_captures_gold_counts() -> None:
    context = _context()
    started = datetime(2026, 7, 22, 13, 0, tzinfo=timezone.utc)
    completed = datetime(2026, 7, 22, 13, 2, 30, tzinfo=timezone.utc)

    record = build_table_run_end_record(
        context,
        target_gold_table="competition_match_sides",
        build_stage="foundation",
        build_order=10,
        started_ts=started,
        completed_ts=completed,
        status="SUCCEEDED",
        input_row_count=100,
        output_row_count=98,
        excluded_row_count=2,
    )

    assert record["build_stage"] == "foundation"
    assert record["build_order"] == 10
    assert record["output_row_count"] == 98
    assert record["duration_seconds"] == 150.0


def test_build_quality_and_reconciliation_records() -> None:
    context = _context()

    quality_record = build_quality_result_record(
        context,
        target_table="competition_match_sides",
        rule_id="GOLD_001",
        rule_category="cardinality",
        severity="CRITICAL",
        status="FAILED",
        evaluated_row_count=100,
        failed_row_count=3,
        failure_pct=3.0,
        sample_keys=["match-1", "match-2"],
    )
    reconciliation_record = build_reconciliation_record(
        context,
        reconciliation_name="competition_match_sides_row_balance",
        source_count=100,
        accepted_count=98,
        excluded_count=2,
    )

    assert quality_record["rule_id"] == "GOLD_001"
    assert quality_record["sample_keys"] == ["match-1", "match-2"]
    assert reconciliation_record["difference"] == 0
    assert reconciliation_record["status"] == "PASSED"


def test_build_model_and_recommendation_records() -> None:
    context = _context()

    model_start = build_model_run_start_record(
        context,
        model_run_id="model-run-1",
        model_name="match_baseline",
        model_version="1.0.0",
        algorithm="rating_baseline",
        feature_definition_version="features-v1",
    )
    model_end = build_model_run_end_record(model_start, status="SUCCEEDED")
    model_metric = build_model_metric_record(
        model_run_id="model-run-1",
        split_name="validation",
        metric_name="brier_score",
        metric_value=0.183,
        evaluated_row_count=120,
    )
    recommendation = build_recommendation_run_record(
        context,
        recommendation_run_id="recommendation-run-1",
        methodology_version="scorecards-v1",
        eligible_team_count=15,
        primary_recommendation_count=2,
        alternate_recommendation_count=2,
        watchlist_count=3,
    )

    assert model_start["status"] == "RUNNING"
    assert model_end["status"] == "SUCCEEDED"
    assert model_metric["metric_name"] == "brier_score"
    assert recommendation["scoring_scenario"] == "BALANCED"
    assert recommendation["analysis_as_of_date"] == date(2026, 6, 25)


def test_calculate_record_hash_is_deterministic() -> None:
    record = {"target_gold_table": "competition_match_sides", "output_row_count": 10}
    assert calculate_record_hash(record) == calculate_record_hash(record)


def test_append_records_uses_existing_table_schema() -> None:
    spark = FakeSparkSession()
    records = [
        {
            "pipeline_run_id": "run-123",
            "release_name": "napa_5k",
            "target_table": "competition_match_sides",
        }
    ]

    append_records(spark, "workspace.instructor_ops.gold_quality_results", records)

    assert spark.table_requests == ["workspace.instructor_ops.gold_quality_results"]
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
            "target_table": "competition_match_sides",
        }
    ]

    with pytest.raises(ValueError, match="required field 'release_name' is null or missing"):
        append_records(spark, "workspace.instructor_ops.gold_quality_results", records)


def test_complete_pipeline_run_merges_completion_into_open_record() -> None:
    context = _context()
    pipeline_runs_fqn = f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}"
    schema = FakeSchema(
        [
            FakeField("pipeline_run_id", False),
            FakeField("pipeline_name", False),
            FakeField("pipeline_version", False),
            FakeField("release_name", False),
            FakeField("processing_mode", False),
            FakeField("configuration_hash", False),
            FakeField("workflow_run_id", True),
            FakeField("status", False),
            FakeField("started_ts", False),
            FakeField("completed_ts", True),
            FakeField("duration_seconds", True),
            FakeField("triggered_by", True),
            FakeField("error_class", True),
            FakeField("error_message", True),
        ]
    )
    open_row = FakeExistingRow(
        {
            "pipeline_run_id": "run-123",
            "pipeline_name": "silver_to_gold",
            "pipeline_version": "1.0.0",
            "release_name": "napa_5k",
            "processing_mode": "full_refresh",
            "configuration_hash": "hash",
            "workflow_run_id": None,
            "status": "RUNNING",
            "started_ts": datetime(2026, 7, 22, 12, 0, tzinfo=timezone.utc),
            "completed_ts": None,
            "duration_seconds": None,
            "triggered_by": None,
            "error_class": None,
            "error_message": None,
        }
    )
    spark = FakeSparkSession()
    spark.tables[pipeline_runs_fqn] = FakePipelineTable(schema, [open_row])

    complete_pipeline_run(spark, context, status="SUCCEEDED")

    assert spark.created_schema == schema
    assert spark.created_records[0]["status"] == "SUCCEEDED"
    assert any("MERGE INTO" in query for query in spark.executed_queries)
