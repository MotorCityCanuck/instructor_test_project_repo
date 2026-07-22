"""Tests for Silver-to-Gold pipeline finalization helpers."""

from datetime import datetime, timezone

import pytest

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import build_runtime_context, resolve_release_environment
from napa_pipeline.silver_to_gold.finalize import (
    PipelineFinalizationError,
    finalize_pipeline_run,
    summarize_pipeline_run,
)
from napa_pipeline.silver_to_gold.operations import create_pipeline_context


class FakeRow:
    """Minimal row stub with asDict support."""

    def __init__(self, mapping):
        self.mapping = mapping

    def asDict(self, recursive: bool = True):
        return dict(self.mapping)


class FakeField:
    """Minimal schema field stub."""

    def __init__(self, name: str):
        self.name = name


class FakeSchema:
    """Minimal schema stub."""

    def __init__(self, field_names):
        self.fields = [FakeField(name) for name in field_names]


class FakeTableData:
    """Minimal table stub returning collected rows."""

    def __init__(self, rows, schema=None):
        self._rows = rows
        self.schema = schema or FakeSchema(
            [
                "pipeline_run_id",
                "pipeline_name",
                "pipeline_version",
                "release_name",
                "processing_mode",
                "configuration_hash",
                "workflow_run_id",
                "status",
                "started_ts",
                "completed_ts",
                "duration_seconds",
                "triggered_by",
                "error_class",
                "error_message",
            ]
        )

    def collect(self):
        return [FakeRow(row) for row in self._rows]


class FakeUpdateDataFrame:
    """Minimal update DataFrame stub."""

    def __init__(self):
        self.temp_views: list[str] = []

    def createOrReplaceTempView(self, name: str):
        self.temp_views.append(name)


class FakeCatalog:
    """Minimal Spark catalog stub."""

    def __init__(self):
        self.dropped_views: list[str] = []

    def dropTempView(self, name: str):
        self.dropped_views.append(name)


class FakeSparkSession:
    """Minimal Spark session for finalization tests."""

    def __init__(self, tables):
        self.tables = tables
        self.sql_queries: list[str] = []
        self.created_records = None
        self.created_schema = None
        self.catalog = FakeCatalog()

    def table(self, table_name: str):
        return self.tables[table_name]

    def createDataFrame(self, records, schema=None):
        self.created_records = list(records)
        self.created_schema = schema
        return FakeUpdateDataFrame()

    def sql(self, query: str):
        self.sql_queries.append(query)
        return None


def _context():
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    runtime_context = build_runtime_context(
        config,
        environment,
        upstream_silver_run_id="upstream-run-123",
        match_rows=[{"match_date": "2026-06-25", "completed_flag": True}],
    )
    return create_pipeline_context(runtime_context, pipeline_run_id="run-123")


def test_summarize_pipeline_run_reports_success_when_counts_match() -> None:
    context = _context()
    tables = {
        f"{context.operations_schema_fqn}.pipeline_runs": FakeTableData(
            [{"pipeline_run_id": "run-123", "completed_ts": None}]
        ),
        f"{context.operations_schema_fqn}.gold_table_runs": FakeTableData(
            [
                {
                    "pipeline_run_id": "run-123",
                    "completed_ts": datetime.now(timezone.utc),
                    "status": "SUCCEEDED",
                },
                {
                    "pipeline_run_id": "run-123",
                    "completed_ts": datetime.now(timezone.utc),
                    "status": "SUCCEEDED",
                },
            ]
        ),
        f"{context.operations_schema_fqn}.gold_reconciliation_results": FakeTableData(
            [
                {"pipeline_run_id": "run-123", "status": "PASSED"},
                {"pipeline_run_id": "run-123", "status": "PASSED"},
            ]
        ),
        f"{context.operations_schema_fqn}.gold_quality_results": FakeTableData([]),
    }

    summary = summarize_pipeline_run(FakeSparkSession(tables), context, 2)

    assert summary.final_status == "SUCCEEDED"
    assert summary.failed_table_run_count == 0
    assert summary.failed_reconciliation_count == 0


def test_summarize_pipeline_run_reports_failure_when_counts_do_not_match() -> None:
    context = _context()
    tables = {
        f"{context.operations_schema_fqn}.pipeline_runs": FakeTableData(
            [{"pipeline_run_id": "run-123", "completed_ts": None}]
        ),
        f"{context.operations_schema_fqn}.gold_table_runs": FakeTableData(
            [
                {
                    "pipeline_run_id": "run-123",
                    "completed_ts": datetime.now(timezone.utc),
                    "status": "SUCCEEDED",
                }
            ]
        ),
        f"{context.operations_schema_fqn}.gold_reconciliation_results": FakeTableData(
            [
                {"pipeline_run_id": "run-123", "status": "FAILED"},
            ]
        ),
        f"{context.operations_schema_fqn}.gold_quality_results": FakeTableData([]),
    }

    summary = summarize_pipeline_run(FakeSparkSession(tables), context, 2)

    assert summary.final_status == "FAILED"
    assert "completed_table_runs=1/2" in summary.summary_text


def test_summarize_pipeline_run_raises_clear_error_when_run_id_has_no_records() -> None:
    context = _context()
    tables = {
        f"{context.operations_schema_fqn}.pipeline_runs": FakeTableData([]),
        f"{context.operations_schema_fqn}.gold_table_runs": FakeTableData([]),
        f"{context.operations_schema_fqn}.gold_reconciliation_results": FakeTableData([]),
        f"{context.operations_schema_fqn}.gold_quality_results": FakeTableData([]),
    }

    with pytest.raises(
        PipelineFinalizationError,
        match="No pipeline_runs, gold_table_runs, or gold_reconciliation_results were found",
    ):
        summarize_pipeline_run(FakeSparkSession(tables), context, 22)


def test_finalize_pipeline_run_merges_completion_into_open_record() -> None:
    context = _context()
    pipeline_schema = FakeSchema(
        [
            "pipeline_run_id",
            "pipeline_name",
            "pipeline_version",
            "release_name",
            "processing_mode",
            "configuration_hash",
            "workflow_run_id",
            "status",
            "started_ts",
            "completed_ts",
            "duration_seconds",
            "triggered_by",
            "error_class",
            "error_message",
        ]
    )
    tables = {
        f"{context.operations_schema_fqn}.pipeline_runs": FakeTableData(
            [
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
            ],
            schema=pipeline_schema,
        )
    }
    spark = FakeSparkSession(tables)
    summary = summarize_pipeline_run(
        FakeSparkSession(
            {
                f"{context.operations_schema_fqn}.pipeline_runs": FakeTableData(
                    [{"pipeline_run_id": "run-123", "completed_ts": None}],
                    schema=pipeline_schema,
                ),
                f"{context.operations_schema_fqn}.gold_table_runs": FakeTableData(
                    [
                        {
                            "pipeline_run_id": "run-123",
                            "completed_ts": datetime.now(timezone.utc),
                            "status": "SUCCEEDED",
                        }
                    ]
                ),
                f"{context.operations_schema_fqn}.gold_reconciliation_results": FakeTableData(
                    [
                        {"pipeline_run_id": "run-123", "status": "PASSED"},
                    ]
                ),
                f"{context.operations_schema_fqn}.gold_quality_results": FakeTableData([]),
            }
        ),
        context,
        1,
    )

    finalize_pipeline_run(spark, context, summary)

    assert spark.created_schema == pipeline_schema
    assert spark.created_records[0]["status"] == "SUCCEEDED"
    assert any("MERGE INTO" in query for query in spark.sql_queries)
