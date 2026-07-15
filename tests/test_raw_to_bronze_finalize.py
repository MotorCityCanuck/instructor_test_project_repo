"""Tests for Raw-to-Bronze pipeline finalization helpers."""

from datetime import datetime, timezone

from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.finalize import (
    finalize_pipeline_run,
    summarize_pipeline_run,
)
from napa_pipeline.raw_to_bronze.operations import create_pipeline_context


class FakeRow:
    """Minimal row stub with asDict support."""

    def __init__(self, mapping):
        self.mapping = mapping

    def asDict(self, recursive: bool = True):
        return dict(self.mapping)


class FakeTableData:
    """Minimal table stub returning collected rows."""

    def __init__(self, rows, schema="schema-from-table"):
        self._rows = rows
        self.schema = schema

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
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    return create_pipeline_context(config, environment, pipeline_run_id="run-123")


def test_summarize_pipeline_run_reports_success_when_counts_match() -> None:
    context = _context()
    tables = {
        f"{context.operations_schema_fqn}.table_runs": FakeTableData(
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
        f"{context.operations_schema_fqn}.reconciliation_results": FakeTableData(
            [
                {"pipeline_run_id": "run-123", "status": "MATCHED"},
                {"pipeline_run_id": "run-123", "status": "MATCHED"},
            ]
        ),
    }

    summary = summarize_pipeline_run(FakeSparkSession(tables), context, 2)

    assert summary.final_status == "SUCCEEDED"
    assert summary.failed_table_run_count == 0
    assert summary.mismatched_reconciliation_count == 0


def test_summarize_pipeline_run_reports_failure_when_counts_do_not_match() -> None:
    context = _context()
    tables = {
        f"{context.operations_schema_fqn}.table_runs": FakeTableData(
            [
                {
                    "pipeline_run_id": "run-123",
                    "completed_ts": datetime.now(timezone.utc),
                    "status": "SUCCEEDED",
                }
            ]
        ),
        f"{context.operations_schema_fqn}.reconciliation_results": FakeTableData(
            [
                {"pipeline_run_id": "run-123", "status": "MISMATCH"},
            ]
        ),
    }

    summary = summarize_pipeline_run(FakeSparkSession(tables), context, 2)

    assert summary.final_status == "FAILED"
    assert "completed_table_runs=1/2" in summary.summary_text


def test_finalize_pipeline_run_merges_completion_into_open_record() -> None:
    context = _context()
    tables = {
        f"{context.operations_schema_fqn}.pipeline_runs": FakeTableData(
            [
                {
                    "pipeline_run_id": "run-123",
                    "pipeline_name": "raw_to_bronze",
                    "pipeline_version": "1.0.0",
                    "release_name": "napa_5k",
                    "processing_mode": "full_refresh",
                    "configuration_hash": "hash",
                    "workflow_run_id": None,
                    "status": "RUNNING",
                    "started_ts": datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc),
                    "completed_ts": None,
                    "duration_seconds": None,
                    "triggered_by": None,
                    "error_class": None,
                    "error_message": None,
                }
            ]
        )
    }
    spark = FakeSparkSession(tables)
    summary = summarize_pipeline_run(
        FakeSparkSession(
            {
                f"{context.operations_schema_fqn}.table_runs": FakeTableData(
                    [
                        {
                            "pipeline_run_id": "run-123",
                            "completed_ts": datetime.now(timezone.utc),
                            "status": "SUCCEEDED",
                        }
                    ]
                ),
                f"{context.operations_schema_fqn}.reconciliation_results": FakeTableData(
                    [
                        {"pipeline_run_id": "run-123", "status": "MATCHED"},
                    ]
                ),
            }
        ),
        context,
        1,
    )

    finalize_pipeline_run(spark, context, summary)

    assert spark.created_schema == "schema-from-table"
    assert spark.created_records[0]["status"] == "SUCCEEDED"
    assert any("MERGE INTO" in query for query in spark.sql_queries)
