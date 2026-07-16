"""Tests for Bronze-to-Silver execution and publication helpers."""

from datetime import datetime, timezone

from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.finalize import summarize_pipeline_run
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context
from napa_pipeline.bronze_to_silver.publish import (
    append_quality_results_for_rejects,
    publish_records_to_view,
)


class FakeRow:
    def __init__(self, mapping):
        self._mapping = mapping

    def asDict(self, recursive: bool = True):
        return dict(self._mapping)


class FakeSchemaField:
    def __init__(self, name: str, data_type: str, nullable: bool = True):
        self.name = name
        self.nullable = nullable
        self.dataType = type("DataType", (), {"simpleString": lambda self_: data_type})()


class FakeSchema:
    def __init__(self, fields):
        self.fields = fields


class FakeCatalog:
    def __init__(self, existing_tables=None):
        self.existing_tables = set(existing_tables or [])
        self.dropped_views: list[str] = []

    def tableExists(self, table_name: str) -> bool:
        return table_name in self.existing_tables

    def dropTempView(self, view_name: str) -> None:
        self.dropped_views.append(view_name)


class FakeWriteBuilder:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    def format(self, value: str):
        self.calls.append(("format", value))
        return self

    def mode(self, value: str):
        self.calls.append(("mode", value))
        return self

    def option(self, key: str, value: str):
        self.calls.append((f"option:{key}", value))
        return self

    def saveAsTable(self, value: str):
        self.calls.append(("saveAsTable", value))
        return None


class FakeDataFrame:
    def __init__(self, rows):
        self.rows = rows
        self.write = FakeWriteBuilder()
        self.temp_views: list[str] = []

    def createOrReplaceTempView(self, view_name: str) -> None:
        self.temp_views.append(view_name)


class FakeTable:
    def __init__(self, rows=None, schema=None):
        self._rows = rows or []
        self.schema = schema or FakeSchema([FakeSchemaField("value", "string")])

    def toLocalIterator(self):
        return iter(self._rows)

    def collect(self):
        return list(self._rows)


class FakeSparkSession:
    def __init__(self, tables=None, existing_tables=None):
        self.tables = tables or {}
        self.catalog = FakeCatalog(existing_tables or self.tables.keys())
        self.created_dataframes: list[FakeDataFrame] = []
        self.executed_sql: list[str] = []

    def table(self, table_name: str):
        return self.tables[table_name]

    def createDataFrame(self, rows, schema=None):
        dataframe = FakeDataFrame(list(rows))
        self.created_dataframes.append(dataframe)
        return dataframe

    def sql(self, query: str):
        self.executed_sql.append(query)
        return None


def _context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    return create_pipeline_context(config, environment, pipeline_run_id="run-123")


def test_append_quality_results_for_rejects_groups_by_rule() -> None:
    context = _context()
    table_fqn = f"{context.operations_schema_fqn}.quality_results"
    spark = FakeSparkSession(
        tables={table_fqn: FakeTable(schema="schema-from-table")},
        existing_tables={table_fqn},
    )
    rejected_rows = [
        {"rule_id": "RULE_1", "rule_severity": "ERROR", "source_business_key": "a"},
        {"rule_id": "RULE_1", "rule_severity": "ERROR", "source_business_key": "b"},
        {"rule_id": "RULE_2", "rule_severity": "WARNING", "source_business_key": "c"},
    ]

    quality_records = append_quality_results_for_rejects(
        spark,
        context,
        target_table="players",
        evaluated_row_count=10,
        rejected_rows=rejected_rows,
    )

    assert len(quality_records) == 2
    grouped = {record["rule_id"]: record for record in quality_records}
    assert grouped["RULE_1"]["failed_row_count"] == 2
    assert grouped["RULE_2"]["failed_row_count"] == 1


def test_publish_records_to_view_emits_create_or_replace_view_sql() -> None:
    spark = FakeSparkSession()

    row_count = publish_records_to_view(
        spark,
        "workspace.instructor_5k_silver.vw_players_current",
        [{"player_id": "player-1"}],
    )

    assert row_count == 1
    assert any("CREATE OR REPLACE VIEW" in query for query in spark.executed_sql)


def test_summarize_pipeline_run_detects_critical_quality_failures() -> None:
    context = _context()
    pipeline_runs_fqn = f"{context.operations_schema_fqn}.pipeline_runs"
    table_runs_fqn = f"{context.operations_schema_fqn}.table_runs"
    reconciliation_fqn = f"{context.operations_schema_fqn}.reconciliation_results"
    quality_fqn = f"{context.operations_schema_fqn}.quality_results"
    run_messages_fqn = f"{context.operations_schema_fqn}.run_messages"

    spark = FakeSparkSession(
        tables={
            pipeline_runs_fqn: FakeTable(
                rows=[
                    FakeRow(
                        {
                            "pipeline_run_id": "run-123",
                            "started_ts": datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc),
                            "completed_ts": None,
                        }
                    )
                ]
            ),
            table_runs_fqn: FakeTable(
                rows=[
                    FakeRow(
                        {
                            "pipeline_run_id": "run-123",
                            "status": "SUCCEEDED",
                            "completed_ts": datetime(2026, 7, 16, 12, 5, tzinfo=timezone.utc),
                        }
                    )
                ]
            ),
            reconciliation_fqn: FakeTable(
                rows=[
                    FakeRow(
                        {
                            "pipeline_run_id": "run-123",
                            "status": "PASSED",
                        }
                    )
                ]
            ),
            quality_fqn: FakeTable(
                rows=[
                    FakeRow(
                        {
                            "pipeline_run_id": "run-123",
                            "severity": "ERROR",
                            "failed_row_count": 1,
                        }
                    )
                ]
            ),
            run_messages_fqn: FakeTable(rows=[]),
        }
    )

    summary = summarize_pipeline_run(spark, context, expected_table_count=1)

    assert summary.final_status == "FAILED"
    assert summary.critical_quality_failure_count == 1
