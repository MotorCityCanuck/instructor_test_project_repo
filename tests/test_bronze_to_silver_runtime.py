"""Tests for Bronze-to-Silver execution and publication helpers."""

from datetime import datetime, timezone

import napa_pipeline.bronze_to_silver.cross_table as cross_table_module
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.cross_table import run_cross_table_validations_sql
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.execute import publish_convenience_views_task
from napa_pipeline.bronze_to_silver.finalize import summarize_pipeline_run
from napa_pipeline.bronze_to_silver.operations import (
    PIPELINE_RUNS_TABLE,
    QUALITY_RESULTS_TABLE,
    RECONCILIATION_RESULTS_TABLE,
    RUN_MESSAGES_TABLE,
    TABLE_RUNS_TABLE,
    create_pipeline_context,
)
from napa_pipeline.bronze_to_silver.publish import (
    append_quality_results_for_rejects,
    publish_records_to_view,
    publish_sql_view,
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

    def count(self):
        return len(self._rows)


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
        if query.startswith("CREATE OR REPLACE VIEW "):
            view_name = query.split(" AS ", 1)[0].replace("CREATE OR REPLACE VIEW ", "").strip()
            self.tables.setdefault(view_name, FakeTable(rows=[]))
        return None


def _context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    return create_pipeline_context(config, environment, pipeline_run_id="run-123")


def test_append_quality_results_for_rejects_groups_by_rule() -> None:
    context = _context()
    table_fqn = f"{context.operations_schema_fqn}.{QUALITY_RESULTS_TABLE}"
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
    assert grouped["RULE_1"]["sample_business_keys"] == ["a", "b"]
    assert grouped["RULE_2"]["sample_business_keys"] == ["c"]


def test_append_quality_results_for_rejects_ignores_null_sample_business_keys() -> None:
    context = _context()
    table_fqn = f"{context.operations_schema_fqn}.{QUALITY_RESULTS_TABLE}"
    spark = FakeSparkSession(
        tables={table_fqn: FakeTable(schema="schema-from-table")},
        existing_tables={table_fqn},
    )
    rejected_rows = [
        {"rule_id": "RULE_1", "rule_severity": "ERROR", "source_business_key": None},
        {"rule_id": "RULE_1", "rule_severity": "ERROR", "source_business_key": "player-1"},
    ]

    quality_records = append_quality_results_for_rejects(
        spark,
        context,
        target_table="players",
        evaluated_row_count=5,
        rejected_rows=rejected_rows,
    )

    assert len(quality_records) == 1
    assert quality_records[0]["sample_business_keys"] == ["player-1"]


def test_publish_records_to_view_emits_create_or_replace_view_sql() -> None:
    spark = FakeSparkSession()

    row_count = publish_records_to_view(
        spark,
        "workspace.instructor_5k_silver.vw_players_current",
        [{"player_id": "player-1"}],
    )

    assert row_count == 1
    assert any("CREATE OR REPLACE VIEW" in query for query in spark.executed_sql)


def test_publish_sql_view_emits_create_or_replace_view_sql() -> None:
    spark = FakeSparkSession()

    row_count = publish_sql_view(
        spark,
        "workspace.instructor_5k_silver.vw_team_rosters",
        "SELECT 1 AS roster_count",
    )

    assert row_count == 0
    assert any("CREATE OR REPLACE VIEW workspace.instructor_5k_silver.vw_team_rosters" in query for query in spark.executed_sql)


def test_summarize_pipeline_run_detects_critical_quality_failures() -> None:
    context = _context()
    pipeline_runs_fqn = f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}"
    table_runs_fqn = f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}"
    reconciliation_fqn = f"{context.operations_schema_fqn}.{RECONCILIATION_RESULTS_TABLE}"
    quality_fqn = f"{context.operations_schema_fqn}.{QUALITY_RESULTS_TABLE}"
    run_messages_fqn = f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}"

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
                            "severity": "CRITICAL",
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


def test_summarize_pipeline_run_allows_error_quality_rejects() -> None:
    context = _context()
    pipeline_runs_fqn = f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}"
    table_runs_fqn = f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}"
    reconciliation_fqn = f"{context.operations_schema_fqn}.{RECONCILIATION_RESULTS_TABLE}"
    quality_fqn = f"{context.operations_schema_fqn}.{QUALITY_RESULTS_TABLE}"
    run_messages_fqn = f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}"

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
                            "failed_row_count": 112,
                        }
                    )
                ]
            ),
            run_messages_fqn: FakeTable(rows=[]),
        }
    )

    summary = summarize_pipeline_run(spark, context, expected_table_count=1)

    assert summary.final_status == "SUCCEEDED"
    assert summary.critical_quality_failure_count == 0


def test_summarize_pipeline_run_reports_failed_and_incomplete_tables() -> None:
    context = _context()
    pipeline_runs_fqn = f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}"
    table_runs_fqn = f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}"
    reconciliation_fqn = f"{context.operations_schema_fqn}.{RECONCILIATION_RESULTS_TABLE}"
    quality_fqn = f"{context.operations_schema_fqn}.{QUALITY_RESULTS_TABLE}"
    run_messages_fqn = f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}"

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
                            "target_table": "monthly_batches",
                            "build_stage": "reference",
                            "build_order": 10,
                            "status": "SUCCEEDED",
                            "completed_ts": datetime(2026, 7, 16, 12, 5, tzinfo=timezone.utc),
                        }
                    ),
                    FakeRow(
                        {
                            "pipeline_run_id": "run-123",
                            "target_table": "regions",
                            "build_stage": "reference",
                            "build_order": 20,
                            "status": "SUCCEEDED",
                            "completed_ts": datetime(2026, 7, 16, 12, 6, tzinfo=timezone.utc),
                        }
                    ),
                    FakeRow(
                        {
                            "pipeline_run_id": "run-123",
                            "target_table": "players",
                            "build_stage": "athlete",
                            "build_order": 30,
                            "status": "FAILED",
                            "completed_ts": datetime(2026, 7, 16, 12, 7, tzinfo=timezone.utc),
                            "error_message": "source column display_name cannot be resolved",
                        }
                    ),
                ]
            ),
            reconciliation_fqn: FakeTable(
                rows=[
                    FakeRow({"pipeline_run_id": "run-123", "status": "PASSED"}),
                    FakeRow({"pipeline_run_id": "run-123", "status": "PASSED"}),
                ]
            ),
            quality_fqn: FakeTable(rows=[]),
            run_messages_fqn: FakeTable(rows=[]),
        }
    )

    summary = summarize_pipeline_run(
        spark,
        context,
        expected_table_count=5,
        expected_table_names=[
            "monthly_batches",
            "regions",
            "players",
            "player_registrations",
            "player_assessment_history",
        ],
    )

    assert summary.final_status == "FAILED"
    assert "athlete.players: source column display_name cannot be resolved" in summary.summary_text
    assert "Tables not completed: player_registrations, player_assessment_history." in summary.summary_text


def test_run_cross_table_validations_sql_uses_sql_aggregation(monkeypatch) -> None:
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment, pipeline_run_id="run-123")
    spark = FakeSparkSession()

    def fake_scalar_count(_spark, query: str) -> int:
        if "COUNT(*) AS value FROM (" in query and "HAVING COUNT(tm.team_membership_id) <> 2" in query:
            return 1
        if "COUNT(*) AS value FROM (" in query and "HAVING COUNT(mt.match_team_id) <> 2" in query:
            return 0
        if "COUNT(*) AS value FROM (" in query and "HAVING COUNT(mg.match_game_id) < 1" in query:
            return 0
        if "COUNT(*) AS value FROM (" in query and "HAVING COUNT(mtp.match_team_player_id) <> 2" in query:
            return 1
        if "COUNT(*) AS value FROM (" in query and "team_membership_id" in query and "LEFT ANTI JOIN workspace.instructor_5k_silver.players" in query:
            return 0
        if "COUNT(*) AS value FROM (" in query and "match_team_player_id" in query and "LEFT ANTI JOIN workspace.instructor_5k_silver.players" in query:
            return 0
        if "COUNT(*) AS value FROM (" in query and "team_winners AS" in query:
            return 0
        if "SELECT COUNT(*) AS value FROM workspace.instructor_5k_silver.teams WHERE active_flag = true" in query:
            return 2
        if "SELECT COUNT(*) AS value FROM workspace.instructor_5k_silver.matches WHERE completed_flag = true" in query:
            return 1
        if "SELECT COUNT(*) AS value FROM workspace.instructor_5k_silver.match_teams" in query:
            return 2
        if "SELECT COUNT(*) AS value FROM workspace.instructor_5k_silver.team_memberships" in query:
            return 3
        if "SELECT COUNT(*) AS value FROM workspace.instructor_5k_silver.match_team_players" in query:
            return 3
        if "SELECT COUNT(*) AS value FROM workspace.instructor_5k_silver.matches" in query:
            return 1
        return 0

    def fake_sample_keys(_spark, query: str) -> list[str]:
        if "HAVING COUNT(tm.team_membership_id) <> 2" in query:
            return ["team-2"]
        if "HAVING COUNT(mtp.match_team_player_id) <> 2" in query:
            return ["mt-2"]
        return []

    monkeypatch.setattr(cross_table_module, "_scalar_count", fake_scalar_count)
    monkeypatch.setattr(cross_table_module, "_sample_business_keys", fake_sample_keys)

    result = run_cross_table_validations_sql(
        spark,
        context,
        environment,
        expected_match_team_count=2,
        expected_match_team_player_count=2,
    )

    quality_by_rule = {row["rule_id"]: row for row in result.quality_results}
    assert quality_by_rule["CROSS_TEAM_001"]["failed_row_count"] == 1
    assert quality_by_rule["CROSS_MATCH_TEAM_001"]["failed_row_count"] == 1
    assert quality_by_rule["CROSS_WINNER_001"]["failed_row_count"] == 0
    assert result.warning_count == 2
    assert result.failure_count == 0


def test_publish_convenience_views_task_uses_sql_views() -> None:
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment, pipeline_run_id="run-123")
    spark = FakeSparkSession()

    published_counts = publish_convenience_views_task(spark, config, environment, context)

    assert set(published_counts) == {
        "vw_players_current",
        "vw_current_team_memberships",
        "vw_team_rosters",
        "vw_match_results",
        "vw_player_match_history",
    }
    assert sum(1 for query in spark.executed_sql if query.startswith("CREATE OR REPLACE VIEW ")) == 5
