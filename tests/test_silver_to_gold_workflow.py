"""Tests for Silver-to-Gold workflow-support helpers."""

from collections.abc import Iterable

import pytest

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import build_runtime_context, resolve_release_environment
from napa_pipeline.silver_to_gold.operations import PIPELINE_RUNS_TABLE, create_pipeline_context
from napa_pipeline.silver_to_gold.workflow import (
    PHASE3_TARGET_TABLES,
    REQUIRED_SILVER_SOURCE_TABLES,
    SilverSourceValidationError,
    UpstreamSilverRunNotFoundError,
    collect_match_rows_for_analysis_date,
    initialize_pipeline_run,
    require_required_silver_source_tables,
    resolve_latest_successful_upstream_run_id,
    validate_required_silver_source_tables,
)


class FakeRow:
    def __init__(self, mapping):
        self._mapping = mapping

    def asDict(self, recursive: bool = True):
        return dict(self._mapping)


class FakeCollectResult:
    def __init__(self, rows: list[dict[str, object]]):
        self._rows = rows

    def collect(self):
        return [FakeRow(row) for row in self._rows]


class FakeTable:
    def __init__(self, rows: Iterable[dict[str, object]] | None = None, schema="schema-from-table"):
        self._rows = [FakeRow(row) for row in (rows or [])]
        self.schema = schema

    def toLocalIterator(self):
        return iter(self._rows)

    def collect(self):
        return list(self._rows)


class FakeCatalog:
    def __init__(self, existing_tables=None):
        self._existing_tables = set(existing_tables or [])

    def tableExists(self, table_name: str) -> bool:
        return table_name in self._existing_tables


class FakeWriteBuilder:
    def format(self, _value: str):
        return self

    def mode(self, _value: str):
        return self

    def saveAsTable(self, _value: str):
        return None


class FakeDataFrame:
    def __init__(self):
        self.write = FakeWriteBuilder()


class FakeSparkSession:
    def __init__(self, *, existing_tables=None, sql_rows_by_query=None, tables=None):
        self.catalog = FakeCatalog(existing_tables)
        self.sql_rows_by_query = sql_rows_by_query or {}
        self.tables = tables or {}
        self.executed_sql: list[str] = []
        self.created_records = None
        self.created_schema = None

    def sql(self, query: str):
        self.executed_sql.append(query)
        if query.startswith("CREATE TABLE IF NOT EXISTS "):
            return None
        rows = self.sql_rows_by_query.get(query)
        if rows is None:
            raise RuntimeError(f"unexpected query: {query}")
        return FakeCollectResult(rows)

    def table(self, table_name: str):
        return self.tables[table_name]

    def createDataFrame(self, records, schema=None):
        self.created_records = list(records)
        self.created_schema = schema
        return FakeDataFrame()


def _config_environment():
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    return config, environment


def _pipeline_context():
    config, environment = _config_environment()
    runtime_context = build_runtime_context(
        config,
        environment,
        upstream_silver_run_id="upstream-run-123",
        match_rows=[{"match_date": "2026-06-25", "completed_flag": True}],
    )
    return create_pipeline_context(runtime_context, pipeline_run_id="gold-run-123")


def test_phase3_target_table_inventory_is_expected() -> None:
    assert PHASE3_TARGET_TABLES == (
        "competition_match_sides",
        "competition_player_matches",
    )


def test_validate_required_silver_source_tables_returns_existing_and_missing() -> None:
    _config, environment = _config_environment()
    existing_fqns = {
        f"{environment.catalog}.{environment.silver_schema}.{table_name}"
        for table_name in REQUIRED_SILVER_SOURCE_TABLES[:-1]
    }
    spark = FakeSparkSession(existing_tables=existing_fqns)

    existing_tables, missing_tables = validate_required_silver_source_tables(spark, environment)

    assert len(existing_tables) == len(REQUIRED_SILVER_SOURCE_TABLES) - 1
    assert missing_tables == [
        f"{environment.catalog}.{environment.silver_schema}.match_games"
    ]


def test_require_required_silver_source_tables_raises_clear_error() -> None:
    _config, environment = _config_environment()
    spark = FakeSparkSession(existing_tables=set())

    with pytest.raises(SilverSourceValidationError, match="Missing required Silver source tables"):
        require_required_silver_source_tables(spark, environment)


def test_resolve_latest_successful_upstream_run_id_returns_latest_run() -> None:
    config, environment = _config_environment()
    query = f"""
SELECT pipeline_run_id
FROM {environment.catalog}.{environment.operations_schema}.b2s_pipeline_runs
WHERE pipeline_name = 'bronze_to_silver'
  AND release_name = '{config.release_name}'
  AND status = 'SUCCEEDED'
ORDER BY completed_ts DESC, started_ts DESC
LIMIT 1
""".strip()
    spark = FakeSparkSession(
        sql_rows_by_query={query: [{"pipeline_run_id": "upstream-run-999"}]}
    )

    upstream_run_id = resolve_latest_successful_upstream_run_id(spark, config, environment)

    assert upstream_run_id == "upstream-run-999"


def test_resolve_latest_successful_upstream_run_id_raises_when_missing() -> None:
    config, environment = _config_environment()
    query = f"""
SELECT pipeline_run_id
FROM {environment.catalog}.{environment.operations_schema}.b2s_pipeline_runs
WHERE pipeline_name = 'bronze_to_silver'
  AND release_name = '{config.release_name}'
  AND status = 'SUCCEEDED'
ORDER BY completed_ts DESC, started_ts DESC
LIMIT 1
""".strip()
    spark = FakeSparkSession(sql_rows_by_query={query: []})

    with pytest.raises(UpstreamSilverRunNotFoundError, match="No successful Bronze-to-Silver run was found"):
        resolve_latest_successful_upstream_run_id(spark, config, environment)


def test_collect_match_rows_for_analysis_date_returns_minimal_rows() -> None:
    _config, environment = _config_environment()
    query = f"""
SELECT
    match_date,
    completed_flag
FROM {environment.catalog}.{environment.silver_schema}.matches
""".strip()
    spark = FakeSparkSession(
        sql_rows_by_query={
            query: [
                {"match_date": "2026-06-24", "completed_flag": True},
                {"match_date": "2026-06-25", "completed_flag": False},
            ]
        }
    )

    rows = collect_match_rows_for_analysis_date(spark, environment)

    assert rows == [
        {"match_date": "2026-06-24", "completed_flag": True},
        {"match_date": "2026-06-25", "completed_flag": False},
    ]


def test_initialize_pipeline_run_creates_shared_pipeline_run_once() -> None:
    context = _pipeline_context()
    pipeline_runs_fqn = f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}"
    spark = FakeSparkSession(
        tables={
            pipeline_runs_fqn: FakeTable(rows=[]),
        }
    )

    initialize_pipeline_run(spark, context)

    assert any(query.startswith("CREATE TABLE IF NOT EXISTS") for query in spark.executed_sql)
    assert spark.created_schema == "schema-from-table"
    assert spark.created_records[0]["pipeline_run_id"] == "gold-run-123"


def test_initialize_pipeline_run_skips_existing_pipeline_run() -> None:
    context = _pipeline_context()
    pipeline_runs_fqn = f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}"
    spark = FakeSparkSession(
        tables={
            pipeline_runs_fqn: FakeTable(rows=[{"pipeline_run_id": "gold-run-123"}]),
        }
    )

    initialize_pipeline_run(spark, context)

    assert spark.created_records is None
