"""Tests for Silver-to-Gold environment and runtime-context helpers."""

from datetime import date

import pytest

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import (
    EnvironmentValidationError,
    build_runtime_context,
    ensure_release_environment,
    resolve_release_environment,
)


class FakeRow:
    """Minimal row stub with asDict support."""

    def __init__(self, mapping):
        self.mapping = mapping

    def asDict(self, recursive: bool = True):
        return dict(self.mapping)


class FakeCollectResult:
    """Minimal collect wrapper."""

    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return [FakeRow(row) for row in self.rows]


class FakeSparkSession:
    """Minimal Spark session for environment tests."""

    def __init__(self, schema_rows=None, error_queries=None):
        self.schema_rows = schema_rows or []
        self.error_queries = set(error_queries or [])
        self.sql_queries: list[str] = []

    def sql(self, query: str):
        self.sql_queries.append(query)
        if query in self.error_queries:
            raise RuntimeError("simulated failure")
        if query.startswith("SHOW SCHEMAS IN "):
            return FakeCollectResult(self.schema_rows)
        return FakeCollectResult([])


def test_resolve_release_environment_uses_release_specific_schema_names() -> None:
    config = load_silver_to_gold_config("napa_5k")

    environment = resolve_release_environment(config)

    assert environment.catalog == "workspace"
    assert environment.silver_schema == "instructor_5k_silver"
    assert environment.gold_schema == "instructor_5k_gold"
    assert environment.gold_stage_schema == "instructor_5k_gold_stage"
    assert environment.operations_schema == "instructor_ops"


def test_ensure_release_environment_creates_missing_schemas() -> None:
    config = load_silver_to_gold_config("napa_5k")
    spark = FakeSparkSession(schema_rows=[])

    status = ensure_release_environment(spark, config, create_missing=True)

    assert status.release_environment.gold_schema == "instructor_5k_gold"
    joined = "\n".join(spark.sql_queries)
    assert "CREATE SCHEMA IF NOT EXISTS workspace.instructor_5k_silver" in joined
    assert "CREATE SCHEMA IF NOT EXISTS workspace.instructor_5k_gold" in joined
    assert "CREATE SCHEMA IF NOT EXISTS workspace.instructor_5k_gold_stage" in joined
    assert "CREATE SCHEMA IF NOT EXISTS workspace.instructor_ops" in joined


def test_ensure_release_environment_rejects_missing_schema_when_creation_disabled() -> None:
    config = load_silver_to_gold_config("napa_5k")
    spark = FakeSparkSession(schema_rows=[{"databaseName": "instructor_5k_silver"}])

    with pytest.raises(EnvironmentValidationError, match="Required schema does not exist"):
        ensure_release_environment(spark, config, create_missing=False)


def test_ensure_release_environment_wraps_catalog_access_errors() -> None:
    config = load_silver_to_gold_config("napa_5k")
    spark = FakeSparkSession(error_queries={"SHOW SCHEMAS IN workspace"})

    with pytest.raises(EnvironmentValidationError, match="Could not access catalog 'workspace'"):
        ensure_release_environment(spark, config)


def test_build_runtime_context_resolves_analysis_date_and_core_fields() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    context = build_runtime_context(
        config,
        environment,
        upstream_silver_run_id="upstream-run-123",
        match_rows=[
            {"match_date": "2026-06-15", "completed_flag": True},
            {"match_date": "2026-06-25", "completed_flag": True},
        ],
    )

    assert context.release_name == "napa_5k"
    assert context.release_role == "development"
    assert context.analysis_as_of_date == date(2026, 6, 25)
    assert context.scoring_scenario == "BALANCED"
    assert context.model_enabled is True
    assert context.authoritative_recommendation_flag is False
    assert context.deterministic_seed == 42
    assert context.upstream_silver_run_id == "upstream-run-123"


def test_build_runtime_context_respects_explicit_analysis_date() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    context = build_runtime_context(
        config,
        environment,
        upstream_silver_run_id="upstream-run-123",
        match_rows=[{"match_date": "2026-06-25", "completed_flag": True}],
        analysis_as_of_date="2026-06-10",
    )

    assert context.analysis_as_of_date == date(2026, 6, 10)


def test_build_runtime_context_rejects_empty_upstream_run_id() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    with pytest.raises(EnvironmentValidationError, match="upstream_silver_run_id must not be empty"):
        build_runtime_context(
            config,
            environment,
            upstream_silver_run_id="",
            match_rows=[{"match_date": "2026-06-25", "completed_flag": True}],
        )
