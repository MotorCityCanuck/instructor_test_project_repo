"""Tests for Bronze-to-Silver environment helpers."""

import pytest

from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import (
    EnvironmentValidationError,
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
    config = load_bronze_to_silver_config("napa_5k")

    environment = resolve_release_environment(config)

    assert environment.catalog == "workspace"
    assert environment.bronze_schema == "instructor_5k_bronze"
    assert environment.silver_schema == "instructor_5k_silver"
    assert environment.silver_reject_schema == "instructor_5k_silver_reject"
    assert environment.operations_schema == "instructor_ops"


def test_ensure_release_environment_creates_missing_schemas() -> None:
    config = load_bronze_to_silver_config("napa_5k")
    spark = FakeSparkSession(schema_rows=[])

    status = ensure_release_environment(spark, config, create_missing=True)

    assert status.release_environment.silver_schema == "instructor_5k_silver"
    joined = "\n".join(spark.sql_queries)
    assert "CREATE SCHEMA IF NOT EXISTS workspace.instructor_5k_bronze" in joined
    assert "CREATE SCHEMA IF NOT EXISTS workspace.instructor_5k_silver" in joined
    assert "CREATE SCHEMA IF NOT EXISTS workspace.instructor_5k_silver_reject" in joined
    assert "CREATE SCHEMA IF NOT EXISTS workspace.instructor_ops" in joined


def test_ensure_release_environment_rejects_missing_schema_when_creation_disabled() -> None:
    config = load_bronze_to_silver_config("napa_5k")
    spark = FakeSparkSession(schema_rows=[{"databaseName": "instructor_5k_bronze"}])

    with pytest.raises(EnvironmentValidationError, match="Required schema does not exist"):
        ensure_release_environment(spark, config, create_missing=False)


def test_ensure_release_environment_wraps_catalog_access_errors() -> None:
    config = load_bronze_to_silver_config("napa_5k")
    spark = FakeSparkSession(error_queries={"SHOW SCHEMAS IN workspace"})

    with pytest.raises(EnvironmentValidationError, match="Could not access catalog 'workspace'"):
        ensure_release_environment(spark, config)
