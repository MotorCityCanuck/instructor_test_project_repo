"""Tests for Raw-to-Bronze release environment setup and validation."""

from dataclasses import dataclass

import pytest

from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import (
    EnvironmentValidationError,
    ensure_release_environment,
    resolve_release_environment,
)


@dataclass
class FakeRow:
    """Minimal row-like object for fake Spark SQL responses."""

    values: dict[str, object]

    def asDict(self) -> dict[str, object]:
        return self.values


class FakeQueryResult:
    """Collectable fake Spark SQL result."""

    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class FakeSparkSession:
    """Small fake Spark session for environment SQL contract tests."""

    def __init__(self, schema_names=None, volume_names=None, fail_queries=None):
        self.schema_names = set(schema_names or [])
        self.volume_names = set(volume_names or [])
        self.fail_queries = tuple(fail_queries or [])
        self.executed_queries: list[str] = []

    def sql(self, query: str):
        self.executed_queries.append(query)
        for prefix in self.fail_queries:
            if query.startswith(prefix):
                raise RuntimeError(f"Query failed: {query}")

        if query.startswith("SHOW SCHEMAS IN "):
            return FakeQueryResult(
                [FakeRow({"databaseName": schema_name}) for schema_name in self.schema_names]
            )
        if query.startswith("SHOW VOLUMES IN "):
            return FakeQueryResult(
                [FakeRow({"volume_name": volume_name}) for volume_name in self.volume_names]
            )
        if query.startswith("CREATE SCHEMA IF NOT EXISTS "):
            schema_name = query.split(".")[-1]
            self.schema_names.add(schema_name)
            return FakeQueryResult([])
        if query.startswith("CREATE VOLUME IF NOT EXISTS "):
            volume_name = query.split(".")[-1]
            self.volume_names.add(volume_name)
            return FakeQueryResult([])

        raise AssertionError(f"Unexpected query: {query}")


def test_resolve_release_environment_for_50k() -> None:
    config = load_raw_to_bronze_config("napa_50k")

    environment = resolve_release_environment(config)

    assert environment.catalog == "workspace"
    assert environment.raw_schema == "instructor_50k_raw"
    assert environment.bronze_schema == "instructor_50k_bronze"
    assert environment.operations_schema == "instructor_ops"
    assert environment.raw_volume_fqn == "workspace.instructor_50k_raw.napa_files"


def test_ensure_release_environment_creates_missing_objects() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    spark = FakeSparkSession()

    status = ensure_release_environment(spark, config, create_missing=True)

    assert [item.existed for item in status.schema_statuses] == [False, False, False]
    assert status.volume_status.existed is False
    assert "CREATE SCHEMA IF NOT EXISTS workspace.instructor_5k_raw" in spark.executed_queries
    assert (
        "CREATE VOLUME IF NOT EXISTS workspace.instructor_5k_raw.napa_files"
        in spark.executed_queries
    )


def test_ensure_release_environment_reports_existing_objects() -> None:
    config = load_raw_to_bronze_config("napa_250k")
    spark = FakeSparkSession(
        schema_names={"instructor_250k_raw", "instructor_250k_bronze", "instructor_ops"},
        volume_names={"napa_files"},
    )

    status = ensure_release_environment(spark, config, create_missing=True)

    assert all(item.existed for item in status.schema_statuses)
    assert status.volume_status.existed is True
    assert not any(query.startswith("CREATE SCHEMA") for query in spark.executed_queries)
    assert not any(query.startswith("CREATE VOLUME") for query in spark.executed_queries)


def test_ensure_release_environment_fails_when_create_missing_disabled() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    spark = FakeSparkSession()

    with pytest.raises(EnvironmentValidationError, match="Required schema does not exist"):
        ensure_release_environment(spark, config, create_missing=False)


def test_ensure_release_environment_fails_when_catalog_unavailable() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    spark = FakeSparkSession(fail_queries=("SHOW SCHEMAS IN workspace",))

    with pytest.raises(EnvironmentValidationError, match="Could not access catalog"):
        ensure_release_environment(spark, config, create_missing=True)
