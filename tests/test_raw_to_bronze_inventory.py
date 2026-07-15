"""Tests for Raw-to-Bronze inventory validation."""

from dataclasses import dataclass
from datetime import datetime

import pytest

from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.inventory import (
    RawInventoryError,
    validate_raw_inventory,
    validate_raw_inventory_and_readiness,
    validate_source_readiness,
)


@dataclass
class FakeFileInfo:
    """Minimal dbutils.fs.ls entry for tests."""

    name: str
    path: str
    size: int = 0
    modificationTime: int | None = None


class FakeFs:
    """Fake dbutils.fs implementation."""

    def __init__(self, entries):
        self.entries = entries

    def ls(self, _path: str):
        return self.entries


class FakeDbutils:
    """Fake dbutils wrapper."""

    def __init__(self, entries):
        self.fs = FakeFs(entries)


@dataclass
class FakeDataType:
    """Minimal Spark data type stub."""

    simple_string: str

    def simpleString(self) -> str:
        return self.simple_string


@dataclass
class FakeSchemaField:
    """Minimal Spark schema field stub."""

    name: str
    dataType: FakeDataType
    nullable: bool


@dataclass
class FakeSchema:
    """Minimal Spark schema stub."""

    fields: list[FakeSchemaField]


class FakeDataFrame:
    """Minimal DataFrame stub for inventory readiness tests."""

    def __init__(self, schema: FakeSchema, row_count: int):
        self.schema = schema
        self._row_count = row_count

    def count(self) -> int:
        return self._row_count


class FakeSparkReader:
    """Fake spark.read implementation keyed by path."""

    def __init__(self, datasets, error_paths=None):
        self.datasets = datasets
        self.error_paths = set(error_paths or [])

    def parquet(self, path: str):
        if path in self.error_paths:
            raise ValueError("broken parquet")
        return self.datasets[path]


class FakeSparkSession:
    """Fake Spark session wrapper."""

    def __init__(self, datasets, error_paths=None):
        self.read = FakeSparkReader(datasets, error_paths=error_paths)


def _expected_entries():
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    return [
        FakeFileInfo(
            name=source["file_name"],
            path=f"{environment.raw_volume_path}/{source['file_name']}",
            size=123,
            modificationTime=1721030400000,
        )
        for source in config.sources_in_build_order
    ]


def test_validate_raw_inventory_accepts_exact_inventory() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    dbutils = FakeDbutils(_expected_entries())

    status = validate_raw_inventory(dbutils, config, environment)

    assert len(status.expected_files) == 13
    assert len(status.discovered_files) == 13
    assert status.missing_files == ()
    assert status.unexpected_files == ()
    assert status.policy == "fail"
    assert isinstance(status.discovered_files[0].modification_ts, datetime)


def test_validate_raw_inventory_fails_on_missing_file() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    dbutils = FakeDbutils(_expected_entries()[:-1])

    with pytest.raises(RawInventoryError, match="Missing required Raw files"):
        validate_raw_inventory(dbutils, config, environment)


def test_validate_raw_inventory_fails_on_unexpected_file_when_policy_is_fail() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    dbutils = FakeDbutils(
        _expected_entries()
        + [
            FakeFileInfo(
                name="README.txt",
                path=f"{environment.raw_volume_path}/README.txt",
            )
        ]
    )

    with pytest.raises(RawInventoryError, match="Unexpected Raw files detected"):
        validate_raw_inventory(dbutils, config, environment)


def test_validate_raw_inventory_normalizes_trailing_slash_names() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    entries = [
        FakeFileInfo(
            name=f"{source['file_name']}/",
            path=f"{environment.raw_volume_path}/{source['file_name']}",
            size=123,
            modificationTime=1721030400000,
        )
        for source in config.sources_in_build_order
    ]

    status = validate_raw_inventory(FakeDbutils(entries), config, environment)

    assert status.unexpected_files == ()
    assert status.missing_files == ()
    assert status.discovered_files[0].file_name == "regions.parquet"


def test_validate_source_readiness_captures_row_counts_and_schema_hashes() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    inventory_status = validate_raw_inventory(FakeDbutils(_expected_entries()), config, environment)

    datasets = {
        entry.path: FakeDataFrame(
            FakeSchema(
                [
                    FakeSchemaField("id", FakeDataType("string"), False),
                    FakeSchemaField("loaded_at", FakeDataType("timestamp"), True),
                ]
            ),
            row_count=7,
        )
        for entry in _expected_entries()
    }
    spark = FakeSparkSession(datasets)

    readiness = validate_source_readiness(spark, inventory_status, config)

    assert len(readiness) == 13
    assert readiness[0].row_count == 7
    assert readiness[0].schema_fields[0]["column_name"] == "id"
    assert readiness[0].schema_fields[1]["data_type"] == "timestamp"
    assert readiness[0].schema_hash


def test_validate_source_readiness_raises_raw_inventory_error_on_unreadable_file() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    entries = _expected_entries()
    inventory_status = validate_raw_inventory(FakeDbutils(entries), config, environment)

    datasets = {
        entry.path: FakeDataFrame(
            FakeSchema([FakeSchemaField("id", FakeDataType("string"), False)]),
            row_count=1,
        )
        for entry in entries
    }
    broken_path = entries[0].path
    spark = FakeSparkSession(datasets, error_paths={broken_path})

    with pytest.raises(RawInventoryError, match="could not be read as Parquet"):
        validate_source_readiness(spark, inventory_status, config)


def test_validate_raw_inventory_and_readiness_returns_combined_result() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    entries = _expected_entries()
    datasets = {
        entry.path: FakeDataFrame(
            FakeSchema([FakeSchemaField("id", FakeDataType("string"), False)]),
            row_count=2,
        )
        for entry in entries
    }

    result = validate_raw_inventory_and_readiness(
        FakeSparkSession(datasets),
        FakeDbutils(entries),
        config,
        environment,
    )

    assert len(result.inventory_status.discovered_files) == 13
    assert len(result.source_readiness) == 13
