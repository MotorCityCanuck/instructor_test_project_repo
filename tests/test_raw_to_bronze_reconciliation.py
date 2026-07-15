"""Tests for Raw-to-Bronze Bronze reconciliation helpers."""

from dataclasses import dataclass
from datetime import datetime

import pytest

from napa_pipeline.raw_to_bronze.reconciliation import (
    ReconciliationError,
    reconcile_bronze_table,
)
from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.inventory import SourceReadinessRecord


class FakeSparkSession:
    """Fake Spark session keyed by table name."""

    def __init__(self, tables=None, error_tables=None):
        self.tables = tables or {}
        self.error_tables = set(error_tables or [])

    def table(self, table_name: str):
        if table_name in self.error_tables:
            raise ValueError("missing table")
        return self.tables[table_name]


class FakeDataFrame:
    """Minimal DataFrame stub for reconciliation."""

    def __init__(self, columns, row_count: int):
        self.columns = columns
        self._row_count = row_count

    def count(self) -> int:
        return self._row_count


def _environment():
    config = load_raw_to_bronze_config("napa_5k")
    return resolve_release_environment(config)


def _source_config():
    return {
        "source_name": "regions",
        "file_name": "regions.parquet",
        "bronze_table": "regions",
    }


def _source_readiness():
    return SourceReadinessRecord(
        source_name="regions",
        file_name="regions.parquet",
        file_path="dbfs:/Volumes/workspace/instructor_5k_raw/napa_files/regions.parquet",
        file_size=123,
        modification_ts=datetime(2026, 7, 15, 12, 0, 0),
        row_count=10,
        schema_hash="raw-schema-hash",
        schema_fields=(
            {"column_name": "id", "data_type": "string", "nullable": False},
            {"column_name": "name", "data_type": "string", "nullable": True},
        ),
    )


def test_reconcile_bronze_table_returns_match_for_valid_bronze_output() -> None:
    environment = _environment()
    target_table = "workspace.instructor_5k_bronze.regions"
    spark = FakeSparkSession(
        tables={
            target_table: FakeDataFrame(
                columns=[
                    "id",
                    "name",
                    "_pipeline_run_id",
                    "_pipeline_name",
                    "_pipeline_version",
                    "_release_name",
                    "_source_file_name",
                    "_source_file_path",
                    "_source_file_size",
                    "_source_file_modification_ts",
                    "_ingested_ts",
                    "_source_record_hash",
                ],
                row_count=10,
            )
        }
    )

    result = reconcile_bronze_table(
        spark,
        environment,
        _source_config(),
        _source_readiness(),
    )

    assert result.status == "MATCHED"
    assert result.row_count_difference == 0
    assert result.raw_business_column_count == 2
    assert result.bronze_business_column_count == 2
    assert result.metadata_column_count == 10
    assert result.missing_metadata_columns == ()


def test_reconcile_bronze_table_detects_schema_and_count_mismatches() -> None:
    environment = _environment()
    target_table = "workspace.instructor_5k_bronze.regions"
    spark = FakeSparkSession(
        tables={
            target_table: FakeDataFrame(
                columns=[
                    "id",
                    "unexpected_flag",
                    "_pipeline_run_id",
                    "_pipeline_name",
                ],
                row_count=8,
            )
        }
    )

    result = reconcile_bronze_table(
        spark,
        environment,
        _source_config(),
        _source_readiness(),
    )

    assert result.status == "MISMATCH"
    assert result.row_count_difference == -2
    assert result.missing_metadata_columns
    assert result.missing_business_columns == ("name",)
    assert result.unexpected_business_columns == ("unexpected_flag",)


def test_reconcile_bronze_table_wraps_missing_table_errors() -> None:
    environment = _environment()
    spark = FakeSparkSession(
        tables={},
        error_tables={"workspace.instructor_5k_bronze.regions"},
    )

    with pytest.raises(ReconciliationError, match="Could not read Bronze table"):
        reconcile_bronze_table(
            spark,
            environment,
            _source_config(),
            _source_readiness(),
        )
