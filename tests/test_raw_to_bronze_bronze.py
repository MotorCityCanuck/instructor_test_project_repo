"""Tests for Raw-to-Bronze Bronze publication helpers."""

from dataclasses import dataclass
from datetime import datetime

import pytest

from napa_pipeline.raw_to_bronze.bronze import (
    BronzePublicationError,
    BRONZE_METADATA_COLUMNS,
    build_bronze_table,
    finalize_bronze_table_metadata,
    get_bronze_table_comment,
    get_bronze_table_properties,
    get_bronze_target_table_fqn,
    publish_bronze_table,
)
from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.inventory import SourceReadinessRecord
from napa_pipeline.raw_to_bronze.operations import create_pipeline_context


class FakeSparkSession:
    """Capture SQL statements executed by Bronze helpers."""

    def __init__(self):
        self.executed_queries: list[str] = []

    def sql(self, query: str):
        self.executed_queries.append(query)
        return None


class FakeWriteBuilder:
    """Minimal DataFrameWriter stub."""

    def __init__(self):
        self.calls: list[tuple[str, str | None]] = []

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
    """Minimal DataFrame stub with a writer."""

    def __init__(self):
        self.write = FakeWriteBuilder()


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


class FakeBronzeDataFrame:
    """Minimal Bronze DataFrame stub."""

    def __init__(self, row_count: int):
        self._row_count = row_count
        self.schema = FakeSchema(
            [
                FakeSchemaField("id", FakeDataType("string"), False),
                FakeSchemaField(BRONZE_METADATA_COLUMNS[0], FakeDataType("string"), False),
            ]
        )

    def count(self) -> int:
        return self._row_count


def _context():
    config = load_raw_to_bronze_config("napa_5k")
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment, pipeline_run_id="run-123")
    return config, environment, context


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
        ),
    )


def test_get_bronze_target_table_fqn_uses_release_specific_schema() -> None:
    _, environment, _ = _context()

    target_table = get_bronze_target_table_fqn(environment, _source_config())

    assert target_table == "workspace.instructor_5k_bronze.regions"


def test_get_bronze_table_comment_and_properties_are_standardized() -> None:
    _, _, context = _context()

    comment = get_bronze_table_comment(context.release_name, "regions")
    properties = get_bronze_table_properties(context)

    assert "regions Parquet file" in comment
    assert properties["napa.layer"] == "bronze"
    assert properties["napa.release"] == "napa_5k"


def test_publish_bronze_table_uses_configured_overwrite_mode() -> None:
    config, _, _ = _context()
    dataframe = FakeDataFrame()

    publish_bronze_table(
        dataframe,
        config,
        "workspace.instructor_5k_bronze.regions",
    )

    assert ("format", "delta") in dataframe.write.calls
    assert ("mode", "overwrite") in dataframe.write.calls
    assert ("option:overwriteSchema", "true") in dataframe.write.calls
    assert ("saveAsTable", "workspace.instructor_5k_bronze.regions") in dataframe.write.calls


def test_finalize_bronze_table_metadata_emits_comment_and_properties_sql() -> None:
    _, _, context = _context()
    spark = FakeSparkSession()

    finalize_bronze_table_metadata(
        spark,
        context,
        "regions",
        "workspace.instructor_5k_bronze.regions",
    )

    joined = "\n".join(spark.executed_queries)
    assert "COMMENT ON TABLE workspace.instructor_5k_bronze.regions" in joined
    assert "ALTER TABLE workspace.instructor_5k_bronze.regions SET TBLPROPERTIES" in joined


def test_build_bronze_table_returns_result_metadata(monkeypatch) -> None:
    config, environment, context = _context()
    spark = FakeSparkSession()
    bronze_dataframe = FakeBronzeDataFrame(row_count=10)

    monkeypatch.setattr(
        "napa_pipeline.raw_to_bronze.bronze.read_raw_source",
        lambda _spark, _readiness: object(),
    )
    monkeypatch.setattr(
        "napa_pipeline.raw_to_bronze.bronze.build_bronze_dataframe",
        lambda _source_df, _context, _readiness, ingested_ts=None: bronze_dataframe,
    )
    monkeypatch.setattr(
        "napa_pipeline.raw_to_bronze.bronze.publish_bronze_table",
        lambda _bronze_df, _config, _target: None,
    )
    monkeypatch.setattr(
        "napa_pipeline.raw_to_bronze.bronze.finalize_bronze_table_metadata",
        lambda _spark, _context, _source_name, _target: None,
    )

    result = build_bronze_table(
        spark,
        config,
        context,
        environment,
        _source_config(),
        _source_readiness(),
    )

    assert result.target_table_fqn == "workspace.instructor_5k_bronze.regions"
    assert result.source_row_count == 10
    assert result.bronze_row_count == 10
    assert result.source_schema_hash == "raw-schema-hash"
    assert result.bronze_schema_hash


def test_build_bronze_table_wraps_failures_with_domain_error(monkeypatch) -> None:
    config, environment, context = _context()

    monkeypatch.setattr(
        "napa_pipeline.raw_to_bronze.bronze.read_raw_source",
        lambda _spark, _readiness: (_ for _ in ()).throw(ValueError("boom")),
    )

    with pytest.raises(BronzePublicationError, match="Failed to build Bronze table"):
        build_bronze_table(
            FakeSparkSession(),
            config,
            context,
            environment,
            _source_config(),
            _source_readiness(),
        )
