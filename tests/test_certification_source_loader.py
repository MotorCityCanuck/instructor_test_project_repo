"""Tests for Raw certification source loading and inventory rules."""

from dataclasses import dataclass
from datetime import datetime

import pytest

from napa_pipeline.certification.config import load_certification_config
from napa_pipeline.certification.environment import resolve_release_environment
from napa_pipeline.certification.source_loader import run_inventory_certification


@dataclass
class FakeFileInfo:
    """Minimal dbutils.fs.ls entry for tests."""

    name: str
    path: str
    size: int = 0
    modificationTime: int | None = None


class FakeFs:
    """Fake dbutils.fs implementation."""

    def __init__(self, entries, *, missing_path: bool = False, head_payloads=None):
        self.entries = entries
        self.missing_path = missing_path
        self.head_payloads = head_payloads or {}

    def ls(self, _path: str):
        if self.missing_path:
            raise FileNotFoundError("path not found")
        return self.entries

    def head(self, path: str, _max_bytes: int):
        if path not in self.head_payloads:
            raise FileNotFoundError("missing head payload")
        return self.head_payloads[path]


class FakeDbutils:
    """Fake dbutils wrapper."""

    def __init__(self, entries, *, missing_path: bool = False, head_payloads=None):
        self.fs = FakeFs(
            entries,
            missing_path=missing_path,
            head_payloads=head_payloads,
        )


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
    """Minimal DataFrame stub for certification loader tests."""

    def __init__(self, schema: FakeSchema, row_count: int):
        self.schema = schema
        self._row_count = row_count
        self.views: list[str] = []

    def count(self) -> int:
        return self._row_count

    def createOrReplaceTempView(self, name: str) -> None:
        self.views.append(name)


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
    config = load_certification_config("5k")
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


def _datasets_for_entries(entries, row_count: int = 7):
    return {
        entry.path: FakeDataFrame(
            FakeSchema(
                [
                    FakeSchemaField("id", FakeDataType("string"), False),
                    FakeSchemaField("loaded_at", FakeDataType("timestamp"), True),
                ]
            ),
            row_count=row_count,
        )
        for entry in entries
    }


def _get_rule(result, rule_id: str):
    return next(rule for rule in result.rule_results if rule.rule_id == rule_id)


def test_run_inventory_certification_succeeds_for_exact_inventory() -> None:
    config = load_certification_config("5k")
    environment = resolve_release_environment(config)
    entries = _expected_entries()
    datasets = _datasets_for_entries(entries, row_count=7)

    result = run_inventory_certification(
        FakeSparkSession(datasets),
        FakeDbutils(entries),
        config,
        environment,
    )

    assert result.path_exists is True
    assert len(result.loaded_sources) == 13
    assert _get_rule(result, "RAW_REQUIRED_FILES_PRESENT").status == "PASS"
    assert _get_rule(result, "RAW_PARQUET_READABLE").status == "PASS"
    assert _get_rule(result, "RAW_NONEMPTY_SOURCE").status == "PASS"


def test_run_inventory_certification_fails_when_release_path_is_missing() -> None:
    config = load_certification_config("5k")
    environment = resolve_release_environment(config)

    result = run_inventory_certification(
        FakeSparkSession({}),
        FakeDbutils([], missing_path=True),
        config,
        environment,
    )

    assert result.path_exists is False
    assert len(result.loaded_sources) == 0
    assert _get_rule(result, "RAW_PATH_EXISTS").status == "FAIL"


def test_run_inventory_certification_fails_on_missing_required_file() -> None:
    config = load_certification_config("5k")
    environment = resolve_release_environment(config)
    entries = _expected_entries()[:-1]
    datasets = _datasets_for_entries(entries)

    result = run_inventory_certification(
        FakeSparkSession(datasets),
        FakeDbutils(entries),
        config,
        environment,
    )

    rule = _get_rule(result, "RAW_REQUIRED_FILES_PRESENT")
    assert rule.status == "FAIL"
    assert rule.affected_count == 1


def test_run_inventory_certification_fails_on_duplicate_domain_file() -> None:
    config = load_certification_config("5k")
    environment = resolve_release_environment(config)
    entries = _expected_entries()
    duplicate = FakeFileInfo(
        name=entries[0].name,
        path=f"{environment.raw_volume_path}/copy_{entries[0].name}",
        size=123,
        modificationTime=1721030400000,
    )
    datasets = _datasets_for_entries(entries)

    result = run_inventory_certification(
        FakeSparkSession(datasets),
        FakeDbutils(entries + [duplicate]),
        config,
        environment,
    )

    assert _get_rule(result, "RAW_UNEXPECTED_DUPLICATE_DOMAIN").status == "FAIL"


def test_run_inventory_certification_fails_on_unreadable_parquet() -> None:
    config = load_certification_config("5k")
    environment = resolve_release_environment(config)
    entries = _expected_entries()
    datasets = _datasets_for_entries(entries)
    broken_path = entries[0].path

    result = run_inventory_certification(
        FakeSparkSession(datasets, error_paths={broken_path}),
        FakeDbutils(entries),
        config,
        environment,
    )

    assert _get_rule(result, "RAW_PARQUET_READABLE").status == "FAIL"


def test_run_inventory_certification_fails_on_empty_source() -> None:
    config = load_certification_config("5k")
    environment = resolve_release_environment(config)
    entries = _expected_entries()
    datasets = _datasets_for_entries(entries)
    datasets[entries[0].path] = FakeDataFrame(
        FakeSchema([FakeSchemaField("id", FakeDataType("string"), False)]),
        row_count=0,
    )

    result = run_inventory_certification(
        FakeSparkSession(datasets),
        FakeDbutils(entries),
        config,
        environment,
    )

    assert _get_rule(result, "RAW_NONEMPTY_SOURCE").status == "FAIL"


def test_run_inventory_certification_detects_manifest_row_count_mismatch() -> None:
    config = load_certification_config("5k")
    environment = resolve_release_environment(config)
    entries = _expected_entries()
    manifest_entry = FakeFileInfo(
        name="release_manifest.json",
        path=f"{environment.raw_volume_path}/release_manifest.json",
        size=512,
        modificationTime=int(datetime(2026, 7, 27).timestamp() * 1000),
    )
    entries_with_manifest = entries + [manifest_entry]
    datasets = _datasets_for_entries(entries, row_count=7)
    head_payloads = {
        manifest_entry.path: '{"file_row_counts":{"regions.parquet":999}}'
    }

    result = run_inventory_certification(
        FakeSparkSession(datasets),
        FakeDbutils(entries_with_manifest, head_payloads=head_payloads),
        config,
        environment,
    )

    assert result.manifest is not None
    assert _get_rule(result, "RAW_MANIFEST_ROW_COUNT_MATCH").status == "FAIL"
