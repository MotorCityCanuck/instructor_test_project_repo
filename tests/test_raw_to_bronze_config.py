"""Tests for Raw-to-Bronze configuration loading and validation."""

from pathlib import Path

import pytest

from napa_pipeline.raw_to_bronze.config import (
    RawToBronzeConfigError,
    deep_merge,
    get_default_config_root,
    load_raw_to_bronze_config,
)


def test_load_raw_to_bronze_config_for_5k_resolves_release_specific_paths() -> None:
    config = load_raw_to_bronze_config("napa_5k")

    assert config.release_name == "napa_5k"
    assert config.data["schemas"]["raw"] == "instructor_5k_raw"
    assert config.data["schemas"]["bronze"] == "instructor_5k_bronze"
    assert config.data["schemas"]["operations"] == "instructor_ops"
    assert config.data["volume"]["path"] == (
        "/Volumes/workspace/instructor_5k_raw/napa_files"
    )


def test_load_raw_to_bronze_config_returns_sources_in_build_order() -> None:
    config = load_raw_to_bronze_config("napa_5k")
    source_names = [source["source_name"] for source in config.sources_in_build_order]

    assert source_names[0] == "regions"
    assert source_names[-1] == "monthly_batches"
    assert len(source_names) == 13


def test_load_raw_to_bronze_config_rejects_unsupported_release() -> None:
    with pytest.raises(RawToBronzeConfigError, match="Unsupported release_name"):
        load_raw_to_bronze_config("napa_999k")


def test_deep_merge_preserves_nested_base_values() -> None:
    merged = deep_merge(
        {"execution": {"fail_fast": True, "require_exact_source_inventory": True}},
        {"execution": {"fail_fast": False}},
    )

    assert merged["execution"]["fail_fast"] is False
    assert merged["execution"]["require_exact_source_inventory"] is True


def test_config_loader_rejects_duplicate_build_order(tmp_path: Path) -> None:
    config_root = tmp_path / "raw_to_bronze"
    environments_root = config_root / "environments"
    environments_root.mkdir(parents=True)

    source_root = get_default_config_root()
    for relative_path in [
        Path("base.yml"),
        Path("logging.yml"),
        Path("environments/napa_5k.yml"),
        Path("environments/napa_50k.yml"),
        Path("environments/napa_250k.yml"),
    ]:
        target_path = config_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(
            (source_root / relative_path).read_text(encoding="utf-8"),
            encoding="utf-8",
        )

    (config_root / "raw_sources.yml").write_text(
        """
sources:
  first:
    enabled: true
    file_name: first.parquet
    bronze_table: first
    build_order: 10
    grain: first
    key_columns: [id]
  second:
    enabled: true
    file_name: second.parquet
    bronze_table: second
    build_order: 10
    grain: second
    key_columns: [id]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(RawToBronzeConfigError, match="Duplicate build_order"):
        load_raw_to_bronze_config("napa_5k", config_root=config_root)
