"""Tests for Bronze-to-Silver configuration loading and validation."""

from pathlib import Path

import pytest

from napa_pipeline.bronze_to_silver.config import (
    BronzeToSilverConfigError,
    deep_merge,
    get_default_config_root,
    load_bronze_to_silver_config,
)


def test_load_bronze_to_silver_config_for_5k_resolves_release_specific_schemas() -> None:
    config = load_bronze_to_silver_config("napa_5k")

    assert config.release_name == "napa_5k"
    assert config.data["schemas"]["bronze"] == "instructor_5k_bronze"
    assert config.data["schemas"]["silver"] == "instructor_5k_silver"
    assert config.data["schemas"]["silver_reject"] == "instructor_5k_silver_reject"
    assert config.data["schemas"]["operations"] == "instructor_ops"


def test_load_bronze_to_silver_config_returns_silver_tables_in_build_order() -> None:
    config = load_bronze_to_silver_config("napa_5k")
    table_names = [table["table_name"] for table in config.silver_tables_in_build_order]

    assert table_names[0] == "monthly_batches"
    assert table_names[-1] == "match_games"
    assert len(table_names) == 13


def test_load_bronze_to_silver_config_rejects_unsupported_release() -> None:
    with pytest.raises(BronzeToSilverConfigError, match="Unsupported release_name"):
        load_bronze_to_silver_config("napa_999k")


def test_deep_merge_preserves_nested_base_values() -> None:
    merged = deep_merge(
        {"execution": {"fail_fast": True, "publish_only_after_validation": True}},
        {"execution": {"fail_fast": False}},
    )

    assert merged["execution"]["fail_fast"] is False
    assert merged["execution"]["publish_only_after_validation"] is True


def test_config_loader_rejects_duplicate_build_order(tmp_path: Path) -> None:
    config_root = tmp_path / "bronze_to_silver"
    environments_root = config_root / "environments"
    environments_root.mkdir(parents=True)

    source_root = get_default_config_root()
    for relative_path in [
        Path("base.yml"),
        Path("domains.yml"),
        Path("quality_rules.yml"),
        Path("logging.yml"),
        Path("sources.yml"),
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

    (config_root / "silver_tables.yml").write_text(
        """
silver_tables:
  first:
    enabled: true
    source: regions
    target: first
    stage: reference
    build_order: 10
    transform: build_regions
    primary_key: [id]
    reject_table: first_exceptions
  second:
    enabled: true
    source: clubs
    target: second
    stage: reference
    build_order: 10
    transform: build_clubs
    primary_key: [id]
    reject_table: second_exceptions
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(BronzeToSilverConfigError, match="Duplicate build_order"):
        load_bronze_to_silver_config("napa_5k", config_root=config_root)


def test_config_loader_rejects_undefined_source_reference(tmp_path: Path) -> None:
    config_root = tmp_path / "bronze_to_silver"
    source_root = get_default_config_root()
    for relative_path in [
        Path("base.yml"),
        Path("domains.yml"),
        Path("quality_rules.yml"),
        Path("logging.yml"),
        Path("sources.yml"),
        Path("silver_tables.yml"),
        Path("environments/napa_5k.yml"),
        Path("environments/napa_50k.yml"),
        Path("environments/napa_250k.yml"),
    ]:
        target_path = config_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        content = (source_root / relative_path).read_text(encoding="utf-8")
        if relative_path == Path("silver_tables.yml"):
            content = content.replace("source: monthly_batches", "source: missing_source", 1)
        target_path.write_text(content, encoding="utf-8")

    with pytest.raises(BronzeToSilverConfigError, match="undefined source 'missing_source'"):
        load_bronze_to_silver_config("napa_5k", config_root=config_root)


def test_config_loader_rejects_undefined_transform_name(tmp_path: Path) -> None:
    config_root = tmp_path / "bronze_to_silver"
    source_root = get_default_config_root()
    for relative_path in [
        Path("base.yml"),
        Path("domains.yml"),
        Path("quality_rules.yml"),
        Path("logging.yml"),
        Path("sources.yml"),
        Path("silver_tables.yml"),
        Path("environments/napa_5k.yml"),
        Path("environments/napa_50k.yml"),
        Path("environments/napa_250k.yml"),
    ]:
        target_path = config_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        content = (source_root / relative_path).read_text(encoding="utf-8")
        if relative_path == Path("silver_tables.yml"):
            content = content.replace("transform: build_monthly_batches", "transform: build_unknown", 1)
        target_path.write_text(content, encoding="utf-8")

    with pytest.raises(BronzeToSilverConfigError, match="undefined transform 'build_unknown'"):
        load_bronze_to_silver_config("napa_5k", config_root=config_root)
