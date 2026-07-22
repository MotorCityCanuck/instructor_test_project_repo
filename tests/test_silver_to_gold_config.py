"""Tests for Silver-to-Gold configuration loading and validation."""

from pathlib import Path

import pytest

from napa_pipeline.silver_to_gold.config import (
    SilverToGoldConfigError,
    deep_merge,
    get_default_config_root,
    load_silver_to_gold_config,
)


def test_load_silver_to_gold_config_for_each_release_resolves_release_specific_schemas() -> None:
    expected = {
        "napa_5k": ("instructor_5k_silver", "instructor_5k_gold", "instructor_5k_gold_stage"),
        "napa_50k": ("instructor_50k_silver", "instructor_50k_gold", "instructor_50k_gold_stage"),
        "napa_250k": ("instructor_250k_silver", "instructor_250k_gold", "instructor_250k_gold_stage"),
    }

    for release_name, schemas in expected.items():
        config = load_silver_to_gold_config(release_name)
        assert config.release_name == release_name
        assert config.data["schemas"]["silver"] == schemas[0]
        assert config.data["schemas"]["gold"] == schemas[1]
        assert config.data["schemas"]["gold_stage"] == schemas[2]
        assert config.data["schemas"]["operations"] == "instructor_ops"


def test_load_silver_to_gold_config_returns_gold_tables_in_build_order() -> None:
    config = load_silver_to_gold_config("napa_5k")
    table_names = [table["table_name"] for table in config.enabled_gold_tables_in_build_order]

    assert table_names[0] == "competition_match_sides"
    assert table_names[-1] == "gold_run_summary"
    assert len(table_names) == 22


def test_load_silver_to_gold_config_rejects_unsupported_release() -> None:
    with pytest.raises(SilverToGoldConfigError, match="Unsupported release_name"):
        load_silver_to_gold_config("napa_999k")


def test_deep_merge_preserves_nested_base_values() -> None:
    merged = deep_merge(
        {"execution": {"model_enabled": True, "deterministic_seed": 42}},
        {"execution": {"model_enabled": False}},
    )

    assert merged["execution"]["model_enabled"] is False
    assert merged["execution"]["deterministic_seed"] == 42


def test_config_loader_rejects_duplicate_build_order(tmp_path: Path) -> None:
    config_root = _copy_config_tree(tmp_path)
    (config_root / "gold_tables.yml").write_text(
        """
gold_tables:
  first:
    enabled: true
    build_order: 10
    stage: foundation
    target: first
    transform: build_first
    primary_key: [id]
  second:
    enabled: true
    build_order: 10
    stage: foundation
    target: second
    transform: build_second
    primary_key: [id]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(SilverToGoldConfigError, match="Duplicate build_order"):
        load_silver_to_gold_config("napa_5k", config_root=config_root)


def test_config_loader_rejects_missing_configuration_file(tmp_path: Path) -> None:
    config_root = _copy_config_tree(tmp_path)
    (config_root / "models.yml").unlink()

    with pytest.raises(SilverToGoldConfigError, match="Missing configuration file"):
        load_silver_to_gold_config("napa_5k", config_root=config_root)


def test_config_loader_rejects_invalid_score_total(tmp_path: Path) -> None:
    config_root = _copy_config_tree(tmp_path)
    (config_root / "scorecards.yml").write_text(
        """
scorecards:
  player_weights:
    performance: 0.6
    rating: 0.3
    consistency: 0.2
  team_weights:
    partnership: 0.5
    player_strength: 0.3
    prediction: 0.2
  development_weights:
    trend: 0.5
    headroom: 0.3
    confidence: 0.2
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(SilverToGoldConfigError, match="player_weights must sum to 1.0"):
        load_silver_to_gold_config("napa_5k", config_root=config_root)


def test_config_loader_rejects_negative_weight(tmp_path: Path) -> None:
    config_root = _copy_config_tree(tmp_path)
    (config_root / "scorecards.yml").write_text(
        """
scorecards:
  player_weights:
    performance: -0.1
    rating: 0.6
    consistency: 0.5
  team_weights:
    partnership: 0.5
    player_strength: 0.3
    prediction: 0.2
  development_weights:
    trend: 0.5
    headroom: 0.3
    confidence: 0.2
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(SilverToGoldConfigError, match="must be non-negative"):
        load_silver_to_gold_config("napa_5k", config_root=config_root)


def test_config_loader_rejects_invalid_evidence_window(tmp_path: Path) -> None:
    config_root = _copy_config_tree(tmp_path)
    (config_root / "evidence_windows.yml").write_text(
        """
evidence_windows:
  primary_window_days: 365
  recent_window_days: 0
  trend_window_days: 180
  minimum_matches_for_ranking: 5
  minimum_matches_for_team_scorecard: 3
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(SilverToGoldConfigError, match="must be a positive integer"):
        load_silver_to_gold_config("napa_5k", config_root=config_root)


def test_config_loader_rejects_invalid_model_split(tmp_path: Path) -> None:
    config_root = _copy_config_tree(tmp_path)
    (config_root / "models.yml").write_text(
        """
models:
  baseline_model: analytical_rating_probability
  enabled: true
  train_fraction: 0.7
  validation_fraction: 0.4
  random_seed: 42
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(SilverToGoldConfigError, match="train_fraction \\+ validation_fraction must equal 1.0"):
        load_silver_to_gold_config("napa_5k", config_root=config_root)


def test_config_hash_is_stable_for_identical_loads() -> None:
    config_one = load_silver_to_gold_config("napa_5k")
    config_two = load_silver_to_gold_config("napa_5k")

    assert config_one.config_hash == config_two.config_hash


def _copy_config_tree(tmp_path: Path) -> Path:
    config_root = tmp_path / "silver_to_gold"
    source_root = get_default_config_root()
    for relative_path in [
        Path("base.yml"),
        Path("gold_tables.yml"),
        Path("eligibility.yml"),
        Path("evidence_windows.yml"),
        Path("ratings.yml"),
        Path("features.yml"),
        Path("models.yml"),
        Path("scorecards.yml"),
        Path("sensitivity.yml"),
        Path("quality_rules.yml"),
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
    return config_root
