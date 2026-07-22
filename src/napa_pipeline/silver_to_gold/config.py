"""Configuration loader for the NAPA Silver-to-Gold pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
from pathlib import Path
import re
from typing import Any

import yaml


PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")
ALLOWED_RELEASES = ("napa_5k", "napa_50k", "napa_250k")
ALLOWED_PROCESSING_MODES = ("full_refresh",)
REQUIRED_TOP_LEVEL_KEYS = (
    "project",
    "runtime",
    "objects",
    "execution",
    "publication",
    "time_controls",
    "paths",
    "source_contract",
    "release",
    "schemas",
    "performance",
    "gold_tables",
    "eligibility",
    "evidence_windows",
    "ratings",
    "features",
    "models",
    "scorecards",
    "sensitivity",
    "quality_rules",
    "logging",
)


class SilverToGoldConfigError(ValueError):
    """Raised when Silver-to-Gold configuration is invalid."""


@dataclass(frozen=True)
class SilverToGoldConfig:
    """Resolved configuration for one Silver-to-Gold release."""

    data: dict[str, Any]
    config_hash: str
    config_root: Path

    @property
    def release_name(self) -> str:
        return str(self.data["release"]["release_name"])

    @property
    def release_role(self) -> str:
        return str(self.data["release"]["release_role"])

    @property
    def scoring_scenario(self) -> str:
        return str(self.data["execution"]["scoring_scenario"])

    @property
    def model_enabled(self) -> bool:
        return bool(self.data["models"]["enabled"])

    @property
    def deterministic_seed(self) -> int:
        return int(self.data["execution"]["deterministic_seed"])

    @property
    def enabled_gold_tables_in_build_order(self) -> list[dict[str, Any]]:
        enabled_tables = [
            {"table_name": name, **table}
            for name, table in self.data["gold_tables"].items()
            if table.get("enabled", False)
        ]
        return sorted(enabled_tables, key=lambda item: item["build_order"])


def get_default_config_root() -> Path:
    """Return the repository config directory for Silver-to-Gold."""
    return Path(__file__).resolve().parents[3] / "config" / "silver_to_gold"


def load_silver_to_gold_config(
    release_name: str,
    config_root: Path | str | None = None,
) -> SilverToGoldConfig:
    """Load, merge, resolve, and validate Silver-to-Gold configuration."""
    if release_name not in ALLOWED_RELEASES:
        raise SilverToGoldConfigError(
            f"Unsupported release_name '{release_name}'. "
            f"Allowed values: {', '.join(ALLOWED_RELEASES)}."
        )

    root = Path(config_root) if config_root else get_default_config_root()
    merged = _load_yaml_file(root / "base.yml")
    merged = deep_merge(merged, _load_yaml_file(root / "environments" / f"{release_name}.yml"))
    for file_name in (
        "gold_tables.yml",
        "eligibility.yml",
        "evidence_windows.yml",
        "ratings.yml",
        "features.yml",
        "models.yml",
        "scorecards.yml",
        "sensitivity.yml",
        "quality_rules.yml",
        "logging.yml",
    ):
        merged = deep_merge(merged, _load_yaml_file(root / file_name))

    merged = resolve_placeholders(merged)
    validate_config(merged, expected_release_name=release_name)

    config_hash = hashlib.sha256(
        yaml.safe_dump(merged, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return SilverToGoldConfig(data=merged, config_hash=config_hash, config_root=root)


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge dictionaries without mutating the inputs."""
    merged: dict[str, Any] = {}
    for key in base.keys() | override.keys():
        base_value = base.get(key)
        override_value = override.get(key)
        if isinstance(base_value, dict) and isinstance(override_value, dict):
            merged[key] = deep_merge(base_value, override_value)
        elif key in override:
            merged[key] = override_value
        else:
            merged[key] = base_value
    return merged


def resolve_placeholders(data: Any) -> Any:
    """Resolve ${path.to.value} placeholders inside nested configuration."""
    while True:
        flattened = _flatten_mapping(data)
        resolved = _resolve_value(data, flattened)
        if resolved == data:
            return resolved
        data = resolved


def validate_config(config: dict[str, Any], expected_release_name: str) -> None:
    """Validate required structure and supported values."""
    missing_sections = [key for key in REQUIRED_TOP_LEVEL_KEYS if key not in config]
    if missing_sections:
        raise SilverToGoldConfigError(
            f"Missing required top-level sections: {', '.join(missing_sections)}."
        )

    release_name = config["release"].get("release_name")
    if release_name != expected_release_name:
        raise SilverToGoldConfigError(
            f"Resolved release_name '{release_name}' does not match "
            f"requested release_name '{expected_release_name}'."
        )

    processing_mode = config["project"].get("processing_mode")
    if processing_mode not in ALLOWED_PROCESSING_MODES:
        raise SilverToGoldConfigError(
            f"Unsupported processing_mode '{processing_mode}'."
        )

    analysis_date_strategy = str(config["time_controls"].get("analysis_date_strategy", ""))
    if analysis_date_strategy != "MAX_VALID_MATCH_DATE":
        raise SilverToGoldConfigError(
            f"Unsupported analysis_date_strategy '{analysis_date_strategy}'."
        )

    deterministic_seed = config["execution"].get("deterministic_seed")
    if not isinstance(deterministic_seed, int) or deterministic_seed < 0:
        raise SilverToGoldConfigError(
            f"deterministic_seed must be a non-negative integer, got '{deterministic_seed}'."
        )

    if not isinstance(config["gold_tables"], dict) or not config["gold_tables"]:
        raise SilverToGoldConfigError("No Gold tables are configured.")

    _validate_gold_tables(config["gold_tables"])
    _validate_schema_names(config["schemas"])
    _validate_evidence_windows(config["evidence_windows"])
    _validate_model_config(config["models"])
    _validate_eligibility(config["eligibility"])
    _validate_scorecards(config["scorecards"])

    unresolved = list(PLACEHOLDER_PATTERN.finditer(yaml.safe_dump(config)))
    if unresolved:
        raise SilverToGoldConfigError("Unresolved placeholders remain in configuration.")


def _validate_gold_tables(gold_tables: dict[str, Any]) -> None:
    build_orders: set[int] = set()
    for table_name, table_config in gold_tables.items():
        build_order = table_config.get("build_order")
        stage_name = table_config.get("stage")
        target_name = table_config.get("target")
        transform_name = table_config.get("transform")
        primary_key = table_config.get("primary_key")

        if not isinstance(build_order, int):
            raise SilverToGoldConfigError(
                f"Gold table '{table_name}' has invalid build_order '{build_order}'."
            )
        if build_order in build_orders:
            raise SilverToGoldConfigError(
                f"Duplicate build_order '{build_order}' detected."
            )
        if not stage_name:
            raise SilverToGoldConfigError(
                f"Gold table '{table_name}' is missing stage."
            )
        if not target_name:
            raise SilverToGoldConfigError(
                f"Gold table '{table_name}' is missing target."
            )
        if not transform_name:
            raise SilverToGoldConfigError(
                f"Gold table '{table_name}' is missing transform."
            )
        if not isinstance(primary_key, list) or not primary_key:
            raise SilverToGoldConfigError(
                f"Gold table '{table_name}' has invalid primary_key '{primary_key}'."
            )
        build_orders.add(build_order)


def _validate_schema_names(schemas: dict[str, Any]) -> None:
    required_schema_keys = ("silver", "gold", "gold_stage", "operations")
    missing = [key for key in required_schema_keys if not schemas.get(key)]
    if missing:
        raise SilverToGoldConfigError(
            f"Missing required schema values: {', '.join(missing)}."
        )

    resolved_names = [str(schemas[key]) for key in required_schema_keys]
    if len(set(resolved_names)) != len(resolved_names):
        raise SilverToGoldConfigError(
            "Silver, Gold, Gold stage, and operations schemas must all be distinct."
        )


def _validate_evidence_windows(evidence_windows: dict[str, Any]) -> None:
    for key, value in evidence_windows.items():
        if not isinstance(value, int) or value <= 0:
            raise SilverToGoldConfigError(
                f"Evidence window '{key}' must be a positive integer, got '{value}'."
            )


def _validate_model_config(models: dict[str, Any]) -> None:
    enabled = models.get("enabled")
    if not isinstance(enabled, bool):
        raise SilverToGoldConfigError(f"models.enabled must be boolean, got '{enabled}'.")

    train_fraction = models.get("train_fraction")
    validation_fraction = models.get("validation_fraction")
    if not isinstance(train_fraction, (int, float)) or not 0 < float(train_fraction) < 1:
        raise SilverToGoldConfigError(
            f"train_fraction must be between 0 and 1, got '{train_fraction}'."
        )
    if not isinstance(validation_fraction, (int, float)) or not 0 < float(validation_fraction) < 1:
        raise SilverToGoldConfigError(
            f"validation_fraction must be between 0 and 1, got '{validation_fraction}'."
        )

    total_fraction = round(float(train_fraction) + float(validation_fraction), 8)
    if total_fraction != 1.0:
        raise SilverToGoldConfigError(
            f"train_fraction + validation_fraction must equal 1.0, got '{total_fraction}'."
        )


def _validate_eligibility(eligibility: dict[str, Any]) -> None:
    if not eligibility.get("countries"):
        raise SilverToGoldConfigError("eligibility.countries must not be empty.")
    if not eligibility.get("categories"):
        raise SilverToGoldConfigError("eligibility.categories must not be empty.")

    for key in (
        "primary_teams_per_country_category",
        "alternate_teams_per_country_category",
        "watchlist_teams_per_country_category",
    ):
        value = eligibility.get(key)
        if not isinstance(value, int) or value <= 0:
            raise SilverToGoldConfigError(
                f"{key} must be a positive integer, got '{value}'."
            )


def _validate_scorecards(scorecards: dict[str, Any]) -> None:
    for weights_name in ("player_weights", "team_weights", "development_weights"):
        weights = scorecards.get(weights_name)
        if not isinstance(weights, dict) or not weights:
            raise SilverToGoldConfigError(f"{weights_name} must be a non-empty mapping.")

        total = 0.0
        for component_name, raw_value in weights.items():
            if not isinstance(raw_value, (int, float)):
                raise SilverToGoldConfigError(
                    f"{weights_name}.{component_name} must be numeric, got '{raw_value}'."
                )
            if float(raw_value) < 0:
                raise SilverToGoldConfigError(
                    f"{weights_name}.{component_name} must be non-negative, got '{raw_value}'."
                )
            total += float(raw_value)

        if round(total, 8) != 1.0:
            raise SilverToGoldConfigError(
                f"{weights_name} must sum to 1.0, got '{round(total, 8)}'."
            )


def parse_optional_analysis_as_of_date(value: str | None) -> date | None:
    """Parse an optional analysis-as-of date string."""
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    try:
        return date.fromisoformat(trimmed)
    except ValueError as exc:
        raise SilverToGoldConfigError(
            f"Invalid analysis_as_of_date '{value}'. Expected YYYY-MM-DD."
        ) from exc


def _load_yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SilverToGoldConfigError(f"Missing configuration file: {path}")

    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}

    if not isinstance(loaded, dict):
        raise SilverToGoldConfigError(f"Configuration file is not a mapping: {path}")
    return loaded


def _flatten_mapping(data: Any, prefix: str = "") -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            qualified_key = f"{prefix}.{key}" if prefix else key
            flattened[qualified_key] = value
            flattened.update(_flatten_mapping(value, qualified_key))
    return flattened


def _resolve_value(value: Any, flattened: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {key: _resolve_value(item, flattened) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve_value(item, flattened) for item in value]
    if isinstance(value, str):
        matches = list(PLACEHOLDER_PATTERN.finditer(value))
        if not matches:
            return value

        resolved_value = value
        for match in matches:
            placeholder_key = match.group(1)
            if placeholder_key not in flattened:
                raise SilverToGoldConfigError(
                    f"Unknown placeholder '{placeholder_key}'."
                )
            replacement = flattened[placeholder_key]
            resolved_value = resolved_value.replace(match.group(0), str(replacement))
        return _coerce_resolved_scalar(resolved_value)
    return value


def _coerce_resolved_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if re.fullmatch(r"[0-9]+", value):
        return int(value)
    return value

