"""Configuration loader for the NAPA Bronze-to-Silver pipeline."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Any

import yaml


PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")
ALLOWED_RELEASES = ("napa_5k", "napa_50k", "napa_250k")
ALLOWED_PROCESSING_MODES = ("full_refresh",)
ALLOWED_RULE_TYPES = ("not_null", "unique", "allowed_values", "foreign_key", "date_not_after")
ALLOWED_STAGES = ("reference", "athlete", "organization", "partnership", "competition")
ALLOWED_TRANSFORMS = (
    "build_monthly_batches",
    "build_regions",
    "build_players",
    "build_clubs",
    "build_teams",
    "build_player_registrations",
    "build_player_assessment_history",
    "build_club_memberships",
    "build_team_memberships",
    "build_matches",
    "build_match_teams",
    "build_match_team_players",
    "build_match_games",
)
REQUIRED_TOP_LEVEL_KEYS = (
    "project",
    "runtime",
    "objects",
    "execution",
    "publication",
    "metadata",
    "thresholds",
    "paths",
    "release",
    "schemas",
    "performance",
    "sources",
    "silver_tables",
    "domains",
    "quality_rules",
    "logging",
)


class BronzeToSilverConfigError(ValueError):
    """Raised when Bronze-to-Silver configuration is invalid."""


@dataclass(frozen=True)
class BronzeToSilverConfig:
    """Resolved configuration for a single Bronze-to-Silver release."""

    data: dict[str, Any]
    config_hash: str
    config_root: Path

    @property
    def release_name(self) -> str:
        return str(self.data["release"]["release_name"])

    @property
    def enabled_sources(self) -> dict[str, dict[str, Any]]:
        return {
            name: source
            for name, source in self.data["sources"].items()
            if source.get("enabled", False)
        }

    @property
    def silver_tables_in_build_order(self) -> list[dict[str, Any]]:
        enabled_tables = [
            {"table_name": name, **table}
            for name, table in self.data["silver_tables"].items()
            if table.get("enabled", False)
        ]
        return sorted(enabled_tables, key=lambda item: item["build_order"])


def get_default_config_root() -> Path:
    """Return the repository config directory for Bronze-to-Silver."""
    return Path(__file__).resolve().parents[3] / "config" / "bronze_to_silver"


def load_bronze_to_silver_config(
    release_name: str,
    config_root: Path | str | None = None,
) -> BronzeToSilverConfig:
    """Load, merge, resolve, and validate Bronze-to-Silver configuration."""
    if release_name not in ALLOWED_RELEASES:
        raise BronzeToSilverConfigError(
            f"Unsupported release_name '{release_name}'. "
            f"Allowed values: {', '.join(ALLOWED_RELEASES)}."
        )

    root = Path(config_root) if config_root else get_default_config_root()
    base_data = _load_yaml_file(root / "base.yml")
    env_data = _load_yaml_file(root / "environments" / f"{release_name}.yml")
    sources_data = _load_yaml_file(root / "sources.yml")
    silver_tables_data = _load_yaml_file(root / "silver_tables.yml")
    domains_data = _load_yaml_file(root / "domains.yml")
    quality_rules_data = _load_yaml_file(root / "quality_rules.yml")
    logging_data = _load_yaml_file(root / "logging.yml")

    merged = deep_merge(base_data, env_data)
    merged = deep_merge(merged, sources_data)
    merged = deep_merge(merged, silver_tables_data)
    merged = deep_merge(merged, domains_data)
    merged = deep_merge(merged, quality_rules_data)
    merged = deep_merge(merged, logging_data)
    merged = resolve_placeholders(merged)
    validate_config(merged, expected_release_name=release_name)

    config_hash = hashlib.sha256(
        yaml.safe_dump(merged, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return BronzeToSilverConfig(data=merged, config_hash=config_hash, config_root=root)


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
        raise BronzeToSilverConfigError(
            f"Missing required top-level sections: {', '.join(missing_sections)}."
        )

    release_name = config["release"].get("release_name")
    if release_name != expected_release_name:
        raise BronzeToSilverConfigError(
            f"Resolved release_name '{release_name}' does not match "
            f"requested release_name '{expected_release_name}'."
        )

    processing_mode = config["project"].get("processing_mode")
    if processing_mode not in ALLOWED_PROCESSING_MODES:
        raise BronzeToSilverConfigError(
            f"Unsupported processing_mode '{processing_mode}'."
        )

    if not isinstance(config["sources"], dict) or not config["sources"]:
        raise BronzeToSilverConfigError("No Bronze sources are configured.")
    if not isinstance(config["silver_tables"], dict) or not config["silver_tables"]:
        raise BronzeToSilverConfigError("No Silver tables are configured.")

    _validate_sources(config["sources"])
    _validate_silver_tables(config["silver_tables"], config["sources"])
    _validate_quality_rules(config["quality_rules"], config["silver_tables"])

    unresolved = list(PLACEHOLDER_PATTERN.finditer(yaml.safe_dump(config)))
    if unresolved:
        raise BronzeToSilverConfigError("Unresolved placeholders remain in configuration.")


def _validate_sources(sources: dict[str, Any]) -> None:
    for source_name, source_config in sources.items():
        bronze_table = source_config.get("bronze_table")
        source_file = source_config.get("source_file")
        natural_key = source_config.get("natural_key")
        if not bronze_table:
            raise BronzeToSilverConfigError(
                f"Source '{source_name}' is missing bronze_table."
            )
        if not source_file or not str(source_file).endswith(".parquet"):
            raise BronzeToSilverConfigError(
                f"Source '{source_name}' has invalid source_file '{source_file}'."
            )
        if not isinstance(natural_key, list) or not natural_key:
            raise BronzeToSilverConfigError(
                f"Source '{source_name}' has invalid natural_key '{natural_key}'."
            )


def _validate_silver_tables(
    silver_tables: dict[str, Any],
    sources: dict[str, Any],
) -> None:
    build_orders: set[int] = set()
    for table_name, table_config in silver_tables.items():
        source_name = table_config.get("source")
        build_order = table_config.get("build_order")
        transform_name = table_config.get("transform")
        stage_name = table_config.get("stage")
        primary_key = table_config.get("primary_key")

        if source_name not in sources:
            raise BronzeToSilverConfigError(
                f"Silver table '{table_name}' references undefined source '{source_name}'."
            )
        if not isinstance(build_order, int):
            raise BronzeToSilverConfigError(
                f"Silver table '{table_name}' has invalid build_order '{build_order}'."
            )
        if build_order in build_orders:
            raise BronzeToSilverConfigError(
                f"Duplicate build_order '{build_order}' detected."
            )
        if transform_name not in ALLOWED_TRANSFORMS:
            raise BronzeToSilverConfigError(
                f"Silver table '{table_name}' references undefined transform '{transform_name}'."
            )
        if stage_name not in ALLOWED_STAGES:
            raise BronzeToSilverConfigError(
                f"Silver table '{table_name}' has unsupported stage '{stage_name}'."
            )
        if not isinstance(primary_key, list) or not primary_key:
            raise BronzeToSilverConfigError(
                f"Silver table '{table_name}' has invalid primary_key '{primary_key}'."
            )

        build_orders.add(build_order)


def _validate_quality_rules(
    quality_rules: dict[str, Any],
    silver_tables: dict[str, Any],
) -> None:
    for table_name, rules in quality_rules.items():
        if table_name not in silver_tables:
            raise BronzeToSilverConfigError(
                f"Quality rules reference undefined Silver table '{table_name}'."
            )
        if not isinstance(rules, list):
            raise BronzeToSilverConfigError(
                f"Quality rules for '{table_name}' must be a list."
            )
        for rule in rules:
            rule_type = rule.get("type")
            if rule_type not in ALLOWED_RULE_TYPES:
                raise BronzeToSilverConfigError(
                    f"Quality rule '{rule.get('id')}' has unsupported type '{rule_type}'."
                )


def _load_yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise BronzeToSilverConfigError(f"Missing configuration file: {path}")

    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}

    if not isinstance(loaded, dict):
        raise BronzeToSilverConfigError(f"Configuration file is not a mapping: {path}")
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
                raise BronzeToSilverConfigError(
                    f"Unknown placeholder '{placeholder_key}'."
                )
            replacement = flattened[placeholder_key]
            resolved_value = resolved_value.replace(match.group(0), str(replacement))
        return resolved_value
    return value
