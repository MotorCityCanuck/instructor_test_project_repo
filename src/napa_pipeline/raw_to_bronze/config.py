"""Configuration loader for the NAPA Raw-to-Bronze pipeline."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Any

import yaml


PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")
ALLOWED_RELEASES = ("napa_5k", "napa_50k", "napa_250k")
REQUIRED_TOP_LEVEL_KEYS = (
    "project",
    "runtime",
    "objects",
    "execution",
    "publication",
    "metadata",
    "performance",
    "release",
    "schemas",
    "volume",
    "sources",
    "logging",
)


class RawToBronzeConfigError(ValueError):
    """Raised when Raw-to-Bronze configuration is invalid."""


@dataclass(frozen=True)
class RawToBronzeConfig:
    """Resolved configuration for a single Raw-to-Bronze release."""

    data: dict[str, Any]
    config_hash: str
    config_root: Path

    @property
    def release_name(self) -> str:
        return str(self.data["release"]["release_name"])

    @property
    def sources_in_build_order(self) -> list[dict[str, Any]]:
        sources = self.data["sources"]
        enabled_sources = [
            {"source_name": source_name, **source_config}
            for source_name, source_config in sources.items()
            if source_config.get("enabled", False)
        ]
        return sorted(enabled_sources, key=lambda item: item["build_order"])


def get_default_config_root() -> Path:
    """Return the repository config directory for Raw-to-Bronze."""
    return Path(__file__).resolve().parents[3] / "config" / "raw_to_bronze"


def load_raw_to_bronze_config(
    release_name: str,
    config_root: Path | str | None = None,
) -> RawToBronzeConfig:
    """Load, merge, resolve, and validate Raw-to-Bronze configuration."""
    if release_name not in ALLOWED_RELEASES:
        raise RawToBronzeConfigError(
            f"Unsupported release_name '{release_name}'. "
            f"Allowed values: {', '.join(ALLOWED_RELEASES)}."
        )

    root = Path(config_root) if config_root else get_default_config_root()
    base_data = _load_yaml_file(root / "base.yml")
    env_data = _load_yaml_file(root / "environments" / f"{release_name}.yml")
    sources_data = _load_yaml_file(root / "raw_sources.yml")
    logging_data = _load_yaml_file(root / "logging.yml")

    merged = deep_merge(base_data, env_data)
    merged = deep_merge(merged, sources_data)
    merged = deep_merge(merged, logging_data)
    merged = resolve_placeholders(merged)
    validate_config(merged, expected_release_name=release_name)

    config_hash = hashlib.sha256(
        yaml.safe_dump(merged, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return RawToBronzeConfig(
        data=merged,
        config_hash=config_hash,
        config_root=root,
    )


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
        raise RawToBronzeConfigError(
            f"Missing required top-level sections: {', '.join(missing_sections)}."
        )

    release_name = config["release"].get("release_name")
    if release_name != expected_release_name:
        raise RawToBronzeConfigError(
            f"Resolved release_name '{release_name}' does not match "
            f"requested release_name '{expected_release_name}'."
        )

    processing_mode = config["project"].get("processing_mode")
    if processing_mode != "full_refresh":
        raise RawToBronzeConfigError(
            f"Unsupported processing_mode '{processing_mode}'."
        )

    volume_name = config["volume"].get("name")
    if volume_name != config["objects"].get("raw_volume_name"):
        raise RawToBronzeConfigError(
            "Resolved volume.name does not match objects.raw_volume_name."
        )

    sources = config["sources"]
    if not isinstance(sources, dict) or not sources:
        raise RawToBronzeConfigError("No Raw sources are configured.")

    build_orders: set[int] = set()
    for source_name, source_config in sources.items():
        bronze_table = source_config.get("bronze_table")
        build_order = source_config.get("build_order")
        file_name = source_config.get("file_name")

        if not bronze_table:
            raise RawToBronzeConfigError(
                f"Source '{source_name}' is missing bronze_table."
            )
        if not isinstance(build_order, int):
            raise RawToBronzeConfigError(
                f"Source '{source_name}' has invalid build_order '{build_order}'."
            )
        if build_order in build_orders:
            raise RawToBronzeConfigError(
                f"Duplicate build_order '{build_order}' detected."
            )
        if not file_name or not str(file_name).endswith(".parquet"):
            raise RawToBronzeConfigError(
                f"Source '{source_name}' has invalid file_name '{file_name}'."
            )

        build_orders.add(build_order)

    unresolved = list(PLACEHOLDER_PATTERN.finditer(yaml.safe_dump(config)))
    if unresolved:
        raise RawToBronzeConfigError("Unresolved placeholders remain in configuration.")


def _load_yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RawToBronzeConfigError(f"Missing configuration file: {path}")

    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}

    if not isinstance(loaded, dict):
        raise RawToBronzeConfigError(f"Configuration file is not a mapping: {path}")
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
                raise RawToBronzeConfigError(
                    f"Unknown placeholder '{placeholder_key}'."
                )
            replacement = flattened[placeholder_key]
            resolved_value = resolved_value.replace(match.group(0), str(replacement))
        return resolved_value
    return value
