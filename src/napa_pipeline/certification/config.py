"""Configuration loader for the Databricks Raw certification pipeline."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Any

import yaml


PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")
RELEASE_ALIAS_TO_NAME = {
    "5k": "napa_5k",
    "50k": "napa_50k",
    "250k": "napa_250k",
    "napa_5k": "napa_5k",
    "napa_50k": "napa_50k",
    "napa_250k": "napa_250k",
}
ALLOWED_RELEASE_ALIASES = tuple(RELEASE_ALIAS_TO_NAME.keys())
ALLOWED_RELEASES = tuple(sorted(set(RELEASE_ALIAS_TO_NAME.values())))
REQUIRED_TOP_LEVEL_KEYS = (
    "project",
    "runtime",
    "objects",
    "execution",
    "calibration",
    "manifest",
    "release",
    "schemas",
    "volumes",
    "artifacts",
    "performance",
    "profiles",
    "sources",
)


class CertificationConfigError(ValueError):
    """Raised when Raw certification configuration is invalid."""


@dataclass(frozen=True)
class CertificationConfig:
    """Resolved configuration for one Raw certification release."""

    data: dict[str, Any]
    config_hash: str
    config_root: Path

    @property
    def release_name(self) -> str:
        return str(self.data["release"]["release_name"])

    @property
    def release_role(self) -> str:
        return str(self.data["release"]["role"])

    @property
    def sources_in_build_order(self) -> list[dict[str, Any]]:
        sources = self.data["sources"]
        enabled_sources = [
            {"source_name": source_name, **source_config}
            for source_name, source_config in sources.items()
            if source_config.get("enabled", False)
        ]
        return sorted(enabled_sources, key=lambda item: item["build_order"])

    @property
    def profile_thresholds(self) -> dict[str, Any]:
        common = self.data["profiles"].get("common", {})
        release_specific = self.data["profiles"].get("release_specific", {})
        return {**common, **release_specific}

    @property
    def threshold_statuses(self) -> dict[str, str]:
        return dict(self.data["calibration"].get("threshold_status", {}))


def get_default_config_root() -> Path:
    """Return the repository config directory for Raw certification."""
    return Path(__file__).resolve().parents[3] / "config" / "certification"


def get_default_sources_config_path() -> Path:
    """Return the repository raw source contract path shared with Raw-to-Bronze."""
    return Path(__file__).resolve().parents[3] / "config" / "raw_to_bronze" / "raw_sources.yml"


def get_default_schema_contract_path() -> Path:
    """Return the certification raw schema and relationship contract path."""
    return Path(__file__).resolve().parents[3] / "config" / "certification" / "raw_schema.yml"


def get_default_calibration_path() -> Path:
    """Return the certification calibration and rollout metadata path."""
    return Path(__file__).resolve().parents[3] / "config" / "certification" / "calibration.yml"


def normalize_release_name(release_name_or_alias: str) -> str:
    """Normalize a certification release alias to the canonical release name."""
    normalized = release_name_or_alias.strip().lower()
    try:
        return RELEASE_ALIAS_TO_NAME[normalized]
    except KeyError as exc:
        allowed = ", ".join(ALLOWED_RELEASE_ALIASES)
        raise CertificationConfigError(
            f"Unsupported release '{release_name_or_alias}'. Allowed values: {allowed}."
        ) from exc


def load_certification_config(
    release_name_or_alias: str,
    config_root: Path | str | None = None,
    sources_config_path: Path | str | None = None,
    schema_contract_path: Path | str | None = None,
    calibration_path: Path | str | None = None,
) -> CertificationConfig:
    """Load, merge, resolve, and validate certification configuration."""
    release_name = normalize_release_name(release_name_or_alias)

    root = Path(config_root) if config_root else get_default_config_root()
    base_data = _load_yaml_file(root / "base.yml")
    env_data = _load_yaml_file(root / "environments" / f"{release_name}.yml")
    sources_path = (
        Path(sources_config_path)
        if sources_config_path
        else get_default_sources_config_path()
    )
    schema_path = (
        Path(schema_contract_path)
        if schema_contract_path
        else get_default_schema_contract_path()
    )
    calibration_metadata_path = (
        Path(calibration_path)
        if calibration_path
        else get_default_calibration_path()
    )
    sources_data = _load_yaml_file(sources_path)
    schema_data = _load_yaml_file(schema_path)
    calibration_data = _load_yaml_file(calibration_metadata_path)

    merged = deep_merge(base_data, env_data)
    merged = deep_merge(merged, sources_data)
    merged = deep_merge(merged, schema_data)
    merged = deep_merge(merged, calibration_data)
    merged = resolve_placeholders(merged)
    validate_config(merged, expected_release_name=release_name)

    config_hash = hashlib.sha256(
        yaml.safe_dump(merged, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return CertificationConfig(
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
        raise CertificationConfigError(
            f"Missing required top-level sections: {', '.join(missing_sections)}."
        )

    release_name = config["release"].get("release_name")
    if release_name != expected_release_name:
        raise CertificationConfigError(
            f"Resolved release_name '{release_name}' does not match "
            f"requested release_name '{expected_release_name}'."
        )

    processing_mode = config["project"].get("processing_mode")
    if processing_mode != "certification":
        raise CertificationConfigError(
            f"Unsupported processing_mode '{processing_mode}'."
        )

    raw_volume = config["volumes"]["raw"]
    artifacts_volume = config["volumes"]["artifacts"]
    if raw_volume.get("name") != config["objects"].get("raw_volume_name"):
        raise CertificationConfigError(
            "Resolved volumes.raw.name does not match objects.raw_volume_name."
        )
    if artifacts_volume.get("name") != config["objects"].get("artifacts_volume_name"):
        raise CertificationConfigError(
            "Resolved volumes.artifacts.name does not match objects.artifacts_volume_name."
        )

    artifacts_root = str(config["artifacts"].get("root_path", "")).strip()
    if not artifacts_root:
        raise CertificationConfigError("artifacts.root_path must be configured.")

    sources = config["sources"]
    if not isinstance(sources, dict) or not sources:
        raise CertificationConfigError("No Raw sources are configured.")

    build_orders: set[int] = set()
    for source_name, source_config in sources.items():
        build_order = source_config.get("build_order")
        file_name = source_config.get("file_name")
        key_columns = source_config.get("key_columns")

        if not isinstance(build_order, int):
            raise CertificationConfigError(
                f"Source '{source_name}' has invalid build_order '{build_order}'."
            )
        if build_order in build_orders:
            raise CertificationConfigError(
                f"Duplicate build_order '{build_order}' detected."
            )
        if not file_name or not str(file_name).endswith(".parquet"):
            raise CertificationConfigError(
                f"Source '{source_name}' has invalid file_name '{file_name}'."
            )
        if not isinstance(key_columns, list) or not key_columns:
            raise CertificationConfigError(
                f"Source '{source_name}' must define non-empty key_columns."
            )
        build_orders.add(build_order)

    schema_contract = config.get("schema_contract")
    if not isinstance(schema_contract, dict) or not schema_contract:
        raise CertificationConfigError("schema_contract must be configured.")
    relationships = config.get("relationships")
    if not isinstance(relationships, list):
        raise CertificationConfigError("relationships must be configured as a list.")

    profiles = config.get("profiles")
    if not isinstance(profiles, dict):
        raise CertificationConfigError("profiles must be configured.")
    if not isinstance(profiles.get("common"), dict):
        raise CertificationConfigError("profiles.common must be configured.")
    if not isinstance(profiles.get("release_specific"), dict):
        raise CertificationConfigError("profiles.release_specific must be configured.")

    calibration = config.get("calibration")
    if not isinstance(calibration, dict):
        raise CertificationConfigError("calibration must be configured.")

    threshold_status = calibration.get("threshold_status")
    if not isinstance(threshold_status, dict):
        raise CertificationConfigError("calibration.threshold_status must be configured.")
    allowed_statuses = {"approved", "provisional", "observational"}
    missing_threshold_statuses = [
        threshold_key
        for threshold_key in config["profiles"].get("common", {}).keys()
        | config["profiles"].get("release_specific", {}).keys()
        if threshold_key not in threshold_status
    ]
    if missing_threshold_statuses:
        raise CertificationConfigError(
            "Missing calibration threshold statuses for: "
            + ", ".join(sorted(missing_threshold_statuses))
            + "."
        )
    invalid_statuses = {
        key: value
        for key, value in threshold_status.items()
        if value not in allowed_statuses
    }
    if invalid_statuses:
        raise CertificationConfigError(
            "Invalid calibration threshold statuses: "
            + ", ".join(f"{key}={value}" for key, value in sorted(invalid_statuses.items()))
            + "."
        )

    required_cases = calibration.get("required_cases")
    if not isinstance(required_cases, list) or not required_cases:
        raise CertificationConfigError("calibration.required_cases must be a non-empty list.")

    approved_baselines = calibration.get("approved_baselines")
    if not isinstance(approved_baselines, dict):
        raise CertificationConfigError("calibration.approved_baselines must be configured.")

    accepted_exceptions = calibration.get("accepted_exceptions")
    if not isinstance(accepted_exceptions, list):
        raise CertificationConfigError("calibration.accepted_exceptions must be configured as a list.")

    production_release_procedure = calibration.get("production_release_procedure")
    if not isinstance(production_release_procedure, list) or not production_release_procedure:
        raise CertificationConfigError(
            "calibration.production_release_procedure must be a non-empty list."
        )

    rollback_and_recertification_procedure = calibration.get("rollback_and_recertification_procedure")
    if not isinstance(rollback_and_recertification_procedure, list) or not rollback_and_recertification_procedure:
        raise CertificationConfigError(
            "calibration.rollback_and_recertification_procedure must be a non-empty list."
        )

    unresolved = list(PLACEHOLDER_PATTERN.finditer(yaml.safe_dump(config)))
    if unresolved:
        raise CertificationConfigError("Unresolved placeholders remain in configuration.")


def _load_yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise CertificationConfigError(f"Missing configuration file: {path}")

    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}

    if not isinstance(loaded, dict):
        raise CertificationConfigError(f"Configuration file is not a mapping: {path}")
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
                raise CertificationConfigError(
                    f"Unknown placeholder '{placeholder_key}'."
                )
            replacement = flattened[placeholder_key]
            resolved_value = resolved_value.replace(match.group(0), str(replacement))
        return resolved_value
    return value
