"""Result models for Raw certification source loading and inventory checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class SourceContract:
    """Expected contract for one Raw source domain."""

    source_name: str
    file_name: str
    key_columns: tuple[str, ...]
    build_order: int


@dataclass(frozen=True)
class DiscoveredFileRecord:
    """Observed file metadata in the Raw release path."""

    file_name: str
    file_path: str
    file_size: int | None
    modification_ts: datetime | None


@dataclass(frozen=True)
class ManifestRecord:
    """Parsed release manifest metadata when present."""

    file_name: str
    file_path: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class SourceLoadResult:
    """Observed metrics and schema metadata for one Raw source."""

    source_name: str
    file_name: str
    file_path: str
    file_size: int | None
    modification_ts: datetime | None
    row_count: int | None
    schema_hash: str | None
    schema_fields: tuple[dict[str, Any], ...]
    temp_view_name: str | None
    read_status: str
    error_message: str | None = None


@dataclass(frozen=True)
class CertificationRuleResult:
    """Outcome of one certification rule."""

    rule_id: str
    name: str
    pillar: str
    category: str
    status: str
    severity: str
    message: str
    affected_count: int = 0
    observed_value: Any | None = None
    expected_value: Any | None = None
    sample_records: tuple[str, ...] = ()


@dataclass(frozen=True)
class InventoryCertificationResult:
    """Combined source-loader and inventory-rule output for one release."""

    release_name: str
    release_path: str
    source_mode: str
    path_exists: bool
    expected_sources: tuple[SourceContract, ...]
    discovered_files: tuple[DiscoveredFileRecord, ...]
    loaded_sources: tuple[SourceLoadResult, ...]
    manifest: ManifestRecord | None
    rule_results: tuple[CertificationRuleResult, ...]

