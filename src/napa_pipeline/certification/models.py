"""Result and assessment models for the Raw certification pipeline."""

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
    expected_min: float | None = None
    expected_max: float | None = None
    numerator: float | None = None
    denominator: float | None = None
    unit: str | None = None
    business_impact: str | None = None
    recommended_action: str | None = None
    execution_mode: str | None = None
    remediation_owner: str | None = None


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


@dataclass(frozen=True)
class CertificationMetric:
    """Persisted or rendered metric derived from certification results."""

    rule_id: str
    metric_name: str
    metric_value: float | None
    metric_text: str | None = None
    dimension_json: str | None = None
    expected_min: float | None = None
    expected_max: float | None = None
    unit: str | None = None


@dataclass(frozen=True)
class CertificationFinding:
    """Persisted or rendered finding derived from a rule result."""

    finding_id: str
    rule_id: str
    pillar: str
    category: str
    severity: str
    title: str
    message: str
    business_impact: str
    recommended_action: str
    affected_count: int
    sample_records: tuple[str, ...] = ()
    accepted_exception: bool = False
    exception_reason: str | None = None


@dataclass(frozen=True)
class CertificationArtifact:
    """Published certification artifact."""

    artifact_type: str
    artifact_path: str
    checksum: str
    created_at: datetime


@dataclass(frozen=True)
class CertificationPillarScore:
    """Scored certification pillar."""

    pillar: str
    weight: float
    applicable_rule_count: int
    passed_rule_count: int
    warning_rule_count: int
    failed_rule_count: int
    score: float


@dataclass(frozen=True)
class CertificationAssessment:
    """Run-level certification assessment and durable evidence bundle."""

    certification_run_id: str
    release_name: str
    release_role: str
    intended_use: str
    source_mode: str
    raw_path: str
    analysis_as_of_date: str | None
    started_at: datetime
    completed_at: datetime
    status: str
    certification_decision: str
    overall_score: float
    pillar_scores: tuple[CertificationPillarScore, ...]
    severity_counts: dict[str, int]
    status_counts: dict[str, int]
    rule_results: tuple[CertificationRuleResult, ...]
    findings: tuple[CertificationFinding, ...]
    metrics: tuple[CertificationMetric, ...]
    artifacts: tuple[CertificationArtifact, ...] = ()
    hard_gate_rule_ids: tuple[str, ...] = ()
    release_blocking_rule_ids: tuple[str, ...] = ()
    source_snapshot_path: str | None = None
    baseline_id: str | None = None
    code_version: str | None = None
    git_commit: str | None = None
    config_snapshot_json: str | None = None
    error_message: str | None = None
