from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from napa_pipeline.certification.assessment import (
    build_assessment_snapshot,
    build_certification_assessment,
)
from napa_pipeline.certification.config import CertificationConfig
from napa_pipeline.certification.environment import ReleaseEnvironment
from napa_pipeline.certification.models import (
    CertificationRuleResult,
    InventoryCertificationResult,
)


def test_build_certification_assessment_certified() -> None:
    assessment = build_certification_assessment(
        certification_run_id="run-1",
        config=_build_config("napa_50k", "validation"),
        environment=_build_environment(),
        inventory_result=_build_inventory_result(),
        rule_results=[
            _build_rule(
                rule_id="RAW_PATH_EXISTS",
                pillar="Release Inventory and Format",
                category="inventory",
                status="PASS",
                severity="info",
            ),
            _build_rule(
                rule_id="RAW_MATCH_WINNER_INTEGRITY",
                pillar="Schema and Structural Integrity",
                category="matches",
                status="PASS",
                severity="info",
            ),
        ],
        started_at=_utc(2026, 7, 27, 10, 0, 0),
        completed_at=_utc(2026, 7, 27, 10, 5, 0),
        analysis_as_of_date="2026-06-30",
        git_commit="abc123",
    )

    assert assessment.certification_decision == "CERTIFIED"
    assert assessment.intended_use == "CERTIFIED_FOR_ENGINEERING_VALIDATION"
    assert assessment.overall_score == 100.0
    snapshot = build_assessment_snapshot(assessment)
    assert snapshot["decision"] == "CERTIFIED"
    assert snapshot["git_commit"] == "abc123"


def test_build_certification_assessment_with_warnings() -> None:
    assessment = build_certification_assessment(
        certification_run_id="run-2",
        config=_build_config("napa_5k", "development"),
        environment=_build_environment(),
        inventory_result=_build_inventory_result(),
        rule_results=[
            _build_rule(
                rule_id="RAW_SOURCE_SNAPSHOT_MISSING",
                pillar="Source Reconciliation and Regression",
                category="reconciliation",
                status="WARN",
                severity="warning",
            )
        ],
        started_at=_utc(2026, 7, 27, 10, 0, 0),
        completed_at=_utc(2026, 7, 27, 10, 1, 0),
    )

    assert assessment.certification_decision == "CERTIFIED_WITH_WARNINGS"
    assert assessment.severity_counts["warning"] == 1
    assert len(assessment.findings) == 1


def test_build_certification_assessment_rejected_when_hard_gate_fails_even_with_high_score() -> None:
    assessment = build_certification_assessment(
        certification_run_id="run-3",
        config=_build_config("napa_250k", "production"),
        environment=_build_environment(),
        inventory_result=_build_inventory_result(),
        rule_results=[
            _build_rule(
                rule_id="RAW_PATH_EXISTS",
                pillar="Release Inventory and Format",
                category="inventory",
                status="PASS",
                severity="info",
            ),
            _build_rule(
                rule_id="RAW_TEAM_SELECTION_PROBE_VIABLE",
                pillar="Assignment Pathway Readiness",
                category="assignment",
                status="FAIL",
                severity="blocker",
                message="Candidate depth is below threshold.",
                affected_count=1,
            ),
        ],
        started_at=_utc(2026, 7, 27, 10, 0, 0),
        completed_at=_utc(2026, 7, 27, 10, 2, 0),
    )

    assert assessment.certification_decision == "REJECTED"
    assert assessment.overall_score > 0.0
    assert "RAW_TEAM_SELECTION_PROBE_VIABLE" in assessment.hard_gate_rule_ids


def test_build_certification_assessment_execution_failed() -> None:
    assessment = build_certification_assessment(
        certification_run_id="run-4",
        config=_build_config("napa_250k", "production"),
        environment=_build_environment(),
        inventory_result=_build_inventory_result(),
        rule_results=[
            _build_rule(
                rule_id="RAW_PATH_EXISTS",
                pillar="Release Inventory and Format",
                category="inventory",
                status="PASS",
                severity="info",
            )
        ],
        started_at=_utc(2026, 7, 27, 10, 0, 0),
        completed_at=_utc(2026, 7, 27, 10, 3, 0),
        execution_error="Spark execution failed before a reliable conclusion could be reached.",
    )

    assert assessment.certification_decision == "EXECUTION_FAILED"
    assert assessment.status == "EXECUTION_FAILED"
    assert assessment.error_message is not None
    assert json.loads(assessment.config_snapshot_json or "{}")["project"]["pipeline_name"] == "raw_certification"


def _build_rule(
    *,
    rule_id: str,
    pillar: str,
    category: str,
    status: str,
    severity: str,
    message: str | None = None,
    affected_count: int = 0,
) -> CertificationRuleResult:
    return CertificationRuleResult(
        rule_id=rule_id,
        name=rule_id,
        pillar=pillar,
        category=category,
        status=status,
        severity=severity,
        message=message or f"{rule_id} {status.lower()}",
        affected_count=affected_count,
    )


def _build_inventory_result() -> InventoryCertificationResult:
    return InventoryCertificationResult(
        release_name="napa_50k",
        release_path="/Volumes/workspace/instructor_50k_raw/napa_files/napa_50k",
        source_mode="parquet",
        path_exists=True,
        expected_sources=(),
        discovered_files=(),
        loaded_sources=(),
        manifest=None,
        rule_results=(),
    )


def _build_config(release_name: str, release_role: str) -> CertificationConfig:
    return CertificationConfig(
        data={
            "project": {
                "pipeline_name": "raw_certification",
                "pipeline_version": "1.0.0",
                "processing_mode": "certification",
            },
            "release": {
                "release_name": release_name,
                "role": release_role,
            },
        },
        config_hash="config-hash",
        config_root=Path("."),
    )


def _build_environment() -> ReleaseEnvironment:
    return ReleaseEnvironment(
        catalog="workspace",
        raw_schema="instructor_50k_raw",
        operations_schema="instructor_ops",
        raw_volume_name="napa_files",
        raw_volume_path="/Volumes/workspace/instructor_50k_raw/napa_files",
        artifacts_volume_name="certification_artifacts",
        artifacts_volume_path="/Volumes/workspace/instructor_ops/certification_artifacts",
        artifacts_root_path="/Volumes/workspace/instructor_ops/certification_artifacts/raw_certification/napa_50k",
    )


def _utc(year: int, month: int, day: int, hour: int, minute: int, second: int) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
