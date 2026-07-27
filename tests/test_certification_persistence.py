from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from napa_pipeline.certification.assessment import attach_artifacts, build_certification_assessment
from napa_pipeline.certification.config import CertificationConfig
from napa_pipeline.certification.environment import ReleaseEnvironment
from napa_pipeline.certification.models import (
    CertificationArtifact,
    CertificationRuleResult,
    InventoryCertificationResult,
)
from napa_pipeline.certification.persistence import (
    ARTIFACTS_TABLE,
    FINDINGS_TABLE,
    METRICS_TABLE,
    RULE_RUNS_TABLE,
    RUNS_TABLE,
    build_artifact_records,
    build_finding_records,
    build_metric_records,
    build_rule_run_records,
    build_run_record,
    get_persistence_table_ddls,
)


def test_persistence_table_ddls_include_phase7_tables() -> None:
    ddls = "\n".join(get_persistence_table_ddls("workspace.instructor_ops"))

    assert RUNS_TABLE in ddls
    assert RULE_RUNS_TABLE in ddls
    assert METRICS_TABLE in ddls
    assert FINDINGS_TABLE in ddls
    assert ARTIFACTS_TABLE in ddls


def test_build_persistence_records_from_assessment() -> None:
    assessment = _build_assessment()
    assessment = attach_artifacts(
        assessment,
        [
            CertificationArtifact(
                artifact_type="json_snapshot",
                artifact_path="/tmp/certification.json",
                checksum="abc",
                created_at=assessment.completed_at,
            )
        ],
    )

    run_record = build_run_record(assessment)
    rule_records = build_rule_run_records(assessment)
    metric_records = build_metric_records(assessment)
    finding_records = build_finding_records(assessment)
    artifact_records = build_artifact_records(assessment)

    assert run_record["certification_run_id"] == "run-persist"
    assert run_record["certification_decision"] == "CERTIFIED_WITH_WARNINGS"
    assert len(rule_records) == 2
    assert len(metric_records) >= 3
    assert len(finding_records) == 1
    assert finding_records[0]["sample_records_json"] == '["source snapshot missing"]'
    assert artifact_records[0]["artifact_type"] == "json_snapshot"


def _build_assessment():
    return build_certification_assessment(
        certification_run_id="run-persist",
        config=CertificationConfig(
            data={
                "project": {
                    "pipeline_name": "raw_certification",
                    "pipeline_version": "1.0.0",
                    "processing_mode": "certification",
                },
                "release": {"release_name": "napa_50k", "role": "validation"},
            },
            config_hash="config-hash",
            config_root=Path("."),
        ),
        environment=ReleaseEnvironment(
            catalog="workspace",
            raw_schema="instructor_50k_raw",
            operations_schema="instructor_ops",
            raw_volume_name="napa_files",
            raw_volume_path="/Volumes/workspace/instructor_50k_raw/napa_files",
            artifacts_volume_name="certification_artifacts",
            artifacts_volume_path="/Volumes/workspace/instructor_ops/certification_artifacts",
            artifacts_root_path="/Volumes/workspace/instructor_ops/certification_artifacts/raw_certification/napa_50k",
        ),
        inventory_result=InventoryCertificationResult(
            release_name="napa_50k",
            release_path="/Volumes/workspace/instructor_50k_raw/napa_files/napa_50k",
            source_mode="parquet",
            path_exists=True,
            expected_sources=(),
            discovered_files=(),
            loaded_sources=(),
            manifest=None,
            rule_results=(),
        ),
        rule_results=[
            CertificationRuleResult(
                rule_id="RAW_PATH_EXISTS",
                name="Raw path exists",
                pillar="Release Inventory and Format",
                category="inventory",
                status="PASS",
                severity="info",
                message="Raw path is available.",
            ),
            CertificationRuleResult(
                rule_id="RAW_SOURCE_SNAPSHOT_MISSING",
                name="Source snapshot missing",
                pillar="Source Reconciliation and Regression",
                category="reconciliation",
                status="WARN",
                severity="warning",
                message="Source snapshot missing",
                sample_records=("source snapshot missing",),
            ),
        ],
        started_at=datetime(2026, 7, 27, 13, 0, 0, tzinfo=timezone.utc),
        completed_at=datetime(2026, 7, 27, 13, 1, 0, tzinfo=timezone.utc),
        source_snapshot_path=None,
    )
