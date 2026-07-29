from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from napa_pipeline.certification.assessment import build_certification_assessment
from napa_pipeline.certification.config import CertificationConfig
from napa_pipeline.certification.environment import ReleaseEnvironment
from napa_pipeline.certification.models import (
    CertificationRuleResult,
    InventoryCertificationResult,
)
from napa_pipeline.certification.reporting import publish_artifacts, render_markdown_report


def test_render_markdown_report_is_deterministic(tmp_path: Path) -> None:
    assessment = _build_assessment()

    first = render_markdown_report(assessment, snapshot_path="snapshot.json", findings_path="findings.csv")
    second = render_markdown_report(assessment, snapshot_path="snapshot.json", findings_path="findings.csv")

    assert first == second
    assert "## 5. Release-Blocking Findings" in first
    assert "## 14. Detailed Rule Results" in first


def test_publish_artifacts_writes_snapshot_report_and_csv(tmp_path: Path) -> None:
    assessment = _build_assessment()
    bundle = publish_artifacts(assessment, str(tmp_path))

    assert Path(bundle.snapshot_path).exists()
    assert Path(bundle.report_path).exists()
    assert Path(bundle.findings_path).exists()
    assert "20260727T120200Z" in bundle.snapshot_path.replace("\\", "/")
    assert "/run-report/" in bundle.snapshot_path.replace("\\", "/")
    assert len(bundle.artifacts) == 3
    assert '"decision": "REJECTED"' in bundle.snapshot_json
    assert "RAW_TEAM_SELECTION_PROBE_VIABLE" in bundle.report_markdown
    assert "finding_id,rule_id,pillar,category,severity" in bundle.findings_csv


def _build_assessment():
    return build_certification_assessment(
        certification_run_id="run-report",
        config=CertificationConfig(
            data={
                "project": {
                    "pipeline_name": "raw_certification",
                    "pipeline_version": "1.0.0",
                    "processing_mode": "certification",
                },
                "release": {"release_name": "napa_250k", "role": "production"},
            },
            config_hash="config-hash",
            config_root=Path("."),
        ),
        environment=ReleaseEnvironment(
            catalog="workspace",
            raw_schema="instructor_250k_raw",
            operations_schema="instructor_ops",
            raw_volume_name="napa_files",
            raw_volume_path="/Volumes/workspace/instructor_250k_raw/napa_files",
            artifacts_volume_name="certification_artifacts",
            artifacts_volume_path="/Volumes/workspace/instructor_ops/certification_artifacts",
            artifacts_root_path="/Volumes/workspace/instructor_ops/certification_artifacts/raw_certification/napa_250k",
        ),
        inventory_result=InventoryCertificationResult(
            release_name="napa_250k",
            release_path="/Volumes/workspace/instructor_250k_raw/napa_files/napa_250k",
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
                rule_id="RAW_TEAM_SELECTION_PROBE_VIABLE",
                name="Olympic team-selection probe viable",
                pillar="Assignment Pathway Readiness",
                category="assignment",
                status="FAIL",
                severity="blocker",
                message="Candidate depth is below the minimum viable threshold.",
                affected_count=4,
                sample_records=("USA/mens_doubles", "CAN/mixed_doubles"),
            ),
        ],
        started_at=datetime(2026, 7, 27, 12, 0, 0, tzinfo=timezone.utc),
        completed_at=datetime(2026, 7, 27, 12, 2, 0, tzinfo=timezone.utc),
        analysis_as_of_date="2026-06-30",
        git_commit="deadbeef",
    )
