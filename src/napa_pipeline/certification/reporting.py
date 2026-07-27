"""Snapshot, report, and finding artifact generation for Raw certification."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
import hashlib
import io
import json
from pathlib import Path
from typing import Any

from napa_pipeline.certification.assessment import build_assessment_snapshot
from napa_pipeline.certification.models import (
    CertificationArtifact,
    CertificationAssessment,
    CertificationFinding,
)


@dataclass(frozen=True)
class CertificationArtifactBundle:
    """Published certification artifacts and their rendered content."""

    snapshot_path: str
    report_path: str
    findings_path: str
    snapshot_json: str
    report_markdown: str
    findings_csv: str
    artifacts: tuple[CertificationArtifact, ...]


def publish_artifacts(
    assessment: CertificationAssessment,
    artifacts_root_path: str,
    *,
    dbutils: Any | None = None,
) -> CertificationArtifactBundle:
    """Render and write the certification JSON, Markdown, and CSV artifacts."""
    run_root = _join_artifact_path(artifacts_root_path, assessment.certification_run_id)
    snapshot_path = _join_artifact_path(run_root, "certification.json")
    report_path = _join_artifact_path(run_root, "certification_report.md")
    findings_path = _join_artifact_path(run_root, "findings.csv")

    placeholder_artifacts = (
        CertificationArtifact(
            artifact_type="json_snapshot",
            artifact_path=snapshot_path,
            checksum="",
            created_at=assessment.completed_at,
        ),
        CertificationArtifact(
            artifact_type="markdown_report",
            artifact_path=report_path,
            checksum="",
            created_at=assessment.completed_at,
        ),
        CertificationArtifact(
            artifact_type="findings_csv",
            artifact_path=findings_path,
            checksum="",
            created_at=assessment.completed_at,
        ),
    )
    snapshot_json = json.dumps(
        build_assessment_snapshot(
            assessment.__class__(**{**assessment.__dict__, "artifacts": placeholder_artifacts})
        ),
        indent=2,
        sort_keys=True,
    )
    report_markdown = render_markdown_report(assessment, snapshot_path=snapshot_path, findings_path=findings_path)
    findings_csv = render_findings_csv(assessment.findings)

    _write_text(snapshot_path, snapshot_json, dbutils=dbutils)
    _write_text(report_path, report_markdown, dbutils=dbutils)
    _write_text(findings_path, findings_csv, dbutils=dbutils)

    artifacts = (
        CertificationArtifact(
            artifact_type="json_snapshot",
            artifact_path=snapshot_path,
            checksum=_checksum(snapshot_json),
            created_at=assessment.completed_at,
        ),
        CertificationArtifact(
            artifact_type="markdown_report",
            artifact_path=report_path,
            checksum=_checksum(report_markdown),
            created_at=assessment.completed_at,
        ),
        CertificationArtifact(
            artifact_type="findings_csv",
            artifact_path=findings_path,
            checksum=_checksum(findings_csv),
            created_at=assessment.completed_at,
        ),
    )
    return CertificationArtifactBundle(
        snapshot_path=snapshot_path,
        report_path=report_path,
        findings_path=findings_path,
        snapshot_json=snapshot_json,
        report_markdown=report_markdown,
        findings_csv=findings_csv,
        artifacts=artifacts,
    )


def render_markdown_report(
    assessment: CertificationAssessment,
    *,
    snapshot_path: str | None = None,
    findings_path: str | None = None,
) -> str:
    """Render the deterministic Markdown certification report."""
    blocking_findings = [
        finding for finding in _sorted_findings(assessment.findings) if finding.severity in {"blocker", "error"}
    ]
    warning_findings = [
        finding for finding in _sorted_findings(assessment.findings) if finding.severity == "warning"
    ]
    pathway_findings = [
        finding
        for finding in _sorted_findings(assessment.findings)
        if finding.pillar == "Assignment Pathway Readiness"
    ]
    population_findings = [
        finding
        for finding in _sorted_findings(assessment.findings)
        if finding.pillar in {"Population and Lifecycle Fitness", "Team and Partnership Fitness"}
    ]
    structural_findings = [
        finding
        for finding in _sorted_findings(assessment.findings)
        if finding.pillar in {"Schema and Structural Integrity", "Competition and Evidence Fitness"}
    ]
    reconciliation_findings = [
        finding
        for finding in _sorted_findings(assessment.findings)
        if finding.pillar == "Source Reconciliation and Regression"
    ]

    lines = [
        "# NAPA Raw Student Data Certification",
        "",
        "## 1. Certification Decision",
        f"- Decision: `{assessment.certification_decision}`",
        f"- Run status: `{assessment.status}`",
        f"- Intended use: `{assessment.intended_use}`",
        f"- Overall score: `{assessment.overall_score}`",
        "",
        "## 2. Release Identity",
        f"- Release: `{assessment.release_name}`",
        f"- Release role: `{assessment.release_role}`",
        f"- Source mode: `{assessment.source_mode}`",
        f"- Raw path: `{assessment.raw_path}`",
        f"- Analysis as-of date: `{assessment.analysis_as_of_date or 'not supplied'}`",
        "",
        "## 3. Executive Summary",
        f"- Hard-gate triggers: {len(assessment.hard_gate_rule_ids)}",
        f"- Release-blocking findings: {len(blocking_findings)}",
        f"- Warning findings: {len(warning_findings)}",
        f"- Rule status counts: {json.dumps(assessment.status_counts, sort_keys=True)}",
        "",
        "## 4. Pillar Scorecard",
    ]
    for pillar_score in assessment.pillar_scores:
        lines.append(
            f"- {pillar_score.pillar}: score={pillar_score.score}/{pillar_score.weight}, "
            f"pass={pillar_score.passed_rule_count}, warn={pillar_score.warning_rule_count}, "
            f"fail={pillar_score.failed_rule_count}"
        )

    lines.extend(
        [
            "",
            "## 5. Release-Blocking Findings",
            *_render_findings_section(blocking_findings, empty_message="No release-blocking findings."),
            "",
            "## 6. Assignment Pathway Readiness",
            *_render_findings_section(pathway_findings, empty_message="No assignment pathway concerns."),
            "",
            "## 7. Population and Candidate Depth",
            *_render_findings_section(population_findings, empty_message="No population or candidate-depth concerns."),
            "",
            "## 8. Structural and Relationship Findings",
            *_render_findings_section(structural_findings, empty_message="No structural or relationship concerns."),
            "",
            "## 9. Source Reconciliation",
            *_render_findings_section(
                [finding for finding in reconciliation_findings if finding.severity in {"blocker", "error", "warning"}],
                empty_message="No source reconciliation exceptions.",
            ),
            "",
            "## 10. Cross-Scale and Historical Regression",
            *_render_findings_section(
                [finding for finding in reconciliation_findings if "regression" in finding.category.lower()],
                empty_message="No cross-scale or historical regression concerns.",
            ),
            "",
            "## 11. Warnings and Accepted Exceptions",
            *_render_findings_section(warning_findings, empty_message="No warning findings were recorded."),
            "",
            "## 12. Recommended Remediation",
        ]
    )
    if assessment.findings:
        for finding in _sorted_findings(assessment.findings):
            lines.append(f"- `{finding.rule_id}`: {finding.recommended_action}")
    else:
        lines.append("- No remediation is required for this release.")

    lines.extend(
        [
            "",
            "## 13. Execution and Reproducibility Metadata",
            f"- Certification run id: `{assessment.certification_run_id}`",
            f"- Started at: `{assessment.started_at.isoformat()}`",
            f"- Completed at: `{assessment.completed_at.isoformat()}`",
            f"- Source snapshot path: `{assessment.source_snapshot_path or 'not supplied'}`",
            f"- Baseline id: `{assessment.baseline_id or 'not supplied'}`",
            f"- Code version: `{assessment.code_version or 'unknown'}`",
            f"- Git commit: `{assessment.git_commit or 'unknown'}`",
            f"- JSON snapshot path: `{snapshot_path or 'not published'}`",
            f"- Findings CSV path: `{findings_path or 'not published'}`",
            "",
            "## 14. Detailed Rule Results",
        ]
    )
    for rule in sorted(assessment.rule_results, key=lambda item: (item.pillar, item.rule_id)):
        lines.append(
            f"- `{rule.rule_id}` [{rule.pillar}] status=`{rule.status}` severity=`{rule.severity}`: {rule.message}"
        )
    if assessment.error_message:
        lines.extend(
            [
                "",
                "### Execution Error",
                assessment.error_message,
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def render_findings_csv(findings: tuple[CertificationFinding, ...]) -> str:
    """Render the deterministic CSV extract for certification findings."""
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "finding_id",
            "rule_id",
            "pillar",
            "category",
            "severity",
            "title",
            "message",
            "business_impact",
            "recommended_action",
            "affected_count",
            "sample_records",
            "accepted_exception",
            "exception_reason",
        ],
    )
    writer.writeheader()
    for finding in _sorted_findings(findings):
        writer.writerow(
            {
                "finding_id": finding.finding_id,
                "rule_id": finding.rule_id,
                "pillar": finding.pillar,
                "category": finding.category,
                "severity": finding.severity,
                "title": finding.title,
                "message": finding.message,
                "business_impact": finding.business_impact,
                "recommended_action": finding.recommended_action,
                "affected_count": finding.affected_count,
                "sample_records": json.dumps(list(finding.sample_records), sort_keys=True),
                "accepted_exception": str(finding.accepted_exception).lower(),
                "exception_reason": finding.exception_reason or "",
            }
        )
    return output.getvalue()


def _render_findings_section(
    findings: list[CertificationFinding],
    *,
    empty_message: str,
) -> list[str]:
    if not findings:
        return [empty_message]
    lines: list[str] = []
    for finding in findings:
        lines.append(
            f"- `{finding.rule_id}` ({finding.severity}): {finding.message} "
            f"[affected_count={finding.affected_count}]"
        )
    return lines


def _sorted_findings(findings: tuple[CertificationFinding, ...] | list[CertificationFinding]) -> list[CertificationFinding]:
    severity_rank = {"blocker": 3, "error": 2, "warning": 1, "info": 0}
    return sorted(
        findings,
        key=lambda item: (
            -severity_rank.get(item.severity, -1),
            item.rule_id,
            item.finding_id,
        ),
    )


def _checksum(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _join_artifact_path(root: str, child: str) -> str:
    normalized_root = root.rstrip("/\\")
    normalized_child = child.lstrip("/\\")
    if normalized_root.startswith("/"):
        return f"{normalized_root}/{normalized_child}"
    return str(Path(normalized_root) / normalized_child)


def _write_text(path: str, content: str, *, dbutils: Any | None = None) -> None:
    if path.startswith("/Volumes/") and dbutils is not None:
        dbutils.fs.put(path, content, True)
        return

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
