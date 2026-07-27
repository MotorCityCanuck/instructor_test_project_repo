"""Assessment and decision helpers for Raw certification."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from typing import Any, Iterable, Sequence

from napa_pipeline.certification.config import CertificationConfig
from napa_pipeline.certification.environment import ReleaseEnvironment
from napa_pipeline.certification.models import (
    CertificationAssessment,
    CertificationFinding,
    CertificationMetric,
    CertificationPillarScore,
    CertificationRuleResult,
    InventoryCertificationResult,
)


PILLAR_WEIGHTS: tuple[tuple[str, float], ...] = (
    ("Release Inventory and Format", 10.0),
    ("Schema and Structural Integrity", 20.0),
    ("Population and Lifecycle Fitness", 15.0),
    ("Team and Partnership Fitness", 15.0),
    ("Competition and Evidence Fitness", 15.0),
    ("Ratings, Confidence, and Development Fitness", 10.0),
    ("Assignment Pathway Readiness", 10.0),
    ("Source Reconciliation and Regression", 5.0),
)
PILLAR_WEIGHT_BY_NAME = dict(PILLAR_WEIGHTS)
ROLE_TO_INTENDED_USE = {
    "development": "CERTIFIED_FOR_DEVELOPMENT",
    "validation": "CERTIFIED_FOR_ENGINEERING_VALIDATION",
    "production": "CERTIFIED_FOR_PRODUCTION_ANALYTICS",
}
SEVERITY_RANK = {
    "info": 0,
    "warning": 1,
    "error": 2,
    "blocker": 3,
}
STATUS_RANK = {
    "PASS": 0,
    "WARN": 1,
    "FAIL": 2,
    "ERROR": 3,
}
STATUS_SCORE = {
    "PASS": 1.0,
    "WARN": 0.75,
    "FAIL": 0.0,
    "ERROR": 0.0,
}


def build_certification_assessment(
    *,
    certification_run_id: str,
    config: CertificationConfig,
    environment: ReleaseEnvironment,
    inventory_result: InventoryCertificationResult,
    rule_results: Sequence[CertificationRuleResult] | Iterable[CertificationRuleResult],
    started_at: datetime,
    completed_at: datetime | None = None,
    analysis_as_of_date: date | str | None = None,
    source_snapshot_path: str | None = None,
    baseline_id: str | None = None,
    git_commit: str | None = None,
    execution_error: str | None = None,
) -> CertificationAssessment:
    """Build a deterministic certification assessment from evaluated rule results."""
    completed = completed_at or utc_now()
    materialized_rules = tuple(rule_results)
    findings = tuple(_build_finding(rule) for rule in materialized_rules if _is_finding(rule))
    metrics = tuple(_build_metrics(materialized_rules))
    severity_counts = _count_severities(findings)
    status_counts = _count_statuses(materialized_rules)
    hard_gate_rule_ids = tuple(
        sorted(
            rule.rule_id
            for rule in materialized_rules
            if rule.status == "FAIL" and rule.severity == "blocker"
        )
    )
    release_blocking_rule_ids = tuple(
        sorted(
            rule.rule_id
            for rule in materialized_rules
            if rule.status == "FAIL" and rule.severity in {"blocker", "error"}
        )
    )
    pillar_scores = tuple(_build_pillar_scores(materialized_rules))
    overall_score = round(sum(score.score for score in pillar_scores), 1)
    certification_decision = _resolve_certification_decision(
        rule_results=materialized_rules,
        execution_error=execution_error,
    )
    status = "EXECUTION_FAILED" if execution_error else "COMPLETED"
    return CertificationAssessment(
        certification_run_id=certification_run_id,
        release_name=config.release_name,
        release_role=config.release_role,
        intended_use=ROLE_TO_INTENDED_USE.get(config.release_role, config.release_role.upper()),
        source_mode=inventory_result.source_mode,
        raw_path=inventory_result.release_path,
        analysis_as_of_date=_normalize_date_string(analysis_as_of_date),
        started_at=ensure_utc_datetime(started_at),
        completed_at=ensure_utc_datetime(completed),
        status=status,
        certification_decision=certification_decision,
        overall_score=overall_score,
        pillar_scores=pillar_scores,
        severity_counts=severity_counts,
        status_counts=status_counts,
        rule_results=materialized_rules,
        findings=findings,
        metrics=metrics,
        hard_gate_rule_ids=hard_gate_rule_ids,
        release_blocking_rule_ids=release_blocking_rule_ids,
        source_snapshot_path=source_snapshot_path,
        baseline_id=baseline_id,
        code_version=str(config.data["project"]["pipeline_version"]),
        git_commit=git_commit,
        config_snapshot_json=json.dumps(config.data, sort_keys=True),
        error_message=execution_error,
    )


def attach_artifacts(
    assessment: CertificationAssessment,
    artifacts: Sequence[Any],
) -> CertificationAssessment:
    """Return an assessment with its published artifact records attached."""
    return CertificationAssessment(
        certification_run_id=assessment.certification_run_id,
        release_name=assessment.release_name,
        release_role=assessment.release_role,
        intended_use=assessment.intended_use,
        source_mode=assessment.source_mode,
        raw_path=assessment.raw_path,
        analysis_as_of_date=assessment.analysis_as_of_date,
        started_at=assessment.started_at,
        completed_at=assessment.completed_at,
        status=assessment.status,
        certification_decision=assessment.certification_decision,
        overall_score=assessment.overall_score,
        pillar_scores=assessment.pillar_scores,
        severity_counts=assessment.severity_counts,
        status_counts=assessment.status_counts,
        rule_results=assessment.rule_results,
        findings=assessment.findings,
        metrics=assessment.metrics,
        artifacts=tuple(artifacts),
        hard_gate_rule_ids=assessment.hard_gate_rule_ids,
        release_blocking_rule_ids=assessment.release_blocking_rule_ids,
        source_snapshot_path=assessment.source_snapshot_path,
        baseline_id=assessment.baseline_id,
        code_version=assessment.code_version,
        git_commit=assessment.git_commit,
        config_snapshot_json=assessment.config_snapshot_json,
        error_message=assessment.error_message,
    )


def build_assessment_snapshot(assessment: CertificationAssessment) -> dict[str, Any]:
    """Return the JSON-serializable certification snapshot payload."""
    return {
        "certification_run_id": assessment.certification_run_id,
        "release_name": assessment.release_name,
        "release_role": assessment.release_role,
        "intended_use": assessment.intended_use,
        "analysis_as_of_date": assessment.analysis_as_of_date,
        "source": {
            "mode": assessment.source_mode,
            "path": assessment.raw_path,
            "manifest": assessment.source_snapshot_path,
        },
        "status": assessment.status,
        "decision": assessment.certification_decision,
        "score": assessment.overall_score,
        "pillar_scores": {
            score.pillar: {
                "weight": score.weight,
                "applicable_rule_count": score.applicable_rule_count,
                "passed_rule_count": score.passed_rule_count,
                "warning_rule_count": score.warning_rule_count,
                "failed_rule_count": score.failed_rule_count,
                "score": score.score,
            }
            for score in assessment.pillar_scores
        },
        "severity_counts": assessment.severity_counts,
        "status_counts": assessment.status_counts,
        "hard_gate_rule_ids": list(assessment.hard_gate_rule_ids),
        "release_blocking_rule_ids": list(assessment.release_blocking_rule_ids),
        "results": [serialize_rule_result(rule) for rule in assessment.rule_results],
        "findings": [serialize_finding(finding) for finding in assessment.findings],
        "metrics": [serialize_metric(metric) for metric in assessment.metrics],
        "artifacts": {
            artifact.artifact_type: {
                "path": artifact.artifact_path,
                "checksum": artifact.checksum,
                "created_at": artifact.created_at.isoformat(),
            }
            for artifact in sorted(assessment.artifacts, key=lambda item: item.artifact_type)
        },
        "baseline_id": assessment.baseline_id,
        "code_version": assessment.code_version,
        "git_commit": assessment.git_commit,
        "config_snapshot_json": assessment.config_snapshot_json,
        "error_message": assessment.error_message,
    }


def serialize_rule_result(rule: CertificationRuleResult) -> dict[str, Any]:
    """Return a stable JSON-serializable rule-result mapping."""
    return {
        "rule_id": rule.rule_id,
        "name": rule.name,
        "pillar": rule.pillar,
        "category": rule.category,
        "status": rule.status,
        "severity": rule.severity,
        "message": rule.message,
        "affected_count": rule.affected_count,
        "observed_value": rule.observed_value,
        "expected_value": rule.expected_value,
        "expected_min": rule.expected_min,
        "expected_max": rule.expected_max,
        "numerator": rule.numerator,
        "denominator": rule.denominator,
        "unit": rule.unit,
        "sample_records": list(rule.sample_records),
        "business_impact": rule.business_impact,
        "recommended_action": rule.recommended_action,
        "execution_mode": rule.execution_mode,
        "remediation_owner": rule.remediation_owner,
    }


def serialize_finding(finding: CertificationFinding) -> dict[str, Any]:
    """Return a stable JSON-serializable finding mapping."""
    return {
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
        "sample_records": list(finding.sample_records),
        "accepted_exception": finding.accepted_exception,
        "exception_reason": finding.exception_reason,
    }


def serialize_metric(metric: CertificationMetric) -> dict[str, Any]:
    """Return a stable JSON-serializable metric mapping."""
    return {
        "rule_id": metric.rule_id,
        "metric_name": metric.metric_name,
        "dimension_json": metric.dimension_json,
        "metric_value": metric.metric_value,
        "metric_text": metric.metric_text,
        "expected_min": metric.expected_min,
        "expected_max": metric.expected_max,
        "unit": metric.unit,
    }


def ensure_utc_datetime(value: datetime) -> datetime:
    """Normalize a datetime to UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def _normalize_date_string(value: date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _resolve_certification_decision(
    *,
    rule_results: Sequence[CertificationRuleResult],
    execution_error: str | None,
) -> str:
    if execution_error:
        return "EXECUTION_FAILED"

    if any(rule.status == "FAIL" and rule.severity in {"blocker", "error"} for rule in rule_results):
        return "REJECTED"

    if any(rule.status == "WARN" or rule.severity == "warning" for rule in rule_results):
        return "CERTIFIED_WITH_WARNINGS"

    return "CERTIFIED"


def _build_pillar_scores(
    rule_results: Sequence[CertificationRuleResult],
) -> list[CertificationPillarScore]:
    grouped: dict[str, list[CertificationRuleResult]] = defaultdict(list)
    for rule in rule_results:
        grouped[rule.pillar].append(rule)

    scores: list[CertificationPillarScore] = []
    for pillar, weight in PILLAR_WEIGHTS:
        pillar_rules = grouped.get(pillar, [])
        applicable_rules = [rule for rule in pillar_rules if rule.status not in {"SKIP"}]
        if applicable_rules:
            average_score = sum(STATUS_SCORE.get(rule.status, 0.0) for rule in applicable_rules) / len(
                applicable_rules
            )
        else:
            average_score = 1.0
        scores.append(
            CertificationPillarScore(
                pillar=pillar,
                weight=weight,
                applicable_rule_count=len(applicable_rules),
                passed_rule_count=sum(1 for rule in applicable_rules if rule.status == "PASS"),
                warning_rule_count=sum(1 for rule in applicable_rules if rule.status == "WARN"),
                failed_rule_count=sum(1 for rule in applicable_rules if rule.status == "FAIL"),
                score=round(weight * average_score, 2),
            )
        )
    return scores


def _build_metrics(
    rule_results: Sequence[CertificationRuleResult],
) -> list[CertificationMetric]:
    metrics: list[CertificationMetric] = []
    for rule in rule_results:
        metric_value: float | None = None
        metric_text: str | None = None
        observed = rule.observed_value
        if isinstance(observed, bool):
            metric_value = float(int(observed))
        elif isinstance(observed, (int, float)):
            metric_value = float(observed)
        elif observed is not None:
            metric_text = str(observed)

        if metric_value is None and metric_text is None:
            metric_text = rule.message

        dimension_json = json.dumps(
            {
                "pillar": rule.pillar,
                "category": rule.category,
                "status": rule.status,
                "severity": rule.severity,
            },
            sort_keys=True,
        )
        metrics.append(
            CertificationMetric(
                rule_id=rule.rule_id,
                metric_name="observed_value",
                metric_value=metric_value,
                metric_text=metric_text,
                dimension_json=dimension_json,
                expected_min=rule.expected_min,
                expected_max=rule.expected_max,
                unit=rule.unit,
            )
        )
    return sorted(metrics, key=lambda item: (item.rule_id, item.metric_name))


def _build_finding(rule: CertificationRuleResult) -> CertificationFinding:
    finding_hash = hashlib.sha256(
        "|".join(
            [
                rule.rule_id,
                rule.status,
                rule.severity,
                rule.message,
                str(rule.affected_count),
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]
    return CertificationFinding(
        finding_id=f"finding_{finding_hash}",
        rule_id=rule.rule_id,
        pillar=rule.pillar,
        category=rule.category,
        severity=rule.severity,
        title=rule.name,
        message=rule.message,
        business_impact=rule.business_impact or _default_business_impact(rule),
        recommended_action=rule.recommended_action or _default_recommended_action(rule),
        affected_count=rule.affected_count,
        sample_records=tuple(sorted(str(sample) for sample in rule.sample_records)),
    )


def _default_business_impact(rule: CertificationRuleResult) -> str:
    if rule.severity in {"blocker", "error"}:
        return (
            f"This issue undermines {rule.pillar.lower()} and blocks release approval "
            "for the intended use until it is remediated."
        )
    return (
        f"This issue should be reviewed because it weakens {rule.pillar.lower()} "
        "and may limit student or engineering use cases."
    )


def _default_recommended_action(rule: CertificationRuleResult) -> str:
    return (
        f"Review the {rule.category} records for rule {rule.rule_id}, validate the upstream "
        "release semantics, and rerun certification after remediation."
    )


def _is_finding(rule: CertificationRuleResult) -> bool:
    if rule.status in {"FAIL", "WARN"}:
        return True
    return rule.severity in {"warning", "error", "blocker"} and rule.status != "PASS"


def _count_severities(findings: Sequence[CertificationFinding]) -> dict[str, int]:
    counts = Counter(finding.severity for finding in findings)
    return {severity: counts.get(severity, 0) for severity in ("info", "warning", "error", "blocker")}


def _count_statuses(rule_results: Sequence[CertificationRuleResult]) -> dict[str, int]:
    counts = Counter(rule.status for rule in rule_results)
    return {status: counts.get(status, 0) for status in ("PASS", "WARN", "FAIL", "ERROR", "SKIP")}
