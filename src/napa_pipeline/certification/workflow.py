"""Workflow orchestration helpers for Databricks Raw certification tasks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import subprocess
from typing import Any
from uuid import uuid4

from napa_pipeline.certification.assessment import (
    attach_artifacts,
    build_certification_assessment,
)
from napa_pipeline.certification.assignment_probes import evaluate_assignment_probes
from napa_pipeline.certification.config import CertificationConfig
from napa_pipeline.certification.environment import ReleaseEnvironment
from napa_pipeline.certification.fitness_rules import evaluate_fitness_rules
from napa_pipeline.certification.models import CertificationAssessment, CertificationRuleResult
from napa_pipeline.certification.models import InventoryCertificationResult
from napa_pipeline.certification.persistence import (
    append_records,
    build_artifact_records,
    build_finding_records,
    build_metric_records,
    build_rule_run_records,
    build_run_record,
    ensure_persistence_tables,
    get_persistence_table_fqn,
    ARTIFACTS_TABLE,
    FINDINGS_TABLE,
    METRICS_TABLE,
    RULE_RUNS_TABLE,
    RUNS_TABLE,
)
from napa_pipeline.certification.reconciliation import (
    build_release_metrics,
    evaluate_reconciliation_and_regression_rules,
    load_certification_snapshot,
)
from napa_pipeline.certification.reporting import CertificationArtifactBundle, publish_artifacts
from napa_pipeline.certification.source_loader import run_inventory_certification
from napa_pipeline.certification.structural_rules import evaluate_structural_rules


@dataclass(frozen=True)
class CertificationWorkflowResult:
    """Outcome of the end-to-end certification workflow task."""

    assessment: CertificationAssessment
    artifact_bundle: CertificationArtifactBundle | None


@dataclass(frozen=True)
class ReleaseGateResult:
    """Outcome of evaluating the workflow release gate."""

    should_fail: bool
    message: str


def resolve_certification_run_id(run_id: str | None) -> str:
    """Return the supplied certification run id or generate a new one."""
    normalized = (run_id or "").strip()
    return normalized or str(uuid4())


def execute_certification_workflow(
    *,
    spark: Any,
    dbutils: Any,
    config: CertificationConfig,
    environment: ReleaseEnvironment,
    certification_run_id: str,
    analysis_as_of_date: str | None = None,
    source_snapshot_path: str | None = None,
    baseline_id: str | None = None,
    persist_results: bool = True,
    publish_report: bool = True,
) -> CertificationWorkflowResult:
    """Execute the certification rule pipeline and publish durable evidence."""
    started_at = datetime.now(timezone.utc)
    partial_rules: list[CertificationRuleResult] = []
    artifact_bundle: CertificationArtifactBundle | None = None
    inventory_result: InventoryCertificationResult | None = None
    try:
        inventory_result = run_inventory_certification(spark, dbutils, config, environment)
        partial_rules.extend(inventory_result.rule_results)

        structural_results = evaluate_structural_rules(spark, config, inventory_result)
        partial_rules.extend(structural_results)

        fitness_results = evaluate_fitness_rules(spark, config, inventory_result)
        partial_rules.extend(fitness_results)

        assignment_results = evaluate_assignment_probes(
            spark,
            config,
            inventory_result,
            structural_results=structural_results,
        )
        partial_rules.extend(assignment_results)

        release_metrics = build_release_metrics(spark, inventory_result)
        reconciliation_results = evaluate_reconciliation_and_regression_rules(
            config,
            release_metrics,
            source_snapshot=load_certification_snapshot(source_snapshot_path),
            prior_release_snapshot=load_certification_snapshot(baseline_id),
            cross_scale_snapshots=[],
        )
        partial_rules.extend(reconciliation_results)

        assessment = build_certification_assessment(
            certification_run_id=certification_run_id,
            config=config,
            environment=environment,
            inventory_result=inventory_result,
            rule_results=partial_rules,
            started_at=started_at,
            analysis_as_of_date=analysis_as_of_date,
            source_snapshot_path=source_snapshot_path,
            baseline_id=baseline_id,
            git_commit=resolve_git_commit(),
        )
    except Exception as exc:
        inventory_stub = inventory_result or InventoryCertificationResult(
            release_name=config.release_name,
            release_path=environment.raw_volume_path,
            source_mode="parquet",
            path_exists=False,
            expected_sources=(),
            discovered_files=(),
            loaded_sources=(),
            manifest=None,
            rule_results=(),
        )
        assessment = build_certification_assessment(
            certification_run_id=certification_run_id,
            config=config,
            environment=environment,
            inventory_result=inventory_stub,
            rule_results=partial_rules,
            started_at=started_at,
            analysis_as_of_date=analysis_as_of_date,
            source_snapshot_path=source_snapshot_path,
            baseline_id=baseline_id,
            git_commit=resolve_git_commit(),
            execution_error=str(exc),
        )

    if publish_report:
        artifact_bundle = publish_artifacts(
            assessment,
            environment.artifacts_root_path,
            dbutils=dbutils,
        )
        assessment = attach_artifacts(assessment, artifact_bundle.artifacts)

    if persist_results:
        persist_certification_assessment(spark, environment, assessment)

    return CertificationWorkflowResult(
        assessment=assessment,
        artifact_bundle=artifact_bundle,
    )


def persist_certification_assessment(
    spark: Any,
    environment: ReleaseEnvironment,
    assessment: CertificationAssessment,
) -> None:
    """Persist the certification assessment and related durable records."""
    operations_schema_fqn = f"{environment.catalog}.{environment.operations_schema}"
    ensure_persistence_tables(spark, operations_schema_fqn)
    append_records(
        spark,
        get_persistence_table_fqn(operations_schema_fqn, RUNS_TABLE),
        [build_run_record(assessment)],
    )
    append_records(
        spark,
        get_persistence_table_fqn(operations_schema_fqn, RULE_RUNS_TABLE),
        build_rule_run_records(assessment),
    )
    append_records(
        spark,
        get_persistence_table_fqn(operations_schema_fqn, METRICS_TABLE),
        build_metric_records(assessment),
    )
    append_records(
        spark,
        get_persistence_table_fqn(operations_schema_fqn, FINDINGS_TABLE),
        build_finding_records(assessment),
    )
    append_records(
        spark,
        get_persistence_table_fqn(operations_schema_fqn, ARTIFACTS_TABLE),
        build_artifact_records(assessment),
    )


def evaluate_release_gate(
    *,
    certification_decision: str,
    fail_on: str,
    severity_counts: dict[str, int] | None = None,
) -> ReleaseGateResult:
    """Evaluate the final workflow release gate for a completed certification run."""
    severity = (fail_on or "blocker").strip().lower()
    counts = severity_counts or {}
    if certification_decision == "EXECUTION_FAILED":
        return ReleaseGateResult(
            should_fail=True,
            message="Certification execution failed; the release cannot be approved.",
        )
    if certification_decision == "REJECTED":
        return ReleaseGateResult(
            should_fail=True,
            message="Certification rejected the release; review the published report before release.",
        )
    if severity == "warning" and counts.get("warning", 0) > 0:
        return ReleaseGateResult(
            should_fail=True,
            message="Certification completed with warnings and fail_on=warning.",
        )
    if severity == "never":
        return ReleaseGateResult(
            should_fail=False,
            message="Certification completed and the release gate is configured to never fail.",
        )
    return ReleaseGateResult(
        should_fail=False,
        message=f"Certification completed with decision {certification_decision}.",
    )


def resolve_git_commit() -> str | None:
    """Return the current git commit hash when available."""
    repo_root = Path(__file__).resolve().parents[3]
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    commit = completed.stdout.strip()
    return commit or None
