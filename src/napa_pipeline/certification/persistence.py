"""Durable persistence helpers for Raw certification runs and artifacts."""

from __future__ import annotations

from datetime import datetime
import json
from typing import Any

from napa_pipeline.certification.assessment import (
    ensure_utc_datetime,
    serialize_rule_result,
)
from napa_pipeline.certification.models import CertificationAssessment


RUNS_TABLE = "raw_certification_runs"
RULE_RUNS_TABLE = "raw_certification_rule_runs"
METRICS_TABLE = "raw_certification_metrics"
FINDINGS_TABLE = "raw_certification_findings"
ARTIFACTS_TABLE = "raw_certification_artifacts"
BASELINES_TABLE = "raw_certification_baselines"


def ensure_persistence_tables(spark: Any, operations_schema_fqn: str) -> None:
    """Create certification persistence tables in the shared operations schema."""
    for ddl in get_persistence_table_ddls(operations_schema_fqn):
        spark.sql(ddl)


def append_records(spark: Any, table_fqn: str, records: list[dict[str, Any]]) -> None:
    """Append records to a Delta persistence table when records are present."""
    if not records:
        return
    table_schema = spark.table(table_fqn).schema
    materialized_records = [
        _normalize_record_for_schema(table_fqn, table_schema, record) for record in records
    ]
    spark.createDataFrame(materialized_records, schema=table_schema).write.format("delta").mode(
        "append"
    ).saveAsTable(table_fqn)


def get_persistence_table_fqn(operations_schema_fqn: str, table_name: str) -> str:
    """Return the fully qualified persistence-table name."""
    return f"{operations_schema_fqn}.{table_name}"


def get_persistence_table_ddls(operations_schema_fqn: str) -> list[str]:
    """Return the DDL statements for certification persistence tables."""
    return [
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{RUNS_TABLE} (
    certification_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    release_version STRING,
    schema_version STRING,
    intended_use STRING NOT NULL,
    source_mode STRING NOT NULL,
    raw_path STRING NOT NULL,
    analysis_as_of_date DATE,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP NOT NULL,
    status STRING NOT NULL,
    certification_decision STRING NOT NULL,
    overall_score DOUBLE NOT NULL,
    config_snapshot_json STRING,
    source_snapshot_path STRING,
    baseline_id STRING,
    code_version STRING,
    git_commit STRING,
    error_message STRING
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{RULE_RUNS_TABLE} (
    certification_run_id STRING NOT NULL,
    rule_id STRING NOT NULL,
    pillar STRING NOT NULL,
    category STRING NOT NULL,
    status STRING NOT NULL,
    severity STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP NOT NULL,
    attempt_number INT NOT NULL,
    row_count_scanned BIGINT,
    result_json STRING NOT NULL,
    error_message STRING
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{METRICS_TABLE} (
    certification_run_id STRING NOT NULL,
    rule_id STRING NOT NULL,
    metric_name STRING NOT NULL,
    dimension_json STRING,
    metric_value DOUBLE,
    metric_text STRING,
    expected_min DOUBLE,
    expected_max DOUBLE,
    unit STRING
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{FINDINGS_TABLE} (
    certification_run_id STRING NOT NULL,
    finding_id STRING NOT NULL,
    rule_id STRING NOT NULL,
    severity STRING NOT NULL,
    title STRING NOT NULL,
    message STRING NOT NULL,
    business_impact STRING NOT NULL,
    recommended_action STRING NOT NULL,
    affected_count BIGINT,
    sample_records_json STRING,
    accepted_exception BOOLEAN NOT NULL,
    exception_reason STRING
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{ARTIFACTS_TABLE} (
    certification_run_id STRING NOT NULL,
    artifact_type STRING NOT NULL,
    artifact_path STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    checksum STRING NOT NULL
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{BASELINES_TABLE} (
    baseline_id STRING NOT NULL,
    release_name STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    snapshot_path STRING NOT NULL,
    notes STRING
)
USING DELTA
""".strip(),
    ]


def build_run_record(assessment: CertificationAssessment) -> dict[str, Any]:
    """Return the durable run record for one certification assessment."""
    return {
        "certification_run_id": assessment.certification_run_id,
        "release_name": assessment.release_name,
        "release_version": assessment.release_name,
        "schema_version": "1.0",
        "intended_use": assessment.intended_use,
        "source_mode": assessment.source_mode,
        "raw_path": assessment.raw_path,
        "analysis_as_of_date": assessment.analysis_as_of_date,
        "started_at": ensure_utc_datetime(assessment.started_at),
        "completed_at": ensure_utc_datetime(assessment.completed_at),
        "status": assessment.status,
        "certification_decision": assessment.certification_decision,
        "overall_score": assessment.overall_score,
        "config_snapshot_json": assessment.config_snapshot_json,
        "source_snapshot_path": assessment.source_snapshot_path,
        "baseline_id": assessment.baseline_id,
        "code_version": assessment.code_version,
        "git_commit": assessment.git_commit,
        "error_message": assessment.error_message,
    }


def build_rule_run_records(assessment: CertificationAssessment) -> list[dict[str, Any]]:
    """Return the durable rule-run records for one assessment."""
    started_at = ensure_utc_datetime(assessment.started_at)
    completed_at = ensure_utc_datetime(assessment.completed_at)
    records: list[dict[str, Any]] = []
    for rule in assessment.rule_results:
        records.append(
            {
                "certification_run_id": assessment.certification_run_id,
                "rule_id": rule.rule_id,
                "pillar": rule.pillar,
                "category": rule.category,
                "status": rule.status,
                "severity": rule.severity,
                "started_at": started_at,
                "completed_at": completed_at,
                "attempt_number": 1,
                "row_count_scanned": int(rule.denominator)
                if isinstance(rule.denominator, (int, float))
                else None,
                "result_json": json.dumps(serialize_rule_result(rule), sort_keys=True),
                "error_message": assessment.error_message if rule.status == "ERROR" else None,
            }
        )
    return records


def build_metric_records(assessment: CertificationAssessment) -> list[dict[str, Any]]:
    """Return the durable metric records for one assessment."""
    records: list[dict[str, Any]] = []
    for metric in assessment.metrics:
        records.append(
            {
                "certification_run_id": assessment.certification_run_id,
                "rule_id": metric.rule_id,
                "metric_name": metric.metric_name,
                "dimension_json": metric.dimension_json,
                "metric_value": metric.metric_value,
                "metric_text": metric.metric_text,
                "expected_min": metric.expected_min,
                "expected_max": metric.expected_max,
                "unit": metric.unit,
            }
        )
    for pillar_score in assessment.pillar_scores:
        records.append(
            {
                "certification_run_id": assessment.certification_run_id,
                "rule_id": "__assessment__",
                "metric_name": f"pillar_score::{pillar_score.pillar}",
                "dimension_json": json.dumps(
                    {
                        "pillar": pillar_score.pillar,
                        "weight": pillar_score.weight,
                        "applicable_rule_count": pillar_score.applicable_rule_count,
                    },
                    sort_keys=True,
                ),
                "metric_value": pillar_score.score,
                "metric_text": None,
                "expected_min": 0.0,
                "expected_max": pillar_score.weight,
                "unit": "points",
            }
        )
    records.append(
        {
            "certification_run_id": assessment.certification_run_id,
            "rule_id": "__assessment__",
            "metric_name": "overall_score",
            "dimension_json": json.dumps(
                {"decision": assessment.certification_decision},
                sort_keys=True,
            ),
            "metric_value": assessment.overall_score,
            "metric_text": None,
            "expected_min": 0.0,
            "expected_max": 100.0,
            "unit": "points",
        }
    )
    return records


def build_finding_records(assessment: CertificationAssessment) -> list[dict[str, Any]]:
    """Return the durable finding records for one assessment."""
    records: list[dict[str, Any]] = []
    for finding in assessment.findings:
        records.append(
            {
                "certification_run_id": assessment.certification_run_id,
                "finding_id": finding.finding_id,
                "rule_id": finding.rule_id,
                "severity": finding.severity,
                "title": finding.title,
                "message": finding.message,
                "business_impact": finding.business_impact,
                "recommended_action": finding.recommended_action,
                "affected_count": finding.affected_count,
                "sample_records_json": json.dumps(list(finding.sample_records), sort_keys=True),
                "accepted_exception": finding.accepted_exception,
                "exception_reason": finding.exception_reason,
            }
        )
    return records


def build_artifact_records(assessment: CertificationAssessment) -> list[dict[str, Any]]:
    """Return the durable artifact records for one assessment."""
    records: list[dict[str, Any]] = []
    for artifact in assessment.artifacts:
        records.append(
            {
                "certification_run_id": assessment.certification_run_id,
                "artifact_type": artifact.artifact_type,
                "artifact_path": artifact.artifact_path,
                "created_at": ensure_utc_datetime(artifact.created_at),
                "checksum": artifact.checksum,
            }
        )
    return records


def _normalize_record_for_schema(
    table_fqn: str,
    table_schema: Any,
    record: dict[str, Any],
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field in table_schema.fields:
        value = record.get(field.name)
        if value is None and not field.nullable:
            raise ValueError(
                f"Cannot append to {table_fqn}: required field '{field.name}' is null or missing. "
                f"Record keys: {sorted(record.keys())}"
            )
        normalized[field.name] = value
    return normalized
