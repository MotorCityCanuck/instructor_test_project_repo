"""Operations helpers for Silver-to-Gold audit tables and records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
from typing import Any
from uuid import uuid4

from napa_pipeline.silver_to_gold.environment import GoldRuntimeContext


PIPELINE_RUNS_TABLE = "pipeline_runs"
TABLE_RUNS_TABLE = "gold_table_runs"
QUALITY_RESULTS_TABLE = "gold_quality_results"
RECONCILIATION_RESULTS_TABLE = "gold_reconciliation_results"
MODEL_RUNS_TABLE = "gold_model_runs"
MODEL_METRICS_TABLE = "gold_model_metrics"
RECOMMENDATION_RUNS_TABLE = "gold_recommendation_runs"


def utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def ensure_utc_datetime(value: datetime) -> datetime:
    """Normalize naive or offset-aware datetimes to UTC-aware values."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class PipelineContext:
    """Common context fields for Gold operations records."""

    pipeline_run_id: str
    pipeline_name: str
    pipeline_version: str
    release_name: str
    release_role: str
    processing_mode: str
    configuration_hash: str
    operations_schema_fqn: str
    upstream_pipeline_run_id: str
    analysis_as_of_date: date
    scoring_scenario: str
    authoritative_recommendation_flag: bool
    deterministic_seed: int


def create_pipeline_context(
    runtime_context: GoldRuntimeContext,
    *,
    processing_mode: str = "full_refresh",
    pipeline_run_id: str | None = None,
) -> PipelineContext:
    """Create a pipeline context from the resolved Gold runtime context."""
    return PipelineContext(
        pipeline_run_id=pipeline_run_id or str(uuid4()),
        pipeline_name="silver_to_gold",
        pipeline_version=runtime_context.pipeline_version,
        release_name=runtime_context.release_name,
        release_role=runtime_context.release_role,
        processing_mode=processing_mode,
        configuration_hash=runtime_context.configuration_hash,
        operations_schema_fqn=f"{runtime_context.catalog}.{runtime_context.operations_schema}",
        upstream_pipeline_run_id=runtime_context.upstream_silver_run_id,
        analysis_as_of_date=runtime_context.analysis_as_of_date,
        scoring_scenario=runtime_context.scoring_scenario,
        authoritative_recommendation_flag=runtime_context.authoritative_recommendation_flag,
        deterministic_seed=runtime_context.deterministic_seed,
    )


def ensure_operations_tables(spark: Any, context: PipelineContext) -> None:
    """Create Gold operations tables in the configured shared operations schema."""
    for ddl in get_operations_table_ddls(context.operations_schema_fqn):
        spark.sql(ddl)


def append_records(spark: Any, table_fqn: str, records: list[dict[str, Any]]) -> None:
    """Append records to an operations table when records are present."""
    if not records:
        return
    table_schema = spark.table(table_fqn).schema
    materialized_records = records
    if hasattr(table_schema, "fields"):
        materialized_records = [
            _normalize_record_for_schema(table_fqn, table_schema, record)
            for record in records
        ]
    spark.createDataFrame(materialized_records, schema=table_schema).write.format("delta").mode(
        "append"
    ).saveAsTable(table_fqn)


def get_operations_table_fqn(context: PipelineContext, table_name: str) -> str:
    """Return the fully qualified name of an operations table."""
    return f"{context.operations_schema_fqn}.{table_name}"


def get_operations_table_ddls(operations_schema_fqn: str) -> list[str]:
    """Return the DDL statements for the shared and Gold-specific operations tables."""
    return [
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{PIPELINE_RUNS_TABLE} (
    pipeline_run_id STRING NOT NULL,
    pipeline_name STRING NOT NULL,
    pipeline_version STRING NOT NULL,
    release_name STRING NOT NULL,
    processing_mode STRING NOT NULL,
    configuration_hash STRING NOT NULL,
    workflow_run_id STRING,
    upstream_pipeline_run_id STRING,
    analysis_as_of_date DATE,
    scoring_scenario STRING,
    authoritative_recommendation_flag BOOLEAN,
    status STRING NOT NULL,
    started_ts TIMESTAMP NOT NULL,
    completed_ts TIMESTAMP,
    duration_seconds DOUBLE,
    triggered_by STRING,
    error_class STRING,
    error_message STRING
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{TABLE_RUNS_TABLE} (
    pipeline_run_id STRING NOT NULL,
    upstream_pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    target_gold_table STRING NOT NULL,
    build_stage STRING NOT NULL,
    build_order INT NOT NULL,
    status STRING NOT NULL,
    started_ts TIMESTAMP NOT NULL,
    completed_ts TIMESTAMP,
    duration_seconds DOUBLE,
    input_row_count BIGINT,
    output_row_count BIGINT,
    excluded_row_count BIGINT,
    warning_count BIGINT,
    error_message STRING
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{QUALITY_RESULTS_TABLE} (
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    target_table STRING NOT NULL,
    rule_id STRING NOT NULL,
    rule_category STRING NOT NULL,
    severity STRING NOT NULL,
    evaluated_row_count BIGINT,
    failed_row_count BIGINT,
    failure_pct DOUBLE,
    status STRING NOT NULL,
    sample_keys ARRAY<STRING>,
    evaluated_ts TIMESTAMP NOT NULL
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{RECONCILIATION_RESULTS_TABLE} (
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    reconciliation_name STRING NOT NULL,
    source_count BIGINT,
    accepted_count BIGINT,
    excluded_count BIGINT,
    difference BIGINT,
    status STRING NOT NULL,
    evaluated_ts TIMESTAMP NOT NULL
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{MODEL_RUNS_TABLE} (
    model_run_id STRING NOT NULL,
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    model_name STRING NOT NULL,
    model_version STRING NOT NULL,
    algorithm STRING NOT NULL,
    feature_definition_version STRING NOT NULL,
    training_start_date DATE,
    training_end_date DATE,
    validation_start_date DATE,
    test_start_date DATE,
    random_seed BIGINT,
    hyperparameter_hash STRING,
    status STRING NOT NULL,
    started_ts TIMESTAMP NOT NULL,
    completed_ts TIMESTAMP,
    error_message STRING
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{MODEL_METRICS_TABLE} (
    model_run_id STRING NOT NULL,
    split_name STRING NOT NULL,
    metric_name STRING NOT NULL,
    metric_value DOUBLE,
    evaluated_row_count BIGINT,
    evaluated_ts TIMESTAMP NOT NULL
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{RECOMMENDATION_RUNS_TABLE} (
    recommendation_run_id STRING NOT NULL,
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    analysis_as_of_date DATE NOT NULL,
    scoring_scenario STRING NOT NULL,
    methodology_version STRING NOT NULL,
    eligible_team_count BIGINT,
    primary_recommendation_count BIGINT,
    alternate_recommendation_count BIGINT,
    watchlist_count BIGINT,
    status STRING NOT NULL,
    created_ts TIMESTAMP NOT NULL
)
USING DELTA
""".strip(),
    ]


def build_pipeline_run_start_record(
    context: PipelineContext,
    *,
    started_ts: datetime | None = None,
    workflow_run_id: str | None = None,
    triggered_by: str | None = None,
) -> dict[str, Any]:
    """Build the shared pipeline-runs start record for Gold."""
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "pipeline_name": context.pipeline_name,
        "pipeline_version": context.pipeline_version,
        "release_name": context.release_name,
        "processing_mode": context.processing_mode,
        "configuration_hash": context.configuration_hash,
        "workflow_run_id": workflow_run_id,
        "upstream_pipeline_run_id": context.upstream_pipeline_run_id,
        "analysis_as_of_date": context.analysis_as_of_date,
        "scoring_scenario": context.scoring_scenario,
        "authoritative_recommendation_flag": context.authoritative_recommendation_flag,
        "status": "RUNNING",
        "started_ts": started_ts or utc_now(),
        "completed_ts": None,
        "duration_seconds": None,
        "triggered_by": triggered_by,
        "error_class": None,
        "error_message": None,
    }


def build_pipeline_run_end_record(
    context: PipelineContext,
    *,
    started_ts: datetime,
    status: str,
    completed_ts: datetime | None = None,
    error_class: str | None = None,
    error_message: str | None = None,
    workflow_run_id: str | None = None,
    triggered_by: str | None = None,
) -> dict[str, Any]:
    """Build the shared pipeline-runs completion record for Gold."""
    started = ensure_utc_datetime(started_ts)
    completed = ensure_utc_datetime(completed_ts or utc_now())
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "pipeline_name": context.pipeline_name,
        "pipeline_version": context.pipeline_version,
        "release_name": context.release_name,
        "processing_mode": context.processing_mode,
        "configuration_hash": context.configuration_hash,
        "workflow_run_id": workflow_run_id,
        "upstream_pipeline_run_id": context.upstream_pipeline_run_id,
        "analysis_as_of_date": context.analysis_as_of_date,
        "scoring_scenario": context.scoring_scenario,
        "authoritative_recommendation_flag": context.authoritative_recommendation_flag,
        "status": status,
        "started_ts": started,
        "completed_ts": completed,
        "duration_seconds": (completed - started).total_seconds(),
        "triggered_by": triggered_by,
        "error_class": error_class,
        "error_message": error_message,
    }


def build_table_run_start_record(
    context: PipelineContext,
    *,
    target_gold_table: str,
    build_stage: str,
    build_order: int,
    started_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build the Gold table-run start record."""
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "upstream_pipeline_run_id": context.upstream_pipeline_run_id,
        "release_name": context.release_name,
        "target_gold_table": target_gold_table,
        "build_stage": build_stage,
        "build_order": build_order,
        "status": "RUNNING",
        "started_ts": started_ts or utc_now(),
        "completed_ts": None,
        "duration_seconds": None,
        "input_row_count": None,
        "output_row_count": None,
        "excluded_row_count": None,
        "warning_count": None,
        "error_message": None,
    }


def build_table_run_end_record(
    context: PipelineContext,
    *,
    target_gold_table: str,
    build_stage: str,
    build_order: int,
    started_ts: datetime,
    status: str,
    input_row_count: int | None = None,
    output_row_count: int | None = None,
    excluded_row_count: int | None = None,
    warning_count: int | None = None,
    error_message: str | None = None,
    completed_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build the Gold table-run completion record."""
    started = ensure_utc_datetime(started_ts)
    completed = ensure_utc_datetime(completed_ts or utc_now())
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "upstream_pipeline_run_id": context.upstream_pipeline_run_id,
        "release_name": context.release_name,
        "target_gold_table": target_gold_table,
        "build_stage": build_stage,
        "build_order": build_order,
        "status": status,
        "started_ts": started,
        "completed_ts": completed,
        "duration_seconds": (completed - started).total_seconds(),
        "input_row_count": input_row_count,
        "output_row_count": output_row_count,
        "excluded_row_count": excluded_row_count,
        "warning_count": warning_count,
        "error_message": error_message,
    }


def build_quality_result_record(
    context: PipelineContext,
    *,
    target_table: str,
    rule_id: str,
    rule_category: str,
    severity: str,
    status: str,
    evaluated_row_count: int | None = None,
    failed_row_count: int | None = None,
    failure_pct: float | None = None,
    sample_keys: list[str] | None = None,
    evaluated_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build a Gold quality result record."""
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "release_name": context.release_name,
        "target_table": target_table,
        "rule_id": rule_id,
        "rule_category": rule_category,
        "severity": severity,
        "evaluated_row_count": evaluated_row_count,
        "failed_row_count": failed_row_count,
        "failure_pct": failure_pct,
        "status": status,
        "sample_keys": sample_keys,
        "evaluated_ts": evaluated_ts or utc_now(),
    }


def build_reconciliation_record(
    context: PipelineContext,
    *,
    reconciliation_name: str,
    source_count: int | None,
    accepted_count: int | None,
    excluded_count: int | None,
    status: str | None = None,
    evaluated_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build a Gold reconciliation result record."""
    source = int(source_count) if source_count is not None else 0
    accepted = int(accepted_count) if accepted_count is not None else 0
    excluded = int(excluded_count) if excluded_count is not None else 0
    difference = source - (accepted + excluded)
    resolved_status = status or ("PASSED" if difference == 0 else "FAILED")
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "release_name": context.release_name,
        "reconciliation_name": reconciliation_name,
        "source_count": source_count,
        "accepted_count": accepted_count,
        "excluded_count": excluded_count,
        "difference": difference,
        "status": resolved_status,
        "evaluated_ts": evaluated_ts or utc_now(),
    }


def build_model_run_start_record(
    context: PipelineContext,
    *,
    model_name: str,
    model_version: str,
    algorithm: str,
    feature_definition_version: str,
    model_run_id: str | None = None,
    training_start_date: date | None = None,
    training_end_date: date | None = None,
    validation_start_date: date | None = None,
    test_start_date: date | None = None,
    hyperparameter_hash: str | None = None,
    started_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build a Gold model-run start record."""
    return {
        "model_run_id": model_run_id or str(uuid4()),
        "pipeline_run_id": context.pipeline_run_id,
        "release_name": context.release_name,
        "model_name": model_name,
        "model_version": model_version,
        "algorithm": algorithm,
        "feature_definition_version": feature_definition_version,
        "training_start_date": training_start_date,
        "training_end_date": training_end_date,
        "validation_start_date": validation_start_date,
        "test_start_date": test_start_date,
        "random_seed": context.deterministic_seed,
        "hyperparameter_hash": hyperparameter_hash,
        "status": "RUNNING",
        "started_ts": started_ts or utc_now(),
        "completed_ts": None,
        "error_message": None,
    }


def build_model_run_end_record(
    start_record: dict[str, Any],
    *,
    status: str,
    completed_ts: datetime | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Build a completed Gold model-run record from a start record."""
    record = dict(start_record)
    record["status"] = status
    record["completed_ts"] = ensure_utc_datetime(completed_ts or utc_now())
    record["error_message"] = error_message
    return record


def build_model_metric_record(
    *,
    model_run_id: str,
    split_name: str,
    metric_name: str,
    metric_value: float | None,
    evaluated_row_count: int | None = None,
    evaluated_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build one Gold model metric record."""
    return {
        "model_run_id": model_run_id,
        "split_name": split_name,
        "metric_name": metric_name,
        "metric_value": metric_value,
        "evaluated_row_count": evaluated_row_count,
        "evaluated_ts": evaluated_ts or utc_now(),
    }


def build_recommendation_run_record(
    context: PipelineContext,
    *,
    methodology_version: str,
    eligible_team_count: int | None = None,
    primary_recommendation_count: int | None = None,
    alternate_recommendation_count: int | None = None,
    watchlist_count: int | None = None,
    status: str = "SUCCEEDED",
    recommendation_run_id: str | None = None,
    created_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build a Gold recommendation-run record."""
    return {
        "recommendation_run_id": recommendation_run_id or str(uuid4()),
        "pipeline_run_id": context.pipeline_run_id,
        "release_name": context.release_name,
        "analysis_as_of_date": context.analysis_as_of_date,
        "scoring_scenario": context.scoring_scenario,
        "methodology_version": methodology_version,
        "eligible_team_count": eligible_team_count,
        "primary_recommendation_count": primary_recommendation_count,
        "alternate_recommendation_count": alternate_recommendation_count,
        "watchlist_count": watchlist_count,
        "status": status,
        "created_ts": created_ts or utc_now(),
    }


def calculate_record_hash(record: dict[str, Any], include_keys: list[str] | None = None) -> str:
    """Calculate a deterministic hash for a record or selected business fields."""
    keys = include_keys or sorted(record.keys())
    canonical = "|".join(f"{key}={record.get(key)!r}" for key in keys)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _normalize_record_for_schema(
    table_fqn: str,
    table_schema: Any,
    record: dict[str, Any],
) -> dict[str, Any]:
    """Align one record to the live table schema and fail clearly on missing required fields."""
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
