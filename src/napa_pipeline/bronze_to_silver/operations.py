"""Operations helpers for Bronze-to-Silver audit tables and records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from typing import Any
from uuid import uuid4

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.environment import ReleaseEnvironment


PIPELINE_RUNS_TABLE = "pipeline_runs"
TABLE_RUNS_TABLE = "table_runs"
QUALITY_RESULTS_TABLE = "quality_results"
RECONCILIATION_RESULTS_TABLE = "reconciliation_results"
SCHEMA_SNAPSHOTS_TABLE = "schema_snapshots"
RUN_MESSAGES_TABLE = "run_messages"


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
    """Common context fields for Bronze-to-Silver operations records."""

    pipeline_run_id: str
    pipeline_name: str
    pipeline_version: str
    release_name: str
    processing_mode: str
    configuration_hash: str
    operations_schema_fqn: str


def create_pipeline_context(
    config: BronzeToSilverConfig,
    environment: ReleaseEnvironment,
    pipeline_run_id: str | None = None,
) -> PipelineContext:
    """Create a pipeline context from resolved configuration."""
    return PipelineContext(
        pipeline_run_id=pipeline_run_id or str(uuid4()),
        pipeline_name=str(config.data["project"]["pipeline_name"]),
        pipeline_version=str(config.data["project"]["pipeline_version"]),
        release_name=str(config.data["release"]["release_name"]),
        processing_mode=str(config.data["project"]["processing_mode"]),
        configuration_hash=config.config_hash,
        operations_schema_fqn=f"{environment.catalog}.{environment.operations_schema}",
    )


def ensure_operations_tables(spark: Any, context: PipelineContext) -> None:
    """Create operations tables in the configured shared operations schema."""
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
    """Return the DDL statements for all operations tables."""
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
    release_name STRING NOT NULL,
    source_table STRING NOT NULL,
    target_table STRING NOT NULL,
    build_stage STRING NOT NULL,
    build_order INT NOT NULL,
    status STRING NOT NULL,
    started_ts TIMESTAMP NOT NULL,
    completed_ts TIMESTAMP,
    duration_seconds DOUBLE,
    source_row_count BIGINT,
    exact_duplicate_count BIGINT,
    business_key_duplicate_count BIGINT,
    accepted_row_count BIGINT,
    rejected_row_count BIGINT,
    warning_count BIGINT,
    published_row_count BIGINT,
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
    rule_type STRING NOT NULL,
    severity STRING NOT NULL,
    evaluated_row_count BIGINT,
    failed_row_count BIGINT,
    failure_pct DOUBLE,
    threshold_value STRING,
    status STRING NOT NULL,
    sample_business_keys ARRAY<STRING>,
    evaluated_ts TIMESTAMP NOT NULL
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{RECONCILIATION_RESULTS_TABLE} (
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    source_table STRING NOT NULL,
    target_table STRING NOT NULL,
    bronze_row_count BIGINT NOT NULL,
    exact_duplicate_count BIGINT NOT NULL,
    business_key_loser_count BIGINT NOT NULL,
    rejected_row_count BIGINT NOT NULL,
    accepted_row_count BIGINT NOT NULL,
    reconciliation_difference BIGINT NOT NULL,
    status STRING NOT NULL,
    evaluated_ts TIMESTAMP NOT NULL
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{SCHEMA_SNAPSHOTS_TABLE} (
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    layer_name STRING NOT NULL,
    table_name STRING NOT NULL,
    column_name STRING NOT NULL,
    data_type STRING NOT NULL,
    nullable BOOLEAN,
    ordinal_position INT,
    schema_hash STRING NOT NULL,
    captured_ts TIMESTAMP NOT NULL
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{RUN_MESSAGES_TABLE} (
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    target_table STRING,
    message_level STRING NOT NULL,
    message_code STRING NOT NULL,
    message_text STRING NOT NULL,
    created_ts TIMESTAMP NOT NULL
)
USING DELTA
""".strip(),
    ]


def build_pipeline_run_start_record(
    context: PipelineContext,
    started_ts: datetime | None = None,
    workflow_run_id: str | None = None,
    triggered_by: str | None = None,
    status: str = "STARTED",
) -> dict[str, Any]:
    """Build the pipeline start record."""
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "pipeline_name": context.pipeline_name,
        "pipeline_version": context.pipeline_version,
        "release_name": context.release_name,
        "processing_mode": context.processing_mode,
        "configuration_hash": context.configuration_hash,
        "workflow_run_id": workflow_run_id,
        "status": status,
        "started_ts": started_ts or utc_now(),
        "completed_ts": None,
        "duration_seconds": None,
        "triggered_by": triggered_by,
        "error_class": None,
        "error_message": None,
    }


def build_pipeline_run_end_record(
    context: PipelineContext,
    started_ts: datetime,
    status: str,
    completed_ts: datetime | None = None,
    error_class: str | None = None,
    error_message: str | None = None,
    workflow_run_id: str | None = None,
    triggered_by: str | None = None,
) -> dict[str, Any]:
    """Build the pipeline completion record."""
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
    source_table: str,
    target_table: str,
    build_stage: str,
    build_order: int,
    started_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build the table run start record."""
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "release_name": context.release_name,
        "source_table": source_table,
        "target_table": target_table,
        "build_stage": build_stage,
        "build_order": build_order,
        "status": "RUNNING",
        "started_ts": started_ts or utc_now(),
        "completed_ts": None,
        "duration_seconds": None,
        "source_row_count": None,
        "exact_duplicate_count": None,
        "business_key_duplicate_count": None,
        "accepted_row_count": None,
        "rejected_row_count": None,
        "warning_count": None,
        "published_row_count": None,
        "error_message": None,
    }


def build_table_run_end_record(
    context: PipelineContext,
    source_table: str,
    target_table: str,
    build_stage: str,
    build_order: int,
    started_ts: datetime,
    status: str,
    source_row_count: int | None = None,
    exact_duplicate_count: int | None = None,
    business_key_duplicate_count: int | None = None,
    accepted_row_count: int | None = None,
    rejected_row_count: int | None = None,
    warning_count: int | None = None,
    published_row_count: int | None = None,
    error_message: str | None = None,
    completed_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build the table run completion record."""
    started = ensure_utc_datetime(started_ts)
    completed = ensure_utc_datetime(completed_ts or utc_now())
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "release_name": context.release_name,
        "source_table": source_table,
        "target_table": target_table,
        "build_stage": build_stage,
        "build_order": build_order,
        "status": status,
        "started_ts": started,
        "completed_ts": completed,
        "duration_seconds": (completed - started).total_seconds(),
        "source_row_count": source_row_count,
        "exact_duplicate_count": exact_duplicate_count,
        "business_key_duplicate_count": business_key_duplicate_count,
        "accepted_row_count": accepted_row_count,
        "rejected_row_count": rejected_row_count,
        "warning_count": warning_count,
        "published_row_count": published_row_count,
        "error_message": error_message,
    }


def build_quality_result_record(
    context: PipelineContext,
    target_table: str,
    rule_id: str,
    rule_type: str,
    severity: str,
    status: str,
    evaluated_row_count: int | None = None,
    failed_row_count: int | None = None,
    failure_pct: float | None = None,
    threshold_value: str | None = None,
    sample_business_keys: list[str] | None = None,
    evaluated_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build a quality result record."""
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "release_name": context.release_name,
        "target_table": target_table,
        "rule_id": rule_id,
        "rule_type": rule_type,
        "severity": severity,
        "evaluated_row_count": evaluated_row_count,
        "failed_row_count": failed_row_count,
        "failure_pct": failure_pct,
        "threshold_value": threshold_value,
        "status": status,
        "sample_business_keys": sample_business_keys,
        "evaluated_ts": evaluated_ts or utc_now(),
    }


def build_reconciliation_record(
    context: PipelineContext,
    source_table: str,
    target_table: str,
    bronze_row_count: int,
    exact_duplicate_count: int,
    business_key_loser_count: int,
    rejected_row_count: int,
    accepted_row_count: int,
    status: str | None = None,
    evaluated_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build a reconciliation result record."""
    reconciliation_difference = bronze_row_count - (
        exact_duplicate_count
        + business_key_loser_count
        + rejected_row_count
        + accepted_row_count
    )
    resolved_status = status or ("PASSED" if reconciliation_difference == 0 else "FAILED")
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "release_name": context.release_name,
        "source_table": source_table,
        "target_table": target_table,
        "bronze_row_count": bronze_row_count,
        "exact_duplicate_count": exact_duplicate_count,
        "business_key_loser_count": business_key_loser_count,
        "rejected_row_count": rejected_row_count,
        "accepted_row_count": accepted_row_count,
        "reconciliation_difference": reconciliation_difference,
        "status": resolved_status,
        "evaluated_ts": evaluated_ts or utc_now(),
    }


def build_schema_snapshot_records(
    context: PipelineContext,
    layer_name: str,
    table_name: str,
    schema_fields: list[dict[str, Any]],
    captured_ts: datetime | None = None,
) -> list[dict[str, Any]]:
    """Build schema snapshot records and a deterministic schema hash."""
    captured = captured_ts or utc_now()
    schema_hash = calculate_schema_hash(schema_fields)
    records = []
    for ordinal_position, field in enumerate(schema_fields, start=1):
        records.append(
            {
                "pipeline_run_id": context.pipeline_run_id,
                "release_name": context.release_name,
                "layer_name": layer_name,
                "table_name": table_name,
                "column_name": field["column_name"],
                "data_type": field["data_type"],
                "nullable": field.get("nullable"),
                "ordinal_position": ordinal_position,
                "schema_hash": schema_hash,
                "captured_ts": captured,
            }
        )
    return records


def build_run_message_record(
    context: PipelineContext,
    message_level: str,
    message_code: str,
    message_text: str,
    target_table: str | None = None,
    created_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build a durable run message record."""
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "release_name": context.release_name,
        "target_table": target_table,
        "message_level": message_level,
        "message_code": message_code,
        "message_text": message_text,
        "created_ts": created_ts or utc_now(),
    }


def calculate_schema_hash(schema_fields: list[dict[str, Any]]) -> str:
    """Calculate a deterministic hash for a schema field list."""
    canonical = "|".join(
        f"{field['column_name']}:{field['data_type']}:{field.get('nullable')}"
        for field in schema_fields
    )
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
