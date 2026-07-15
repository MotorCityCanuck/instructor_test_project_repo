"""Operations helpers for Raw-to-Bronze audit tables and records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from typing import Any
from uuid import uuid4

from napa_pipeline.raw_to_bronze.config import RawToBronzeConfig
from napa_pipeline.raw_to_bronze.environment import ReleaseEnvironment


PIPELINE_RUNS_TABLE = "pipeline_runs"
TABLE_RUNS_TABLE = "table_runs"
SCHEMA_SNAPSHOTS_TABLE = "schema_snapshots"
RECONCILIATION_RESULTS_TABLE = "reconciliation_results"
RUN_MESSAGES_TABLE = "run_messages"


def utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class PipelineContext:
    """Common context fields for Raw-to-Bronze operations records."""

    pipeline_run_id: str
    pipeline_name: str
    pipeline_version: str
    release_name: str
    processing_mode: str
    configuration_hash: str
    operations_schema_fqn: str


def create_pipeline_context(
    config: RawToBronzeConfig,
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
    pipeline_name STRING NOT NULL,
    release_name STRING NOT NULL,
    source_file_name STRING NOT NULL,
    source_table STRING NOT NULL,
    target_table STRING NOT NULL,
    status STRING NOT NULL,
    started_ts TIMESTAMP NOT NULL,
    completed_ts TIMESTAMP,
    duration_seconds DOUBLE,
    source_file_size BIGINT,
    source_row_count BIGINT,
    bronze_row_count BIGINT,
    row_count_difference BIGINT,
    source_schema_hash STRING,
    bronze_schema_hash STRING,
    error_message STRING
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{SCHEMA_SNAPSHOTS_TABLE} (
    pipeline_run_id STRING NOT NULL,
    pipeline_name STRING NOT NULL,
    release_name STRING NOT NULL,
    layer_name STRING NOT NULL,
    object_name STRING NOT NULL,
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
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{RECONCILIATION_RESULTS_TABLE} (
    pipeline_run_id STRING NOT NULL,
    pipeline_name STRING NOT NULL,
    release_name STRING NOT NULL,
    source_file_name STRING NOT NULL,
    bronze_table STRING NOT NULL,
    raw_row_count BIGINT NOT NULL,
    bronze_row_count BIGINT NOT NULL,
    row_count_difference BIGINT NOT NULL,
    raw_business_column_count INT NOT NULL,
    bronze_business_column_count INT NOT NULL,
    metadata_column_count INT NOT NULL,
    status STRING NOT NULL,
    evaluated_ts TIMESTAMP NOT NULL
)
USING DELTA
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {operations_schema_fqn}.{RUN_MESSAGES_TABLE} (
    pipeline_run_id STRING NOT NULL,
    pipeline_name STRING NOT NULL,
    release_name STRING NOT NULL,
    source_name STRING,
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
    started_ts: datetime,
    status: str,
    completed_ts: datetime | None = None,
    error_class: str | None = None,
    error_message: str | None = None,
    workflow_run_id: str | None = None,
    triggered_by: str | None = None,
) -> dict[str, Any]:
    """Build the pipeline completion record."""
    completed = completed_ts or utc_now()
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "pipeline_name": context.pipeline_name,
        "pipeline_version": context.pipeline_version,
        "release_name": context.release_name,
        "processing_mode": context.processing_mode,
        "configuration_hash": context.configuration_hash,
        "workflow_run_id": workflow_run_id,
        "status": status,
        "started_ts": started_ts,
        "completed_ts": completed,
        "duration_seconds": (completed - started_ts).total_seconds(),
        "triggered_by": triggered_by,
        "error_class": error_class,
        "error_message": error_message,
    }


def build_table_run_start_record(
    context: PipelineContext,
    source_file_name: str,
    source_table: str,
    target_table: str,
    started_ts: datetime | None = None,
    source_file_size: int | None = None,
) -> dict[str, Any]:
    """Build the table run start record."""
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "pipeline_name": context.pipeline_name,
        "release_name": context.release_name,
        "source_file_name": source_file_name,
        "source_table": source_table,
        "target_table": target_table,
        "status": "RUNNING",
        "started_ts": started_ts or utc_now(),
        "completed_ts": None,
        "duration_seconds": None,
        "source_file_size": source_file_size,
        "source_row_count": None,
        "bronze_row_count": None,
        "row_count_difference": None,
        "source_schema_hash": None,
        "bronze_schema_hash": None,
        "error_message": None,
    }


def build_table_run_end_record(
    context: PipelineContext,
    source_file_name: str,
    source_table: str,
    target_table: str,
    started_ts: datetime,
    status: str,
    source_row_count: int | None = None,
    bronze_row_count: int | None = None,
    source_schema_hash: str | None = None,
    bronze_schema_hash: str | None = None,
    source_file_size: int | None = None,
    error_message: str | None = None,
    completed_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build the table run completion record."""
    completed = completed_ts or utc_now()
    row_count_difference = None
    if source_row_count is not None and bronze_row_count is not None:
        row_count_difference = bronze_row_count - source_row_count

    return {
        "pipeline_run_id": context.pipeline_run_id,
        "pipeline_name": context.pipeline_name,
        "release_name": context.release_name,
        "source_file_name": source_file_name,
        "source_table": source_table,
        "target_table": target_table,
        "status": status,
        "started_ts": started_ts,
        "completed_ts": completed,
        "duration_seconds": (completed - started_ts).total_seconds(),
        "source_file_size": source_file_size,
        "source_row_count": source_row_count,
        "bronze_row_count": bronze_row_count,
        "row_count_difference": row_count_difference,
        "source_schema_hash": source_schema_hash,
        "bronze_schema_hash": bronze_schema_hash,
        "error_message": error_message,
    }


def build_schema_snapshot_records(
    context: PipelineContext,
    layer_name: str,
    object_name: str,
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
                "pipeline_name": context.pipeline_name,
                "release_name": context.release_name,
                "layer_name": layer_name,
                "object_name": object_name,
                "column_name": field["column_name"],
                "data_type": field["data_type"],
                "nullable": field.get("nullable"),
                "ordinal_position": ordinal_position,
                "schema_hash": schema_hash,
                "captured_ts": captured,
            }
        )
    return records


def build_reconciliation_record(
    context: PipelineContext,
    source_file_name: str,
    bronze_table: str,
    raw_row_count: int,
    bronze_row_count: int,
    raw_business_column_count: int,
    bronze_business_column_count: int,
    metadata_column_count: int,
    evaluated_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build a reconciliation result record."""
    row_count_difference = bronze_row_count - raw_row_count
    status = "MATCHED" if row_count_difference == 0 else "MISMATCH"
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "pipeline_name": context.pipeline_name,
        "release_name": context.release_name,
        "source_file_name": source_file_name,
        "bronze_table": bronze_table,
        "raw_row_count": raw_row_count,
        "bronze_row_count": bronze_row_count,
        "row_count_difference": row_count_difference,
        "raw_business_column_count": raw_business_column_count,
        "bronze_business_column_count": bronze_business_column_count,
        "metadata_column_count": metadata_column_count,
        "status": status,
        "evaluated_ts": evaluated_ts or utc_now(),
    }


def build_run_message_record(
    context: PipelineContext,
    message_level: str,
    message_code: str,
    message_text: str,
    source_name: str | None = None,
    created_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build a durable run message record."""
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "pipeline_name": context.pipeline_name,
        "release_name": context.release_name,
        "source_name": source_name,
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
