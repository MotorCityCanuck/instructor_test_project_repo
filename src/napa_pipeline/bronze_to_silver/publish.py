"""Spark publication helpers for Bronze-to-Silver execution."""

from __future__ import annotations

from uuid import uuid4
from typing import Any

from napa_pipeline.bronze_to_silver.metadata import utc_now
from napa_pipeline.bronze_to_silver.operations import (
    PipelineContext,
    QUALITY_RESULTS_TABLE,
    RUN_MESSAGES_TABLE,
    SCHEMA_SNAPSHOTS_TABLE,
    append_records,
    build_quality_result_record,
    build_run_message_record,
    build_schema_snapshot_records,
)


class PublicationError(RuntimeError):
    """Raised when Bronze-to-Silver publication cannot complete."""


def collect_table_rows(spark: Any, table_fqn: str) -> list[dict[str, Any]]:
    """Collect a table into Python dict rows for the current reference builders."""
    return [
        row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        for row in spark.table(table_fqn).toLocalIterator()
    ]


def publish_records_to_table(
    spark: Any,
    table_fqn: str,
    records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    mode: str = "overwrite",
) -> int:
    """Publish records to a Delta table and return the published row count."""
    materialized = list(records)
    if not materialized:
        if not spark.catalog.tableExists(table_fqn):
            return 0
        schema = spark.table(table_fqn).schema
        spark.createDataFrame([], schema=schema).write.format("delta").mode(mode).option(
            "overwriteSchema", "true"
        ).saveAsTable(table_fqn)
        return 0

    spark.createDataFrame(materialized).write.format("delta").mode(mode).option(
        "overwriteSchema", "true"
    ).saveAsTable(table_fqn)
    return len(materialized)


def publish_records_to_view(
    spark: Any,
    view_fqn: str,
    records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> int:
    """Publish records to a SQL view and return the exposed row count."""
    materialized = list(records)
    if not materialized and not _view_exists(spark, view_fqn):
        return 0

    temp_view_name = f"_b2s_publish_{uuid4().hex}"
    try:
        if materialized:
            spark.createDataFrame(materialized).createOrReplaceTempView(temp_view_name)
        else:
            schema = spark.table(view_fqn).schema
            spark.createDataFrame([], schema=schema).createOrReplaceTempView(temp_view_name)
        spark.sql(f"CREATE OR REPLACE VIEW {view_fqn} AS SELECT * FROM {temp_view_name}")
    except Exception as exc:
        raise PublicationError(f"Could not publish view {view_fqn}.") from exc
    finally:
        try:
            spark.catalog.dropTempView(temp_view_name)
        except Exception:
            pass
    return len(materialized)


def publish_sql_view(
    spark: Any,
    view_fqn: str,
    select_sql: str,
) -> int:
    """Publish a SQL-backed view and return its row count when available."""
    try:
        spark.sql(f"CREATE OR REPLACE VIEW {view_fqn} AS {select_sql}")
        try:
            return int(spark.table(view_fqn).count())
        except Exception:
            return 0
    except Exception as exc:
        raise PublicationError(f"Could not publish view {view_fqn}.") from exc


def append_quality_results_for_rejects(
    spark: Any,
    context: PipelineContext,
    *,
    target_table: str,
    evaluated_row_count: int,
    rejected_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> list[dict[str, Any]]:
    """Group reject rows into quality-results records and append them."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rejected_rows:
        grouped.setdefault((str(row["rule_id"]), str(row["rule_severity"])), []).append(row)

    quality_records: list[dict[str, Any]] = []
    for (rule_id, severity), rows in sorted(grouped.items()):
        failed_row_count = len(rows)
        failure_pct = (
            (failed_row_count / evaluated_row_count) * 100.0
            if evaluated_row_count
            else None
        )
        quality_records.append(
            build_quality_result_record(
                context,
                target_table=target_table,
                rule_id=rule_id,
                rule_type="rejects",
                severity=severity,
                status="FAILED",
                evaluated_row_count=evaluated_row_count,
                failed_row_count=failed_row_count,
                failure_pct=failure_pct,
                sample_business_keys=[str(row["source_business_key"]) for row in rows[:10]],
                evaluated_ts=utc_now(),
            )
        )

    append_records(
        spark,
        f"{context.operations_schema_fqn}.{QUALITY_RESULTS_TABLE}",
        quality_records,
    )
    return quality_records


def append_schema_snapshot_for_table(
    spark: Any,
    context: PipelineContext,
    *,
    layer_name: str,
    table_name: str,
    table_fqn: str,
) -> None:
    """Capture and append schema snapshot rows for a published table."""
    if not spark.catalog.tableExists(table_fqn):
        return
    schema = spark.table(table_fqn).schema
    schema_fields = [
        {
            "column_name": field.name,
            "data_type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in schema.fields
    ]
    append_records(
        spark,
        f"{context.operations_schema_fqn}.{SCHEMA_SNAPSHOTS_TABLE}",
        build_schema_snapshot_records(
            context,
            layer_name=layer_name,
            table_name=table_name,
            schema_fields=schema_fields,
        ),
    )


def append_warning_message(
    spark: Any,
    context: PipelineContext,
    *,
    target_table: str,
    message_code: str,
    message_text: str,
) -> None:
    """Append a warning message to the durable run_messages table."""
    append_records(
        spark,
        f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
        [
            build_run_message_record(
                context,
                message_level="WARNING",
                message_code=message_code,
                message_text=message_text,
                target_table=target_table,
            )
        ],
    )


def _view_exists(spark: Any, view_fqn: str) -> bool:
    try:
        spark.table(view_fqn)
        return True
    except Exception:
        return False
