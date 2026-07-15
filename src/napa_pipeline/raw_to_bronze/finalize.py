"""Pipeline finalization helpers for the Raw-to-Bronze workflow."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from napa_pipeline.raw_to_bronze.operations import (
    PIPELINE_RUNS_TABLE,
    RECONCILIATION_RESULTS_TABLE,
    TABLE_RUNS_TABLE,
    PipelineContext,
    append_records,
    build_pipeline_run_end_record,
    get_operations_table_fqn,
    utc_now,
)


@dataclass(frozen=True)
class PipelineRunSummary:
    """Aggregate summary for one pipeline run."""

    expected_source_count: int
    completed_table_run_count: int
    failed_table_run_count: int
    reconciliation_result_count: int
    mismatched_reconciliation_count: int
    final_status: str
    summary_text: str


class PipelineFinalizationError(RuntimeError):
    """Raised when pipeline finalization cannot be completed."""


def summarize_pipeline_run(
    spark: Any,
    context: PipelineContext,
    expected_source_count: int,
) -> PipelineRunSummary:
    """Summarize table-run and reconciliation outcomes for one pipeline run."""
    table_run_rows = [
        row
        for row in _collect_table_rows(
            spark,
            get_operations_table_fqn(context, TABLE_RUNS_TABLE),
        )
        if row.get("pipeline_run_id") == context.pipeline_run_id
        and row.get("completed_ts") is not None
    ]
    reconciliation_rows = [
        row
        for row in _collect_table_rows(
            spark,
            get_operations_table_fqn(context, RECONCILIATION_RESULTS_TABLE),
        )
        if row.get("pipeline_run_id") == context.pipeline_run_id
    ]

    failed_table_run_count = sum(
        1 for row in table_run_rows if row.get("status") != "SUCCEEDED"
    )
    mismatched_reconciliation_count = sum(
        1 for row in reconciliation_rows if row.get("status") != "MATCHED"
    )

    final_status = "SUCCEEDED"
    if (
        len(table_run_rows) != expected_source_count
        or failed_table_run_count != 0
        or len(reconciliation_rows) != expected_source_count
        or mismatched_reconciliation_count != 0
    ):
        final_status = "FAILED"

    summary_text = (
        f"Pipeline succeeded for {expected_source_count} sources."
        if final_status == "SUCCEEDED"
        else (
            "Pipeline failed: "
            f"completed_table_runs={len(table_run_rows)}/{expected_source_count}, "
            f"failed_table_runs={failed_table_run_count}, "
            f"reconciliation_results={len(reconciliation_rows)}/{expected_source_count}, "
            f"mismatched_reconciliations={mismatched_reconciliation_count}."
        )
    )

    return PipelineRunSummary(
        expected_source_count=expected_source_count,
        completed_table_run_count=len(table_run_rows),
        failed_table_run_count=failed_table_run_count,
        reconciliation_result_count=len(reconciliation_rows),
        mismatched_reconciliation_count=mismatched_reconciliation_count,
        final_status=final_status,
        summary_text=summary_text,
    )


def finalize_pipeline_run(
    spark: Any,
    context: PipelineContext,
    summary: PipelineRunSummary,
) -> None:
    """Finalize the durable pipeline run record in the operations table."""
    pipeline_runs_fqn = get_operations_table_fqn(context, PIPELINE_RUNS_TABLE)
    pipeline_run_rows = [
        row
        for row in _collect_table_rows(spark, pipeline_runs_fqn)
        if row.get("pipeline_run_id") == context.pipeline_run_id
    ]
    open_row = next(
        (row for row in pipeline_run_rows if row.get("completed_ts") is None),
        None,
    )

    error_class = None
    error_message = None
    if summary.final_status != "SUCCEEDED":
        error_class = "PipelineRunFailed"
        error_message = summary.summary_text

    if open_row is None:
        append_records(
            spark,
            pipeline_runs_fqn,
            [
                build_pipeline_run_end_record(
                    context,
                    started_ts=utc_now(),
                    status=summary.final_status,
                    error_class=error_class,
                    error_message=error_message,
                )
            ],
        )
        return

    final_record = build_pipeline_run_end_record(
        context,
        started_ts=open_row["started_ts"],
        status=summary.final_status,
        error_class=error_class,
        error_message=error_message,
        workflow_run_id=open_row.get("workflow_run_id"),
        triggered_by=open_row.get("triggered_by"),
    )
    _merge_pipeline_run_record(spark, pipeline_runs_fqn, final_record)


def _collect_table_rows(spark: Any, table_fqn: str) -> list[dict[str, Any]]:
    rows = spark.table(table_fqn).collect()
    return [
        row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        for row in rows
    ]


def _merge_pipeline_run_record(
    spark: Any,
    table_fqn: str,
    record: dict[str, Any],
) -> None:
    temp_view_name = f"_pipeline_run_update_{uuid4().hex}"
    try:
        schema = spark.table(table_fqn).schema
        update_df = spark.createDataFrame([record], schema=schema)
        update_df.createOrReplaceTempView(temp_view_name)
        spark.sql(
            f"""
MERGE INTO {table_fqn} AS target
USING {temp_view_name} AS source
ON target.pipeline_run_id = source.pipeline_run_id
   AND target.completed_ts IS NULL
WHEN MATCHED THEN UPDATE SET
  target.pipeline_name = source.pipeline_name,
  target.pipeline_version = source.pipeline_version,
  target.release_name = source.release_name,
  target.processing_mode = source.processing_mode,
  target.configuration_hash = source.configuration_hash,
  target.workflow_run_id = source.workflow_run_id,
  target.status = source.status,
  target.started_ts = source.started_ts,
  target.completed_ts = source.completed_ts,
  target.duration_seconds = source.duration_seconds,
  target.triggered_by = source.triggered_by,
  target.error_class = source.error_class,
  target.error_message = source.error_message
WHEN NOT MATCHED THEN INSERT *
""".strip()
        )
    except Exception as exc:
        raise PipelineFinalizationError(
            f"Could not finalize pipeline run {record['pipeline_run_id']}."
        ) from exc
    finally:
        try:
            spark.catalog.dropTempView(temp_view_name)
        except Exception:
            pass
