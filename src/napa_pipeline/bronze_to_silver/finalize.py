"""Pipeline finalization helpers for the Bronze-to-Silver workflow."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from napa_pipeline.bronze_to_silver.operations import (
    PIPELINE_RUNS_TABLE,
    QUALITY_RESULTS_TABLE,
    RECONCILIATION_RESULTS_TABLE,
    RUN_MESSAGES_TABLE,
    TABLE_RUNS_TABLE,
    PipelineContext,
    append_records,
    build_pipeline_run_end_record,
    get_operations_table_fqn,
    utc_now,
)


@dataclass(frozen=True)
class PipelineRunSummary:
    """Aggregate summary for one Bronze-to-Silver pipeline run."""

    expected_table_count: int
    completed_table_run_count: int
    failed_table_run_count: int
    reconciliation_result_count: int
    mismatched_reconciliation_count: int
    critical_quality_failure_count: int
    final_status: str
    summary_text: str


class PipelineFinalizationError(RuntimeError):
    """Raised when pipeline finalization cannot be completed."""


def summarize_pipeline_run(
    spark: Any,
    context: PipelineContext,
    expected_table_count: int,
    expected_table_names: list[str] | None = None,
) -> PipelineRunSummary:
    """Summarize published Bronze-to-Silver outcomes for one pipeline run."""
    pipeline_run_rows = [
        row
        for row in _collect_table_rows(
            spark,
            get_operations_table_fqn(context, PIPELINE_RUNS_TABLE),
        )
        if row.get("pipeline_run_id") == context.pipeline_run_id
    ]
    run_message_rows = [
        row
        for row in _collect_table_rows(
            spark,
            get_operations_table_fqn(context, RUN_MESSAGES_TABLE),
        )
        if row.get("pipeline_run_id") == context.pipeline_run_id
    ]
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
    quality_rows = [
        row
        for row in _collect_table_rows(
            spark,
            get_operations_table_fqn(context, QUALITY_RESULTS_TABLE),
        )
        if row.get("pipeline_run_id") == context.pipeline_run_id
    ]

    failed_table_run_count = sum(
        1 for row in table_run_rows if row.get("status") != "SUCCEEDED"
    )
    mismatched_reconciliation_count = sum(
        1 for row in reconciliation_rows if row.get("status") != "PASSED"
    )
    critical_quality_failure_count = sum(
        1
        for row in quality_rows
        if row.get("severity") == "CRITICAL" and (row.get("failed_row_count") or 0) > 0
    )

    if not table_run_rows and not reconciliation_rows and not pipeline_run_rows:
        raise PipelineFinalizationError(
            "No pipeline_runs, table_runs, or reconciliation_results were found for "
            f"pipeline_run_id {context.pipeline_run_id}."
        )

    final_status = "SUCCEEDED"
    if (
        len(table_run_rows) != expected_table_count
        or failed_table_run_count != 0
        or len(reconciliation_rows) != expected_table_count
        or mismatched_reconciliation_count != 0
        or critical_quality_failure_count != 0
    ):
        final_status = "FAILED"

    summary_text = _build_summary_text(
        expected_table_count=expected_table_count,
        expected_table_names=expected_table_names,
        table_run_rows=table_run_rows,
        reconciliation_rows=reconciliation_rows,
        quality_rows=quality_rows,
        run_message_rows=run_message_rows,
        final_status=final_status,
        failed_table_run_count=failed_table_run_count,
        mismatched_reconciliation_count=mismatched_reconciliation_count,
        critical_quality_failure_count=critical_quality_failure_count,
    )

    return PipelineRunSummary(
        expected_table_count=expected_table_count,
        completed_table_run_count=len(table_run_rows),
        failed_table_run_count=failed_table_run_count,
        reconciliation_result_count=len(reconciliation_rows),
        mismatched_reconciliation_count=mismatched_reconciliation_count,
        critical_quality_failure_count=critical_quality_failure_count,
        final_status=final_status,
        summary_text=summary_text,
    )


def _build_summary_text(
    *,
    expected_table_count: int,
    expected_table_names: list[str] | None,
    table_run_rows: list[dict[str, Any]],
    reconciliation_rows: list[dict[str, Any]],
    quality_rows: list[dict[str, Any]],
    run_message_rows: list[dict[str, Any]],
    final_status: str,
    failed_table_run_count: int,
    mismatched_reconciliation_count: int,
    critical_quality_failure_count: int,
) -> str:
    if final_status == "SUCCEEDED":
        return f"Bronze-to-Silver pipeline succeeded for {expected_table_count} configured tables."

    details = [
        "Bronze-to-Silver pipeline failed: "
        f"completed_table_runs={len(table_run_rows)}/{expected_table_count}, "
        f"failed_table_runs={failed_table_run_count}, "
        f"reconciliation_results={len(reconciliation_rows)}/{expected_table_count}, "
        f"mismatched_reconciliations={mismatched_reconciliation_count}, "
        f"critical_quality_failures={critical_quality_failure_count}, "
        f"run_messages={len(run_message_rows)}."
    ]

    failed_table_details = [
        _format_failed_table_detail(row)
        for row in sorted(table_run_rows, key=lambda item: item.get("build_order") or 0)
        if row.get("status") != "SUCCEEDED"
    ]
    if failed_table_details:
        details.append("Failed table runs: " + "; ".join(failed_table_details) + ".")

    if expected_table_names:
        completed_targets = {
            str(row.get("target_table"))
            for row in table_run_rows
            if row.get("status") == "SUCCEEDED"
        }
        failed_targets = {
            str(row.get("target_table"))
            for row in table_run_rows
            if row.get("status") != "SUCCEEDED"
        }
        missing_targets = [
            table_name
            for table_name in expected_table_names
            if table_name not in completed_targets and table_name not in failed_targets
        ]
        if missing_targets:
            details.append("Tables not completed: " + ", ".join(missing_targets) + ".")

    critical_quality_details = [
        f"{row.get('target_table')}:{row.get('rule_id')} failed_row_count={row.get('failed_row_count')}"
        for row in quality_rows
        if row.get("severity") == "CRITICAL" and (row.get("failed_row_count") or 0) > 0
    ]
    if critical_quality_details:
        details.append("Critical quality failures: " + "; ".join(critical_quality_details[:10]) + ".")

    return " ".join(details)


def _format_failed_table_detail(row: dict[str, Any]) -> str:
    table_name = row.get("target_table") or "<unknown_table>"
    stage_name = row.get("build_stage") or "<unknown_stage>"
    error_message = str(row.get("error_message") or "").strip()
    if not error_message:
        return f"{stage_name}.{table_name}"
    return f"{stage_name}.{table_name}: {_truncate(error_message, 500)}"


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


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
    temp_view_name = f"_b2s_pipeline_run_update_{uuid4().hex}"
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
