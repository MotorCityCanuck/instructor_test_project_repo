"""Validate Bronze reconciliation against Raw source readiness."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.raw_to_bronze.cli import (
    add_config_path_argument,
    add_release_type_argument,
    add_run_id_argument,
    get_databricks_global,
    normalize_config_path,
    release_type_to_release_name,
    set_task_value,
)
from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import ensure_release_environment
from napa_pipeline.raw_to_bronze.inventory import validate_raw_inventory_and_readiness
from napa_pipeline.raw_to_bronze.operations import (
    RECONCILIATION_RESULTS_TABLE,
    RUN_MESSAGES_TABLE,
    append_records,
    build_reconciliation_record,
    build_run_message_record,
    create_pipeline_context,
    ensure_operations_tables,
)
from napa_pipeline.raw_to_bronze.reconciliation import (
    ReconciliationError,
    reconcile_bronze_table,
)


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for Bronze reconciliation."""
    parser = argparse.ArgumentParser(
        description="Validate Bronze row and schema reconciliation for a release."
    )
    add_release_type_argument(parser)
    add_run_id_argument(parser)
    add_config_path_argument(parser)
    return parser.parse_args()


def main() -> None:
    """Reconcile every configured Bronze table for the selected release."""
    args = parse_args()
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")

    release_name = release_type_to_release_name(args.release_type)
    config = load_raw_to_bronze_config(
        release_name,
        config_root=normalize_config_path(args.config_path),
    )
    environment_status = ensure_release_environment(
        spark,
        config,
        create_missing=False,
    )
    environment = environment_status.release_environment
    context = create_pipeline_context(config, environment, pipeline_run_id=args.run_id)
    ensure_operations_tables(spark, context)

    validation_result = validate_raw_inventory_and_readiness(
        spark,
        dbutils,
        config,
        environment,
    )
    source_readiness_by_name = {
        record.source_name: record for record in validation_result.source_readiness
    }

    reconciliation_rows = []
    message_rows = []
    mismatch_count = 0

    for source_config in config.sources_in_build_order:
        source_name = source_config["source_name"]
        source_readiness = source_readiness_by_name[source_name]
        source_has_mismatch = False

        try:
            result = reconcile_bronze_table(
                spark,
                environment,
                source_config,
                source_readiness,
            )
        except ReconciliationError as exc:
            append_records(
                spark,
                f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
                [
                    build_run_message_record(
                        context,
                        message_level="ERROR",
                        message_code="SCHEMA_RECONCILIATION_FAILED",
                        message_text=str(exc),
                        source_name=source_name,
                    )
                ],
            )
            raise

        reconciliation_rows.append(
            build_reconciliation_record(
                context,
                source_file_name=result.source_file_name,
                bronze_table=result.bronze_table,
                raw_row_count=result.raw_row_count,
                bronze_row_count=result.bronze_row_count,
                raw_business_column_count=result.raw_business_column_count,
                bronze_business_column_count=result.bronze_business_column_count,
                metadata_column_count=result.metadata_column_count,
                status=result.status,
            )
        )

        if result.row_count_difference != 0:
            source_has_mismatch = True
            message_rows.append(
                build_run_message_record(
                    context,
                    message_level="ERROR",
                    message_code="ROW_COUNT_MISMATCH",
                    message_text=(
                        f"Raw row count {result.raw_row_count} does not match Bronze "
                        f"row count {result.bronze_row_count} for "
                        f"{result.target_table_fqn}."
                    ),
                    source_name=source_name,
                )
            )

        if (
            result.missing_metadata_columns
            or result.missing_business_columns
            or result.unexpected_business_columns
        ):
            source_has_mismatch = True
            schema_issues = []
            if result.missing_metadata_columns:
                schema_issues.append(
                    "missing metadata columns: "
                    f"{', '.join(result.missing_metadata_columns)}"
                )
            if result.missing_business_columns:
                schema_issues.append(
                    "missing business columns: "
                    f"{', '.join(result.missing_business_columns)}"
                )
            if result.unexpected_business_columns:
                schema_issues.append(
                    "unexpected business columns: "
                    f"{', '.join(result.unexpected_business_columns)}"
                )
            message_rows.append(
                build_run_message_record(
                    context,
                    message_level="ERROR",
                    message_code="SCHEMA_RECONCILIATION_FAILED",
                    message_text=(
                        f"Schema reconciliation failed for {result.target_table_fqn}: "
                        f"{'; '.join(schema_issues)}."
                    ),
                    source_name=source_name,
                )
            )

        if source_has_mismatch:
            mismatch_count += 1

    append_records(
        spark,
        f"{context.operations_schema_fqn}.{RECONCILIATION_RESULTS_TABLE}",
        reconciliation_rows,
    )

    summary_code = (
        "PIPELINE_SUCCEEDED" if mismatch_count == 0 else "SCHEMA_RECONCILIATION_FAILED"
    )
    summary_text = (
        f"Bronze reconciliation succeeded for {len(reconciliation_rows)} sources."
        if mismatch_count == 0
        else f"Bronze reconciliation found {mismatch_count} mismatches across "
        f"{len(reconciliation_rows)} sources."
    )
    message_rows.append(
        build_run_message_record(
            context,
            message_level="INFO" if mismatch_count == 0 else "ERROR",
            message_code=summary_code,
            message_text=summary_text,
        )
    )
    append_records(
        spark,
        f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
        message_rows,
    )

    if mismatch_count != 0:
        raise RuntimeError(summary_text)

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release type: {args.release_type}")
    print(f"Release name: {context.release_name}")
    print(f"Run ID: {context.pipeline_run_id}")
    print(f"Bronze schema: {environment.bronze_schema}")
    print(f"Reconciled source count: {len(reconciliation_rows)}")
    print("Reconciliation mismatches detected: <none>")

    set_task_value(dbutils, "run_id", context.pipeline_run_id)
    set_task_value(dbutils, "reconciled_source_count", len(reconciliation_rows))


if __name__ == "__main__":
    main()
