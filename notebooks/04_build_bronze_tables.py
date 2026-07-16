"""Build Bronze Delta tables from validated Raw Parquet sources."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.raw_to_bronze.bronze import (
    BronzePublicationError,
    build_bronze_table,
    get_bronze_target_table_fqn,
)
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
    RUN_MESSAGES_TABLE,
    SCHEMA_SNAPSHOTS_TABLE,
    TABLE_RUNS_TABLE,
    append_records,
    build_run_message_record,
    build_schema_snapshot_records,
    build_table_run_end_record,
    build_table_run_start_record,
    create_pipeline_context,
    ensure_operations_tables,
    utc_now,
)


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for Bronze table publication."""
    parser = argparse.ArgumentParser(
        description="Build Raw-to-Bronze Delta tables for a release."
    )
    add_release_type_argument(parser)
    add_run_id_argument(parser)
    add_config_path_argument(parser)
    return parser.parse_args()


def main() -> None:
    """Build all configured Bronze tables for the selected release."""
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

    append_records(
        spark,
        f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
        [
            build_run_message_record(
                context,
                message_level="INFO",
                message_code="BRONZE_WRITE_STARTED",
                message_text=(
                    "Starting Bronze publication for "
                    f"{len(source_readiness_by_name)} configured sources."
                ),
            )
        ],
    )

    build_results = []
    for source_config in config.sources_in_build_order:
        source_name = source_config["source_name"]
        source_readiness = source_readiness_by_name[source_name]
        target_table_fqn = get_bronze_target_table_fqn(environment, source_config)
        table_started_ts = utc_now()

        append_records(
            spark,
            f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}",
            [
                build_table_run_start_record(
                    context,
                    source_file_name=source_readiness.file_name,
                    source_table=source_name,
                    target_table=target_table_fqn,
                    started_ts=table_started_ts,
                    source_file_size=source_readiness.file_size,
                )
            ],
        )
        append_records(
            spark,
            f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
            [
                build_run_message_record(
                    context,
                    message_level="INFO",
                    message_code="BRONZE_WRITE_STARTED",
                    message_text=(
                        f"Starting Bronze publication for {source_name} "
                        f"from {source_readiness.file_name}."
                    ),
                    source_name=source_name,
                )
            ],
        )

        try:
            result = build_bronze_table(
                spark,
                config,
                context,
                environment,
                source_config,
                source_readiness,
                ingested_ts=table_started_ts,
            )
            build_results.append(result)

            append_records(
                spark,
                f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}",
                [
                    build_table_run_end_record(
                        context,
                        source_file_name=result.source_file_name,
                        source_table=source_name,
                        target_table=result.target_table_fqn,
                        started_ts=table_started_ts,
                        status="SUCCEEDED",
                        source_row_count=result.source_row_count,
                        bronze_row_count=result.bronze_row_count,
                        source_schema_hash=result.source_schema_hash,
                        bronze_schema_hash=result.bronze_schema_hash,
                        source_file_size=result.source_file_size,
                    )
                ],
            )
            append_records(
                spark,
                f"{context.operations_schema_fqn}.{SCHEMA_SNAPSHOTS_TABLE}",
                build_schema_snapshot_records(
                    context,
                    layer_name="bronze",
                    object_name=source_config["bronze_table"],
                    schema_fields=list(result.bronze_schema_fields),
                ),
            )
            append_records(
                spark,
                f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
                [
                    build_run_message_record(
                        context,
                        message_level="INFO",
                        message_code="BRONZE_WRITE_COMPLETED",
                        message_text=(
                            f"Published Bronze table {result.target_table_fqn} with "
                            f"{result.bronze_row_count} rows."
                        ),
                        source_name=source_name,
                    )
                ],
            )
        except BronzePublicationError as exc:
            append_records(
                spark,
                f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}",
                [
                    build_table_run_end_record(
                        context,
                        source_file_name=source_readiness.file_name,
                        source_table=source_name,
                        target_table=target_table_fqn,
                        started_ts=table_started_ts,
                        status="FAILED",
                        source_row_count=source_readiness.row_count,
                        source_schema_hash=source_readiness.schema_hash,
                        source_file_size=source_readiness.file_size,
                        error_message=str(exc),
                    )
                ],
            )
            append_records(
                spark,
                f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
                [
                    build_run_message_record(
                        context,
                        message_level="ERROR",
                        message_code="BRONZE_WRITE_FAILED",
                        message_text=str(exc),
                        source_name=source_name,
                    )
                ],
            )
            raise

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release type: {args.release_type}")
    print(f"Release name: {context.release_name}")
    print(f"Run ID: {context.pipeline_run_id}")
    print(f"Bronze schema: {environment.bronze_schema}")
    print(f"Published Bronze table count: {len(build_results)}")

    set_task_value(dbutils, "run_id", context.pipeline_run_id)
    set_task_value(dbutils, "bronze_table_count", len(build_results))


if __name__ == "__main__":
    main()
