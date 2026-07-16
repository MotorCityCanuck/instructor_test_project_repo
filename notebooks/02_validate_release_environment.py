"""Validate or create the Raw-to-Bronze release environment."""

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
from napa_pipeline.raw_to_bronze.operations import (
    PIPELINE_RUNS_TABLE,
    RUN_MESSAGES_TABLE,
    append_records,
    build_pipeline_run_start_record,
    build_run_message_record,
    create_pipeline_context,
    ensure_operations_tables,
    utc_now,
)


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for environment validation."""
    parser = argparse.ArgumentParser(
        description="Validate the Raw-to-Bronze Databricks environment."
    )
    add_release_type_argument(parser)
    add_run_id_argument(parser)
    add_config_path_argument(parser)
    return parser.parse_args()


def main() -> None:
    """Validate the selected release environment and create the run record."""
    args = parse_args()
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")

    release_name = release_type_to_release_name(args.release_type)
    pipeline_started_ts = utc_now()
    config = load_raw_to_bronze_config(
        release_name,
        config_root=normalize_config_path(args.config_path),
    )
    status = ensure_release_environment(spark, config, create_missing=True)
    environment = status.release_environment
    context = create_pipeline_context(config, environment, pipeline_run_id=args.run_id)

    ensure_operations_tables(spark, context)
    append_records(
        spark,
        f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}",
        [build_pipeline_run_start_record(context, started_ts=pipeline_started_ts)],
    )
    append_records(
        spark,
        f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
        [
            build_run_message_record(
                context,
                message_level="INFO",
                message_code="CONFIG_LOADED",
                message_text=(
                    f"Resolved configuration hash {config.config_hash} for release "
                    f"{config.release_name}."
                ),
            ),
            build_run_message_record(
                context,
                message_level="INFO",
                message_code="ENVIRONMENT_VALIDATED",
                message_text="Release environment validation completed successfully.",
            ),
        ],
    )

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release type: {args.release_type}")
    print(f"Release name: {context.release_name}")
    print(f"Run ID: {context.pipeline_run_id}")
    print(f"Catalog: {environment.catalog}")
    print(f"Raw schema: {environment.raw_schema}")
    print(f"Bronze schema: {environment.bronze_schema}")
    print(f"Operations schema: {environment.operations_schema}")
    print(f"Raw volume: {environment.raw_volume_fqn}")
    print(f"Raw volume path: {environment.raw_volume_path}")

    for schema_status in status.schema_statuses:
        state = "already existed" if schema_status.existed else "created"
        print(f"Schema {state}: {schema_status.object_name}")

    volume_state = "already existed" if status.volume_status.existed else "created"
    print(f"Volume {volume_state}: {status.volume_status.object_name}")

    set_task_value(dbutils, "run_id", context.pipeline_run_id)
    set_task_value(dbutils, "operations_schema_fqn", context.operations_schema_fqn)


if __name__ == "__main__":
    main()
