"""Validate exact Raw inventory and source readability."""

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
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.inventory import (
    RawInventoryError,
    validate_raw_inventory_and_readiness,
)
from napa_pipeline.raw_to_bronze.operations import (
    RUN_MESSAGES_TABLE,
    SCHEMA_SNAPSHOTS_TABLE,
    append_records,
    build_run_message_record,
    build_schema_snapshot_records,
    create_pipeline_context,
    ensure_operations_tables,
)


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for raw inventory validation."""
    parser = argparse.ArgumentParser(
        description="Validate Raw source inventory for a release."
    )
    add_release_type_argument(parser)
    add_run_id_argument(parser)
    add_config_path_argument(parser)
    return parser.parse_args()


def main() -> None:
    """Validate the raw volume inventory and record schema snapshots."""
    args = parse_args()
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")

    release_name = release_type_to_release_name(args.release_type)
    config = load_raw_to_bronze_config(
        release_name,
        config_root=normalize_config_path(args.config_path),
    )
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment, pipeline_run_id=args.run_id)
    ensure_operations_tables(spark, context)

    try:
        validation_result = validate_raw_inventory_and_readiness(
            spark,
            dbutils,
            config,
            environment,
        )
    except RawInventoryError:
        print(f"Release type: {args.release_type}")
        print(f"Release name: {context.release_name}")
        print(f"Raw volume path: {environment.raw_volume_path}")
        print("Direct listing from dbutils.fs.ls:")
        for entry in dbutils.fs.ls(environment.raw_volume_path):
            entry_name = str(getattr(entry, "name", "") or "").rstrip("/")
            print(f"- {entry_name} | {entry.path}")
        raise

    inventory_status = validation_result.inventory_status
    source_readiness = validation_result.source_readiness

    append_records(
        spark,
        f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
        [
            build_run_message_record(
                context,
                message_level="INFO",
                message_code="SOURCE_READ_COMPLETED",
                message_text=(
                    "Validated exact Raw inventory and readable Parquet contracts "
                    f"for {len(source_readiness)} configured sources."
                ),
            )
        ],
    )

    schema_snapshot_records = []
    for source in source_readiness:
        schema_snapshot_records.extend(
            build_schema_snapshot_records(
                context,
                layer_name="raw",
                object_name=source.file_name,
                schema_fields=list(source.schema_fields),
            )
        )

    append_records(
        spark,
        f"{context.operations_schema_fqn}.{SCHEMA_SNAPSHOTS_TABLE}",
        schema_snapshot_records,
    )

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release type: {args.release_type}")
    print(f"Release name: {context.release_name}")
    print(f"Run ID: {context.pipeline_run_id}")
    print(f"Raw volume path: {environment.raw_volume_path}")
    print(f"Expected file count: {len(inventory_status.expected_files)}")
    print(f"Discovered file count: {len(inventory_status.discovered_files)}")
    print(
        "Configured unexpected file policy: "
        f"{inventory_status.policy} "
        "(the run fails only if extra non-configured files are present)"
    )
    print(
        "Unexpected files detected: "
        f"{', '.join(inventory_status.unexpected_files) if inventory_status.unexpected_files else '<none>'}"
    )
    print(
        "Missing required files: "
        f"{', '.join(inventory_status.missing_files) if inventory_status.missing_files else '<none>'}"
    )
    print(
        "Raw inventory validation succeeded for "
        f"{len(source_readiness)} configured sources."
    )

    set_task_value(dbutils, "run_id", context.pipeline_run_id)


if __name__ == "__main__":
    main()
