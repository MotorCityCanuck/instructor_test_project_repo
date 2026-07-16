"""Preview the Bronze-to-Silver athlete-stage build configuration."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.bronze_to_silver.cli import (
    add_config_path_argument,
    add_release_name_argument,
    add_run_id_argument,
    get_databricks_global,
    normalize_config_path,
    set_task_value,
)
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.workflow import get_stage_table_names


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preview the Bronze-to-Silver athlete-stage build configuration."
    )
    add_release_name_argument(parser)
    add_run_id_argument(parser)
    add_config_path_argument(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dbutils = get_databricks_global("dbutils")
    config = load_bronze_to_silver_config(
        args.release_name,
        config_root=normalize_config_path(args.config_path),
    )
    table_names = get_stage_table_names(config, "athlete")

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release name: {args.release_name}")
    print(f"Run ID: {args.run_id}")
    print("Athlete tables:")
    for table_name in table_names:
        print(f"- {table_name}")
    print("This task currently validates workflow wiring and stage registration only.")

    set_task_value(dbutils, "run_id", args.run_id)
    set_task_value(dbutils, "athlete_table_count", len(table_names))


if __name__ == "__main__":
    main()
