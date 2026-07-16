"""Finalize the Bronze-to-Silver workflow wiring summary."""

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


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Finalize the Bronze-to-Silver workflow wiring summary."
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

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release name: {args.release_name}")
    print(f"Run ID: {args.run_id}")
    print(f"Configured Silver table count: {len(config.silver_tables_in_build_order)}")
    print("Bronze-to-Silver Databricks workflow resource is wired for serverless deployment.")
    print("These script tasks currently provide bundle-valid orchestration wiring and stage registration previews.")

    set_task_value(dbutils, "run_id", args.run_id)
    set_task_value(dbutils, "final_status", "WIRED")


if __name__ == "__main__":
    main()
