"""Preview Bronze-to-Silver convenience-view publication."""

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
from napa_pipeline.bronze_to_silver.workflow import CONVENIENCE_VIEW_NAMES


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preview Bronze-to-Silver convenience-view publication."
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
    print(f"create_convenience_views: {config.data['execution']['create_convenience_views']}")
    print("Convenience views:")
    for view_name in CONVENIENCE_VIEW_NAMES:
        print(f"- {view_name}")
    print("This task currently validates workflow wiring and view registration only.")

    set_task_value(dbutils, "run_id", args.run_id)
    set_task_value(dbutils, "convenience_view_count", len(CONVENIENCE_VIEW_NAMES))


if __name__ == "__main__":
    main()
