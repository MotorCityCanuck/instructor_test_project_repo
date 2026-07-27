"""Resolve Raw certification configuration and generate the workflow run ID."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.certification.cli import (
    add_config_path_argument,
    add_release_argument,
    add_run_id_argument,
    get_databricks_global,
    normalize_config_path,
    set_task_value,
)
from napa_pipeline.certification.config import load_certification_config
from napa_pipeline.certification.environment import resolve_release_environment
from napa_pipeline.certification.workflow import resolve_certification_run_id


SCRIPT_VERSION = "2026.07.27.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the certification configuration task."""
    parser = argparse.ArgumentParser(
        description="Resolve Raw certification configuration for a release type."
    )
    add_release_argument(parser)
    add_config_path_argument(parser)
    add_run_id_argument(parser, required=False)
    return parser.parse_args()


def main() -> None:
    """Resolve certification configuration and expose workflow task values."""
    args = parse_args()
    config = load_certification_config(
        args.release_type,
        config_root=normalize_config_path(args.config_path),
    )
    environment = resolve_release_environment(config)
    certification_run_id = resolve_certification_run_id(args.run_id)

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Pipeline name: {config.data['project']['pipeline_name']}")
    print(f"Pipeline version: {config.data['project']['pipeline_version']}")
    print(f"Release type: {args.release_type}")
    print(f"Release name: {config.release_name}")
    print(f"Release role: {config.release_role}")
    print(f"Certification run ID: {certification_run_id}")
    print(f"Config root: {config.config_root}")
    print(f"Config hash: {config.config_hash}")
    print(f"Catalog: {environment.catalog}")
    print(f"Raw schema: {environment.raw_schema}")
    print(f"Operations schema: {environment.operations_schema}")
    print(f"Raw volume path: {environment.raw_volume_path}")
    print(f"Artifacts root path: {environment.artifacts_root_path}")

    dbutils = get_databricks_global("dbutils")
    set_task_value(dbutils, "run_id", certification_run_id)
    set_task_value(dbutils, "certification_run_id", certification_run_id)
    set_task_value(dbutils, "release_name", config.release_name)
    set_task_value(dbutils, "config_hash", config.config_hash)
    set_task_value(dbutils, "raw_path", environment.raw_volume_path)
    set_task_value(dbutils, "artifacts_root_path", environment.artifacts_root_path)
    set_task_value(dbutils, "operations_schema", environment.operations_schema)


if __name__ == "__main__":
    main()
