"""Run a release-parameterized Silver-layer contract audit."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.bronze_to_silver.cli import (
    add_config_path_argument,
    add_release_name_argument,
    get_databricks_global,
    normalize_config_path,
    set_task_value,
)
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.silver_audit import run_silver_layer_audit


SCRIPT_VERSION = "2026.07.24.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the Silver-layer audit."""
    parser = argparse.ArgumentParser(
        description="Audit the published Silver layer for one configured release."
    )
    add_release_name_argument(parser)
    add_config_path_argument(parser)
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=10,
        help="Maximum number of sample values or duplicate keys to show per finding.",
    )
    parser.add_argument(
        "--null-detail-limit",
        type=int,
        default=8,
        help="Maximum number of null-bearing columns to print per table.",
    )
    parser.add_argument(
        "--skip-cross-table",
        action="store_true",
        help="Skip the cross-table contract validations and run table-local checks only.",
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Raise an error when the audit finds any non-warning anomaly.",
    )
    return parser.parse_args()


def main() -> None:
    """Execute the Silver-layer audit and print a readable report."""
    args = parse_args()
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")

    config = load_bronze_to_silver_config(
        args.release_name,
        config_root=normalize_config_path(args.config_path),
    )
    environment = resolve_release_environment(config)
    report = run_silver_layer_audit(
        spark,
        config,
        environment,
        sample_limit=max(int(args.sample_limit), 1),
        include_cross_table=not args.skip_cross_table,
    )

    print(f"Script version: {SCRIPT_VERSION}")
    print(report.render_text(null_detail_limit=max(int(args.null_detail_limit), 1)))

    set_task_value(dbutils, "release_name", report.release_name)
    set_task_value(dbutils, "silver_audit_checked_table_count", report.checked_table_count)
    set_task_value(dbutils, "silver_audit_anomaly_count", report.anomaly_count)
    set_task_value(dbutils, "silver_audit_error_count", report.error_count)
    set_task_value(dbutils, "silver_audit_warning_count", report.warning_count)

    if args.fail_on_error and report.error_count:
        raise RuntimeError(
            f"Silver-layer audit found {report.error_count} error-level anomalies for {report.release_name}."
        )


if __name__ == "__main__":
    main()
