"""Execute the end-to-end Raw certification workflow and publish outputs."""

from __future__ import annotations

import argparse
import json

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.certification.cli import (
    add_analysis_as_of_date_argument,
    add_baseline_id_argument,
    add_config_path_argument,
    add_release_argument,
    add_run_id_argument,
    add_source_snapshot_path_argument,
    get_databricks_global,
    normalize_config_path,
    normalize_optional_string,
    set_task_value,
)
from napa_pipeline.certification.config import load_certification_config
from napa_pipeline.certification.environment import ensure_release_environment
from napa_pipeline.certification.workflow import execute_certification_workflow


SCRIPT_VERSION = "2026.07.27.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the certification execution task."""
    parser = argparse.ArgumentParser(
        description="Run Databricks Raw certification for a configured release."
    )
    add_release_argument(parser)
    add_config_path_argument(parser)
    add_run_id_argument(parser)
    add_analysis_as_of_date_argument(parser)
    add_source_snapshot_path_argument(parser)
    add_baseline_id_argument(parser)
    return parser.parse_args()


def main() -> None:
    """Run certification, publish artifacts, and expose workflow outputs."""
    args = parse_args()
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")

    config = load_certification_config(
        args.release_type,
        config_root=normalize_config_path(args.config_path),
    )
    environment_status = ensure_release_environment(
        spark,
        config,
        create_missing=bool(config.data["execution"].get("create_missing_managed_objects", True)),
    )
    environment = environment_status.release_environment

    workflow_result = execute_certification_workflow(
        spark=spark,
        dbutils=dbutils,
        config=config,
        environment=environment,
        certification_run_id=args.run_id,
        analysis_as_of_date=normalize_optional_string(args.analysis_as_of_date),
        source_snapshot_path=normalize_optional_string(args.source_snapshot_path),
        baseline_id=normalize_optional_string(args.baseline_id),
        persist_results=bool(config.data["execution"].get("persist_results", True)),
        publish_report=bool(config.data["execution"].get("publish_report", True)),
    )
    assessment = workflow_result.assessment
    artifact_bundle = workflow_result.artifact_bundle

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Pipeline name: {config.data['project']['pipeline_name']}")
    print(f"Pipeline version: {config.data['project']['pipeline_version']}")
    print(f"Release name: {assessment.release_name}")
    print(f"Release role: {assessment.release_role}")
    print(f"Certification run ID: {assessment.certification_run_id}")
    print(f"Config root: {config.config_root}")
    print(f"Config hash: {config.config_hash}")
    print(f"Catalog: {environment.catalog}")
    print(f"Raw schema: {environment.raw_schema}")
    print(f"Operations schema: {environment.operations_schema}")
    print(f"Raw path: {assessment.raw_path}")
    print(f"Decision: {assessment.certification_decision}")
    print(f"Run status: {assessment.status}")
    print(f"Overall score: {assessment.overall_score}")
    print(f"Severity counts: {json.dumps(assessment.severity_counts, sort_keys=True)}")
    print(f"Status counts: {json.dumps(assessment.status_counts, sort_keys=True)}")
    if artifact_bundle is not None:
        print(f"Snapshot path: {artifact_bundle.snapshot_path}")
        print(f"Report path: {artifact_bundle.report_path}")
        print(f"Findings CSV path: {artifact_bundle.findings_path}")
    if assessment.error_message:
        print(f"Execution error: {assessment.error_message}")

    set_task_value(dbutils, "run_id", assessment.certification_run_id)
    set_task_value(dbutils, "certification_run_id", assessment.certification_run_id)
    set_task_value(dbutils, "certification_decision", assessment.certification_decision)
    set_task_value(dbutils, "certification_status", assessment.status)
    set_task_value(dbutils, "overall_score", assessment.overall_score)
    set_task_value(dbutils, "warning_count", assessment.severity_counts.get("warning", 0))
    set_task_value(dbutils, "error_count", assessment.severity_counts.get("error", 0))
    set_task_value(dbutils, "blocker_count", assessment.severity_counts.get("blocker", 0))
    set_task_value(dbutils, "hard_gate_count", len(assessment.hard_gate_rule_ids))
    set_task_value(dbutils, "report_path", artifact_bundle.report_path if artifact_bundle else "")
    set_task_value(dbutils, "snapshot_path", artifact_bundle.snapshot_path if artifact_bundle else "")
    set_task_value(dbutils, "findings_path", artifact_bundle.findings_path if artifact_bundle else "")


if __name__ == "__main__":
    main()
