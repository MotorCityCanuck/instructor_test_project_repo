"""Fail the Databricks job after certification output publication when required."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.certification.cli import add_fail_on_argument, add_run_id_argument
from napa_pipeline.certification.workflow import evaluate_release_gate


SCRIPT_VERSION = "2026.07.27.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the release-gate task."""
    parser = argparse.ArgumentParser(
        description="Evaluate the final Databricks Raw certification release gate."
    )
    add_run_id_argument(parser)
    add_fail_on_argument(parser)
    parser.add_argument("--decision", required=True, help="Certification decision from the run task.")
    parser.add_argument("--status", required=True, help="Certification run status from the run task.")
    parser.add_argument("--warning-count", required=False, default="0")
    parser.add_argument("--error-count", required=False, default="0")
    parser.add_argument("--blocker-count", required=False, default="0")
    parser.add_argument("--report-path", required=False, default="")
    parser.add_argument("--snapshot-path", required=False, default="")
    parser.add_argument("--findings-path", required=False, default="")
    return parser.parse_args()


def main() -> None:
    """Evaluate the release gate and fail the task only after artifacts exist."""
    args = parse_args()
    severity_counts = {
        "warning": int(args.warning_count or 0),
        "error": int(args.error_count or 0),
        "blocker": int(args.blocker_count or 0),
    }
    gate_result = evaluate_release_gate(
        certification_decision=args.decision,
        fail_on=args.fail_on,
        severity_counts=severity_counts,
    )

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Certification run ID: {args.run_id}")
    print(f"Decision: {args.decision}")
    print(f"Status: {args.status}")
    print(f"Fail on: {args.fail_on}")
    print(f"Report path: {args.report_path or 'not published'}")
    print(f"Snapshot path: {args.snapshot_path or 'not published'}")
    print(f"Findings path: {args.findings_path or 'not published'}")
    print(gate_result.message)

    if gate_result.should_fail:
        raise RuntimeError(gate_result.message)


if __name__ == "__main__":
    main()
