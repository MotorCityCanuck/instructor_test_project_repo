"""Tests for the standalone Silver-to-Gold audit Databricks workflow definition."""

from pathlib import Path

import yaml


def _load_workflow_definition() -> dict:
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / "config"
        / "silver_to_gold"
        / "workflows"
        / "napa_silver_to_gold_audit.job.yml"
    )
    with workflow_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_audit_workflow_uses_release_and_optional_analysis_date_parameters() -> None:
    definition = _load_workflow_definition()
    job = definition["resources"]["jobs"]["napa_silver_to_gold_audit"]

    assert job["name"] == "NAPA Silver to Gold Audit"
    assert job["parameters"] == [
        {"name": "release_name", "default": "napa_5k"},
        {"name": "analysis_as_of_date", "default": ""},
    ]


def test_audit_workflow_uses_single_python_task() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_silver_to_gold_audit"]["tasks"]

    assert [task["task_key"] for task in tasks] == ["audit_gold_layer"]
    task = tasks[0]
    assert "notebook_task" not in task
    assert task["spark_python_task"]["python_file"].endswith(
        "33_g2g_gold_layer_audit_harness.py"
    )
    assert task["spark_python_task"]["parameters"] == [
        "--release-name",
        "{{job.parameters.release_name}}",
        "--analysis-as-of-date",
        "{{job.parameters.analysis_as_of_date}}",
    ]
