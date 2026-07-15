"""Tests for the Raw-to-Bronze Databricks workflow definition template."""

from pathlib import Path

import yaml


def _load_workflow_definition() -> dict:
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / "config"
        / "raw_to_bronze"
        / "workflows"
        / "napa_raw_to_bronze.job.yml"
    )
    with workflow_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_workflow_uses_single_release_parameter() -> None:
    definition = _load_workflow_definition()
    job = definition["resources"]["jobs"]["napa_raw_to_bronze"]

    assert job["name"] == "NAPA Raw to Bronze"
    assert job["parameters"] == [{"name": "dataset_release", "default": "napa_5k"}]


def test_workflow_tasks_form_linear_raw_to_bronze_graph() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_raw_to_bronze"]["tasks"]

    task_keys = [task["task_key"] for task in tasks]
    assert task_keys == [
        "resolve_configuration",
        "validate_release_environment",
        "validate_raw_inventory",
        "build_bronze_tables",
        "validate_bronze_reconciliation",
    ]

    depends_on = {
        task["task_key"]: [item["task_key"] for item in task.get("depends_on", [])]
        for task in tasks
    }
    assert depends_on["resolve_configuration"] == []
    assert depends_on["validate_release_environment"] == ["resolve_configuration"]
    assert depends_on["validate_raw_inventory"] == ["validate_release_environment"]
    assert depends_on["build_bronze_tables"] == ["validate_raw_inventory"]
    assert depends_on["validate_bronze_reconciliation"] == ["build_bronze_tables"]


def test_all_tasks_receive_shared_dataset_release_parameter() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_raw_to_bronze"]["tasks"]

    for task in tasks:
        base_parameters = task["notebook_task"]["base_parameters"]
        assert (
            base_parameters["dataset_release"]
            == "{{job.parameters.dataset_release}}"
        )
