"""Tests for the Raw certification Databricks workflow definition template."""

from pathlib import Path

import yaml


def _load_workflow_definition() -> dict:
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / "config"
        / "certification"
        / "workflows"
        / "napa_raw_certification.job.yml"
    )
    with workflow_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_workflow_uses_single_release_parameterized_job() -> None:
    definition = _load_workflow_definition()
    job = definition["resources"]["jobs"]["napa_raw_certification"]

    assert job["name"] == "NAPA Raw Certification"
    assert job["parameters"] == [
        {"name": "release_type", "default": "5k"},
        {"name": "analysis_as_of_date", "default": ""},
        {"name": "config_path", "default": ""},
        {"name": "certification_run_id", "default": ""},
        {"name": "source_snapshot_path", "default": ""},
        {"name": "baseline_id", "default": ""},
        {"name": "fail_on", "default": "blocker"},
    ]


def test_workflow_tasks_form_linear_certification_graph() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_raw_certification"]["tasks"]

    assert [task["task_key"] for task in tasks] == [
        "resolve_configuration",
        "run_certification",
        "release_gate",
    ]
    depends_on = {
        task["task_key"]: [item["task_key"] for item in task.get("depends_on", [])]
        for task in tasks
    }
    assert depends_on["resolve_configuration"] == []
    assert depends_on["run_certification"] == ["resolve_configuration"]
    assert depends_on["release_gate"] == ["run_certification"]


def test_workflow_uses_python_script_tasks() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_raw_certification"]["tasks"]

    expected_files = [
        "34_rc_resolve_configuration.py",
        "35_rc_run_certification.py",
        "36_rc_release_gate.py",
    ]
    for task, expected_file in zip(tasks, expected_files, strict=True):
        assert "notebook_task" not in task
        assert task["spark_python_task"]["python_file"].endswith(expected_file)


def test_downstream_tasks_share_resolved_run_id() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_raw_certification"]["tasks"]

    run_task_parameters = tasks[1]["spark_python_task"]["parameters"]
    assert "--run-id" in run_task_parameters
    run_id_index = run_task_parameters.index("--run-id") + 1
    assert run_task_parameters[run_id_index] == "{{tasks.resolve_configuration.values.run_id}}"

    gate_parameters = tasks[2]["spark_python_task"]["parameters"]
    assert "--run-id" in gate_parameters
    gate_run_id_index = gate_parameters.index("--run-id") + 1
    assert gate_parameters[gate_run_id_index] == "{{tasks.resolve_configuration.values.run_id}}"


def test_release_gate_receives_certification_outputs() -> None:
    definition = _load_workflow_definition()
    gate_parameters = definition["resources"]["jobs"]["napa_raw_certification"]["tasks"][2][
        "spark_python_task"
    ]["parameters"]

    assert "{{tasks.run_certification.values.certification_decision}}" in gate_parameters
    assert "{{tasks.run_certification.values.report_path}}" in gate_parameters
    assert "{{tasks.run_certification.values.snapshot_path}}" in gate_parameters
    assert "{{tasks.run_certification.values.findings_path}}" in gate_parameters
