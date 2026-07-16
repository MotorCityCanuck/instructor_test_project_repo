"""Tests for the Bronze-to-Silver Databricks workflow definition template."""

from pathlib import Path

import yaml


def _load_workflow_definition() -> dict:
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / "config"
        / "bronze_to_silver"
        / "workflows"
        / "napa_bronze_to_silver.job.yml"
    )
    with workflow_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_workflow_uses_single_release_parameter() -> None:
    definition = _load_workflow_definition()
    job = definition["resources"]["jobs"]["napa_bronze_to_silver"]

    assert job["name"] == "NAPA Bronze to Silver"
    assert job["parameters"] == [{"name": "release_name", "default": "napa_5k"}]


def test_workflow_tasks_form_linear_bronze_to_silver_graph() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_bronze_to_silver"]["tasks"]

    task_keys = [task["task_key"] for task in tasks]
    assert task_keys == [
        "resolve_configuration",
        "validate_environment",
        "validate_bronze_sources",
        "build_reference",
        "build_athlete",
        "build_organization_partnership",
        "build_competition",
        "run_cross_table_validation",
        "publish_convenience_views",
        "finalize_pipeline_summary",
    ]

    depends_on = {
        task["task_key"]: [item["task_key"] for item in task.get("depends_on", [])]
        for task in tasks
    }
    assert depends_on["resolve_configuration"] == []
    assert depends_on["validate_environment"] == ["resolve_configuration"]
    assert depends_on["validate_bronze_sources"] == ["validate_environment"]
    assert depends_on["build_reference"] == ["validate_bronze_sources"]
    assert depends_on["build_athlete"] == ["build_reference"]
    assert depends_on["build_organization_partnership"] == ["build_athlete"]
    assert depends_on["build_competition"] == ["build_organization_partnership"]
    assert depends_on["run_cross_table_validation"] == ["build_competition"]
    assert depends_on["publish_convenience_views"] == ["run_cross_table_validation"]
    assert depends_on["finalize_pipeline_summary"] == [
        "resolve_configuration",
        "validate_environment",
        "validate_bronze_sources",
        "build_reference",
        "build_athlete",
        "build_organization_partnership",
        "build_competition",
        "run_cross_table_validation",
        "publish_convenience_views",
    ]
    final_task = next(task for task in tasks if task["task_key"] == "finalize_pipeline_summary")
    assert final_task["run_if"] == "ALL_DONE"


def test_all_tasks_receive_shared_release_name_parameter() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_bronze_to_silver"]["tasks"]

    for task in tasks:
        parameters = task["spark_python_task"]["parameters"]
        assert parameters[0:2] == [
            "--release-name",
            "{{job.parameters.release_name}}",
        ]


def test_workflow_uses_python_script_tasks() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_bronze_to_silver"]["tasks"]

    expected_files = [
        "11_b2s_resolve_configuration.py",
        "12_b2s_validate_environment.py",
        "13_b2s_validate_bronze_sources.py",
        "14_b2s_build_reference.py",
        "15_b2s_build_athlete.py",
        "16_b2s_build_organization_partnership.py",
        "17_b2s_build_competition.py",
        "18_b2s_run_cross_table_validation.py",
        "19_b2s_publish_convenience_views.py",
        "20_b2s_finalize_pipeline_summary.py",
    ]

    for task, expected_file in zip(tasks, expected_files, strict=True):
        assert "notebook_task" not in task
        assert task["spark_python_task"]["python_file"].endswith(expected_file)


def test_downstream_tasks_receive_run_id_from_resolve_configuration() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_bronze_to_silver"]["tasks"]

    for task in tasks[1:]:
        parameters = task["spark_python_task"]["parameters"]
        assert "--run-id" in parameters
        run_id_index = parameters.index("--run-id") + 1
        assert parameters[run_id_index] == "{{tasks.resolve_configuration.values.run_id}}"
