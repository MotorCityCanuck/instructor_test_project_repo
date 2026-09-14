"""Tests for the Olympic team-selection Databricks workflow resource."""

from pathlib import Path

import yaml


def _load_workflow_definition() -> dict:
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / "config"
        / "silver_to_gold"
        / "workflows"
        / "napa_olympic_team_selection.job.yml"
    )
    with workflow_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_workflow_exposes_release_and_team_set_parameters() -> None:
    job = _load_workflow_definition()["resources"]["jobs"]["napa_olympic_team_selection"]

    assert job["name"] == "NAPA Olympic Team Selection"
    assert job["parameters"] == [
        {"name": "release_name", "default": "napa_5k"},
        {"name": "team_set_count", "default": "1"},
    ]


def test_workflow_runs_the_team_selection_script() -> None:
    job = _load_workflow_definition()["resources"]["jobs"]["napa_olympic_team_selection"]
    task = job["tasks"]

    assert len(task) == 1
    assert task[0]["task_key"] == "select_olympic_teams"
    assert task[0]["spark_python_task"]["python_file"].endswith(
        "37_select_olympic_teams.py"
    )
    assert task[0]["spark_python_task"]["parameters"] == [
        "--release-name",
        "{{job.parameters.release_name}}",
        "--team-set-count",
        "{{job.parameters.team_set_count}}",
    ]
