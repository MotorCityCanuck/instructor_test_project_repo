"""Tests for the Silver-to-Gold Databricks workflow definition template."""

from pathlib import Path

import yaml


def _load_workflow_definition() -> dict:
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / "config"
        / "silver_to_gold"
        / "workflows"
        / "napa_silver_to_gold.job.yml"
    )
    with workflow_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_workflow_uses_release_and_optional_analysis_date_parameters() -> None:
    definition = _load_workflow_definition()
    job = definition["resources"]["jobs"]["napa_silver_to_gold"]

    assert job["name"] == "NAPA Silver to Gold"
    assert job["parameters"] == [
        {"name": "release_name", "default": "napa_5k"},
        {"name": "analysis_as_of_date", "default": ""},
    ]


def test_workflow_tasks_form_linear_silver_to_gold_graph() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_silver_to_gold"]["tasks"]

    task_keys = [task["task_key"] for task in tasks]
    assert task_keys == [
        "build_competition_foundation",
        "build_persistent_team_resolution",
        "build_analytical_ratings",
        "build_player_features",
        "build_team_features",
        "build_quality_confidence",
        "build_match_outcome_products",
        "build_scorecards_and_rankings",
        "build_olympic_candidates",
        "build_olympic_recommendations",
        "build_sensitivity_and_explanations",
    ]

    depends_on = {
        task["task_key"]: [item["task_key"] for item in task.get("depends_on", [])]
        for task in tasks
    }
    assert depends_on["build_competition_foundation"] == []
    assert depends_on["build_persistent_team_resolution"] == ["build_competition_foundation"]
    assert depends_on["build_analytical_ratings"] == ["build_persistent_team_resolution"]
    assert depends_on["build_player_features"] == ["build_analytical_ratings"]
    assert depends_on["build_team_features"] == ["build_player_features"]
    assert depends_on["build_quality_confidence"] == ["build_team_features"]
    assert depends_on["build_match_outcome_products"] == ["build_quality_confidence"]
    assert depends_on["build_scorecards_and_rankings"] == ["build_match_outcome_products"]
    assert depends_on["build_olympic_candidates"] == ["build_scorecards_and_rankings"]
    assert depends_on["build_olympic_recommendations"] == ["build_olympic_candidates"]
    assert depends_on["build_sensitivity_and_explanations"] == ["build_olympic_recommendations"]


def test_all_tasks_receive_shared_release_and_analysis_date_parameters() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_silver_to_gold"]["tasks"]

    for task in tasks:
        parameters = task["spark_python_task"]["parameters"]
        assert parameters[0:4] == [
            "--release-name",
            "{{job.parameters.release_name}}",
            "--analysis-as-of-date",
            "{{job.parameters.analysis_as_of_date}}",
        ]


def test_workflow_uses_python_script_tasks() -> None:
    definition = _load_workflow_definition()
    tasks = definition["resources"]["jobs"]["napa_silver_to_gold"]["tasks"]

    expected_files = [
        "21_g2g_phase3_competition_foundation_harness.py",
        "22_g2g_phase4_persistent_team_resolution_harness.py",
        "24_g2g_phase5_analytical_rating_engine_harness.py",
        "25_g2g_phase6_player_feature_harness.py",
        "26_g2g_phase7_team_feature_harness.py",
        "27_g2g_phase8_entity_quality_confidence_harness.py",
        "28_g2g_phase9_match_outcome_harness.py",
        "29_g2g_phase10_player_scorecard_harness.py",
        "30_g2g_phase11_team_selection_harness.py",
        "31_g2g_phase12_recommendation_harness.py",
        "32_g2g_phase13_sensitivity_harness.py",
    ]

    for task, expected_file in zip(tasks, expected_files, strict=True):
        assert "notebook_task" not in task
        assert task["spark_python_task"]["python_file"].endswith(expected_file)
