"""Tests for standalone Gold-layer audit helpers."""

from datetime import date, datetime
from types import SimpleNamespace

from napa_pipeline.silver_to_gold.gold_audit import (
    build_gold_audit_error_message,
    build_gold_audit_failure_summary,
    build_expected_reconciliations,
    evaluate_table_profile_anomalies,
)
from napa_pipeline.silver_to_gold.operations import create_pipeline_context
from napa_pipeline.silver_to_gold.environment import GoldRuntimeContext


def _pipeline_context():
    runtime_context = GoldRuntimeContext(
        release_name="napa_5k",
        release_role="development",
        catalog="workspace",
        silver_schema="instructor_5k_silver",
        gold_schema="instructor_5k_gold",
        stage_schema="instructor_5k_gold_stage",
        operations_schema="instructor_ops",
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        model_enabled=True,
        authoritative_recommendation_flag=False,
        pipeline_version="1.0.0",
        configuration_hash="abc123",
        deterministic_seed=42,
        upstream_silver_run_id="upstream-run-1",
    )
    return create_pipeline_context(runtime_context, pipeline_run_id="audit-run-1")


def test_build_expected_reconciliations_uses_known_cross_table_balances() -> None:
    reconciliations = build_expected_reconciliations(
        {
            "competition_match_sides": 100,
            "competition_player_matches": 200,
            "player_development_features": 50,
            "player_performance_features": 200,
            "olympic_team_recommendations": 12,
            "recommendation_explanations": 12,
            "selection_sensitivity_results": 72,
        },
        evidence_window_count=4,
        sensitivity_scenario_count=6,
    )

    assert [(item.name, item.source_count, item.accepted_count) for item in reconciliations] == [
        ("competition_player_matches_per_side", 200, 200),
        ("player_performance_feature_window_balance", 200, 200),
        ("recommendation_explanation_coverage", 12, 12),
        ("selection_sensitivity_scenario_coverage", 72, 72),
    ]


def test_evaluate_table_profile_anomalies_flags_key_and_alignment_failures() -> None:
    context = _pipeline_context()
    profile = SimpleNamespace(
        table_name="player_evaluation_scorecards",
        row_count=10,
        null_primary_key_row_count=1,
        duplicate_primary_key_group_count=2,
        duplicate_primary_key_row_count=3,
        null_primary_key_sample_keys=("player_id=<NULL>",),
        duplicate_primary_key_sample_keys=("player_id=p1",),
        distinct_analysis_as_of_date_count=2,
        distinct_scoring_scenario_count=2,
    )
    column_profiles = {
        "analysis_as_of_date": {"min_value": "2025-12-30", "max_value": "2025-12-31"},
        "scoring_scenario": {"min_value": "BALANCED", "max_value": "RATING_HEAVY"},
    }

    records = evaluate_table_profile_anomalies(
        profile,
        column_profiles=column_profiles,
        expected_analysis_as_of_date="2025-12-31",
        expected_scoring_scenario="BALANCED",
        profiled_ts=datetime(2026, 7, 26, 0, 0),
        context=context,
    )

    assert [record["rule_id"] for record in records] == [
        "primary_key_not_null",
        "primary_key_unique",
        "analysis_as_of_date_alignment",
        "scoring_scenario_alignment",
    ]
    assert all(record["severity"] == "ERROR" for record in records)


def test_evaluate_table_profile_anomalies_warns_on_empty_tables() -> None:
    context = _pipeline_context()
    profile = SimpleNamespace(
        table_name="olympic_team_candidates",
        row_count=0,
        null_primary_key_row_count=0,
        duplicate_primary_key_group_count=0,
        duplicate_primary_key_row_count=0,
        null_primary_key_sample_keys=(),
        duplicate_primary_key_sample_keys=(),
        distinct_analysis_as_of_date_count=1,
        distinct_scoring_scenario_count=1,
    )

    records = evaluate_table_profile_anomalies(
        profile,
        column_profiles={},
        expected_analysis_as_of_date="2025-12-31",
        expected_scoring_scenario="BALANCED",
        profiled_ts=datetime(2026, 7, 26, 0, 0),
        context=context,
    )

    assert len(records) == 1
    assert records[0]["rule_id"] == "non_empty_table"
    assert records[0]["severity"] == "WARNING"


def test_build_gold_audit_failure_summary_includes_failed_rule_details() -> None:
    summary = build_gold_audit_failure_summary(
        [
            {
                "target_table": "team_selection_scorecards",
                "rule_id": "primary_key_unique",
                "severity": "ERROR",
                "status": "FAILED",
                "failed_row_count": 2,
                "failure_pct": 0.5,
                "sample_keys": ["team_id=abc"],
            },
            {
                "target_table": "olympic_team_candidates",
                "rule_id": "non_empty_table",
                "severity": "WARNING",
                "status": "FAILED",
                "failed_row_count": 0,
                "failure_pct": 100.0,
                "sample_keys": None,
            },
        ],
        [
            {
                "reconciliation_name": "recommendation_explanation_coverage",
                "status": "FAILED",
                "source_count": 12,
                "accepted_count": 10,
                "difference": 2,
            }
        ],
    )

    assert "team_selection_scorecards.primary_key_unique" in summary
    assert "olympic_team_candidates.non_empty_table" not in summary
    assert "recommendation_explanation_coverage" in summary
    assert "source_count=12" in summary


def test_build_gold_audit_error_message_includes_failure_summary() -> None:
    message = build_gold_audit_error_message(
        critical_failure_count=2,
        warning_count=1,
        failure_summary="Failed audit details:\nQuality failures: none",
    )

    assert "critical_failures=2, warnings=1" in message
    assert "Failed audit details" in message
