"""Tests for Silver-to-Gold I/O naming helpers."""

from datetime import date

from napa_pipeline.silver_to_gold.environment import GoldRuntimeContext, ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_stage_table_fqn,
    get_gold_target_table_fqn,
    get_operations_table_fqn,
    get_silver_source_table_fqn,
)


def test_io_helpers_return_expected_fully_qualified_names() -> None:
    environment = ReleaseEnvironment(
        catalog="workspace",
        silver_schema="instructor_5k_silver",
        gold_schema="instructor_5k_gold",
        gold_stage_schema="instructor_5k_gold_stage",
        operations_schema="instructor_ops",
    )
    context = GoldRuntimeContext(
        release_name="napa_5k",
        release_role="development",
        catalog="workspace",
        silver_schema="instructor_5k_silver",
        gold_schema="instructor_5k_gold",
        stage_schema="instructor_5k_gold_stage",
        operations_schema="instructor_ops",
        analysis_as_of_date=date(2026, 6, 30),
        scoring_scenario="BALANCED",
        model_enabled=True,
        pipeline_version="1.0.0",
        configuration_hash="abc123",
        deterministic_seed=42,
        upstream_silver_run_id="upstream-run-123",
    )

    assert get_silver_source_table_fqn(environment, "matches") == "workspace.instructor_5k_silver.matches"
    assert get_gold_target_table_fqn(environment, "player_current_ratings") == "workspace.instructor_5k_gold.player_current_ratings"
    assert get_gold_stage_table_fqn(environment, "player_current_ratings") == "workspace.instructor_5k_gold_stage.player_current_ratings"
    assert get_operations_table_fqn(context, "gold_table_runs") == "workspace.instructor_ops.gold_table_runs"
