"""Tests for Bronze-to-Silver competition-stage Spark SQL execution."""

import napa_pipeline.bronze_to_silver.execute as execute_module
from napa_pipeline.bronze_to_silver.competition_sql import (
    build_competition_sql_plan,
    supports_competition_sql_transform,
)
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context


class DummySpark:
    pass


def test_source_table_with_missing_columns_adds_null_aliases_for_competition() -> None:
    source_sql = execute_module._source_table_with_missing_columns(
        "workspace.instructor_5k_bronze.match_teams",
        {"id", "match_id", "side_number"},
        {"match_team_id", "id", "match_id", "team_number", "side_number"},
    )

    assert source_sql.startswith("(SELECT *,")
    assert "CAST(NULL AS STRING) AS match_team_id" in source_sql
    assert "CAST(NULL AS STRING) AS team_number" in source_sql
    assert "CAST(NULL AS STRING) AS side_number" not in source_sql
    assert "FROM workspace.instructor_5k_bronze.match_teams" in source_sql


def _config_environment_context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment, pipeline_run_id="run-123")
    return config, environment, context


def test_supports_competition_sql_transform_for_parent_tables_only() -> None:
    assert supports_competition_sql_transform("build_matches") is True
    assert supports_competition_sql_transform("build_match_teams") is True
    assert supports_competition_sql_transform("build_match_team_players") is True
    assert supports_competition_sql_transform("build_match_games") is True


def test_build_competition_sql_plan_for_matches_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_competition_sql_plan(
        config,
        context,
        target_table="matches",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.matches",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "MATCH_001" in plan.rejected_sql
    assert "MATCH_007" in plan.rejected_sql
    assert "MATCH_DUPLICATE" in plan.rejected_sql
    assert "match_year" in plan.accepted_sql
    assert "completed_flag" in plan.accepted_sql
    assert "winning_team_id" in plan.rejected_sql
    assert "winner_team_lookup" in plan.accepted_sql
    assert "),\nvalid_rows AS (" in plan.business_key_duplicate_count_sql


def test_build_competition_sql_plan_for_match_teams_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_competition_sql_plan(
        config,
        context,
        target_table="match_teams",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.match_teams",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "MATCH_TEAM_001" in plan.rejected_sql
    assert "MATCH_TEAM_005" in plan.rejected_sql
    assert "MATCH_TEAM_DUPLICATE" in plan.rejected_sql
    assert "winner_flag" in plan.accepted_sql
    assert "side_cardinality_warning_flag" in plan.accepted_sql
    assert plan.warning_count_sql is not None


def test_execute_single_table_sql_publishes_match_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {"published": [], "quality": []}

    scalar_values = iter([7, 1, 1])

    monkeypatch.setattr(execute_module, "scalar_sql_value", lambda *_args, **_kwargs: next(scalar_values))
    monkeypatch.setattr(
        execute_module,
        "publish_sql_table",
        lambda _spark, table_fqn, select_sql: captured["published"].append((table_fqn, select_sql)) or (4 if table_fqn.endswith(".matches") else 2),
    )
    monkeypatch.setattr(
        execute_module,
        "append_quality_results_for_reject_table",
        lambda _spark, _context, *, target_table, evaluated_row_count, reject_table_fqn: captured["quality"].append((target_table, evaluated_row_count, reject_table_fqn)) or [],
    )
    monkeypatch.setattr(execute_module, "append_schema_snapshot_for_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_records", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_warning_message", lambda *args, **kwargs: None)

    metrics = execute_module._execute_single_table_sql(
        spark,
        config,
        environment,
        context,
        table_config=config.data["silver_tables"]["matches"],
    )

    assert metrics["source_row_count"] == 7
    assert metrics["exact_duplicate_count"] == 1
    assert metrics["business_key_duplicate_count"] == 1
    assert metrics["accepted_row_count"] == 4
    assert metrics["rejected_row_count"] == 2
    assert metrics["warning_count"] == 0
    assert captured["quality"][0][0] == "matches"


def test_execute_single_table_sql_wraps_match_teams_source_for_matches(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured: dict[str, str | None] = {"match_teams_source_table_fqn": None}

    monkeypatch.setattr(
        execute_module,
        "_get_table_column_names",
        lambda _spark, table_fqn: {"match_id", "team_id", "team_number"}
        if str(table_fqn).endswith(".match_teams")
        else {"match_id", "winning_team_id"},
    )
    monkeypatch.setattr(
        execute_module,
        "build_competition_sql_plan",
        lambda *args, **kwargs: captured.update(
            {"match_teams_source_table_fqn": kwargs.get("match_teams_source_table_fqn")}
        )
        or build_competition_sql_plan(*args, **kwargs),
    )
    monkeypatch.setattr(execute_module, "scalar_sql_value", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(execute_module, "publish_sql_table", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(execute_module, "append_quality_results_for_reject_table", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(execute_module, "append_schema_snapshot_for_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_records", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_warning_message", lambda *args, **kwargs: None)

    execute_module._execute_single_table_sql(
        spark,
        config,
        environment,
        context,
        table_config=config.data["silver_tables"]["matches"],
    )

    assert captured["match_teams_source_table_fqn"] is not None
    assert "CAST(NULL AS STRING) AS side_number" in str(captured["match_teams_source_table_fqn"])


def test_execute_single_table_sql_publishes_match_team_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {"published": [], "quality": []}

    scalar_values = iter([8, 0, 1, 1])

    monkeypatch.setattr(execute_module, "scalar_sql_value", lambda *_args, **_kwargs: next(scalar_values))
    monkeypatch.setattr(
        execute_module,
        "publish_sql_table",
        lambda _spark, table_fqn, select_sql: captured["published"].append((table_fqn, select_sql)) or (6 if table_fqn.endswith(".match_teams") else 1),
    )
    monkeypatch.setattr(
        execute_module,
        "append_quality_results_for_reject_table",
        lambda _spark, _context, *, target_table, evaluated_row_count, reject_table_fqn: captured["quality"].append((target_table, evaluated_row_count, reject_table_fqn)) or [],
    )
    monkeypatch.setattr(execute_module, "append_schema_snapshot_for_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_records", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_warning_message", lambda *args, **kwargs: None)

    metrics = execute_module._execute_single_table_sql(
        spark,
        config,
        environment,
        context,
        table_config=config.data["silver_tables"]["match_teams"],
    )

    assert metrics["source_row_count"] == 8
    assert metrics["exact_duplicate_count"] == 0
    assert metrics["business_key_duplicate_count"] == 1
    assert metrics["accepted_row_count"] == 6
    assert metrics["rejected_row_count"] == 1
    assert metrics["warning_count"] == 1
    assert captured["quality"][0][0] == "match_teams"


def test_build_competition_sql_plan_for_match_team_players_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_competition_sql_plan(
        config,
        context,
        target_table="match_team_players",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.match_team_players",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "MATCH_TEAM_PLAYER_001" in plan.rejected_sql
    assert "MATCH_TEAM_PLAYER_007" in plan.rejected_sql
    assert "MATCH_TEAM_PLAYER_DUPLICATE" in plan.rejected_sql
    assert "membership_history_warning_flag" in plan.accepted_sql
    assert "player_position_raw IS NOT NULL AND player_position IS NULL" in plan.rejected_sql
    assert "position_alias_raw" in plan.rejected_sql
    assert plan.warning_count_sql is not None


def test_build_competition_sql_plan_for_match_games_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_competition_sql_plan(
        config,
        context,
        target_table="match_games",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.match_games",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "MATCH_GAME_001" in plan.rejected_sql
    assert "MATCH_GAME_011" in plan.rejected_sql
    assert "MATCH_GAME_DUPLICATE" in plan.rejected_sql
    assert "score_margin" in plan.accepted_sql
    assert "extended_game_flag" in plan.accepted_sql


def test_execute_single_table_sql_publishes_match_team_player_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {"published": [], "quality": []}

    scalar_values = iter([10, 1, 2, 2])

    monkeypatch.setattr(execute_module, "scalar_sql_value", lambda *_args, **_kwargs: next(scalar_values))
    monkeypatch.setattr(
        execute_module,
        "publish_sql_table",
        lambda _spark, table_fqn, select_sql: captured["published"].append((table_fqn, select_sql)) or (6 if table_fqn.endswith(".match_team_players") else 3),
    )
    monkeypatch.setattr(
        execute_module,
        "append_quality_results_for_reject_table",
        lambda _spark, _context, *, target_table, evaluated_row_count, reject_table_fqn: captured["quality"].append((target_table, evaluated_row_count, reject_table_fqn)) or [],
    )
    monkeypatch.setattr(execute_module, "append_schema_snapshot_for_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_records", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_warning_message", lambda *args, **kwargs: None)

    metrics = execute_module._execute_single_table_sql(
        spark,
        config,
        environment,
        context,
        table_config=config.data["silver_tables"]["match_team_players"],
    )

    assert metrics["source_row_count"] == 10
    assert metrics["exact_duplicate_count"] == 1
    assert metrics["business_key_duplicate_count"] == 2
    assert metrics["accepted_row_count"] == 6
    assert metrics["rejected_row_count"] == 3
    assert metrics["warning_count"] == 2
    assert captured["quality"][0][0] == "match_team_players"


def test_execute_single_table_sql_publishes_match_game_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {"published": [], "quality": []}

    scalar_values = iter([9, 0, 1])

    monkeypatch.setattr(execute_module, "scalar_sql_value", lambda *_args, **_kwargs: next(scalar_values))
    monkeypatch.setattr(
        execute_module,
        "publish_sql_table",
        lambda _spark, table_fqn, select_sql: captured["published"].append((table_fqn, select_sql)) or (7 if table_fqn.endswith(".match_games") else 1),
    )
    monkeypatch.setattr(
        execute_module,
        "append_quality_results_for_reject_table",
        lambda _spark, _context, *, target_table, evaluated_row_count, reject_table_fqn: captured["quality"].append((target_table, evaluated_row_count, reject_table_fqn)) or [],
    )
    monkeypatch.setattr(execute_module, "append_schema_snapshot_for_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_records", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_warning_message", lambda *args, **kwargs: None)

    metrics = execute_module._execute_single_table_sql(
        spark,
        config,
        environment,
        context,
        table_config=config.data["silver_tables"]["match_games"],
    )

    assert metrics["source_row_count"] == 9
    assert metrics["exact_duplicate_count"] == 0
    assert metrics["business_key_duplicate_count"] == 1
    assert metrics["accepted_row_count"] == 7
    assert metrics["rejected_row_count"] == 1
    assert metrics["warning_count"] == 0
    assert captured["quality"][0][0] == "match_games"
