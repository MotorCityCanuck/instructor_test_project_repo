"""Tests for Bronze-to-Silver athlete-stage Spark SQL execution."""

import napa_pipeline.bronze_to_silver.execute as execute_module
from napa_pipeline.bronze_to_silver.athlete_sql import (
    build_athlete_sql_plan,
    supports_athlete_sql_transform,
)
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.operations import (
    RECONCILIATION_RESULTS_TABLE,
    create_pipeline_context,
)


class DummySpark:
    pass


def _config_environment_context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment, pipeline_run_id="run-123")
    return config, environment, context


def test_supports_athlete_sql_transform_only_for_players() -> None:
    assert supports_athlete_sql_transform("build_players") is True
    assert supports_athlete_sql_transform("build_player_registrations") is True
    assert supports_athlete_sql_transform("build_player_assessment_history") is True
    assert supports_athlete_sql_transform("build_teams") is False


def test_build_athlete_sql_plan_for_players_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_athlete_sql_plan(
        config,
        context,
        target_table="players",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.player_master",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "PLAYER_001" in plan.rejected_sql
    assert "PLAYER_005" in plan.rejected_sql
    assert "PLAYER_DUPLICATE" in plan.rejected_sql
    assert "home_region_sk" in plan.accepted_sql
    assert "rating_confidence" in plan.accepted_sql
    assert "'CANADA'" in plan.accepted_sql


def test_build_athlete_sql_plan_for_players_uses_actual_bronze_columns() -> None:
    config, environment, context = _config_environment_context()

    plan = build_athlete_sql_plan(
        config,
        context,
        target_table="players",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.player_master",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
        source_columns={
            "player_id",
            "external_player_key",
            "first_name",
            "last_name",
            "gender",
            "birth_date",
            "dominant_hand",
            "home_region_id",
            "registration_date",
            "player_status",
            "rating_value",
            "confidence_score",
            "volatility_score",
            "global_percentile",
            "match_count_used",
            "rating_date",
            "rating_batch_id",
            "snapshot_month",
        },
    )
    combined_sql = "\n".join(
        [
            plan.accepted_sql,
            plan.rejected_sql,
            plan.exact_duplicate_count_sql,
            plan.business_key_duplicate_count_sql,
        ]
    )

    assert "COALESCE(player_id, id)" not in combined_sql
    assert "COALESCE(display_name, full_name)" not in combined_sql
    assert "COALESCE(rating, player_rating)" not in combined_sql
    assert "COALESCE(rating_confidence, confidence)" not in combined_sql
    assert "COALESCE(active_flag, status)" not in combined_sql
    assert "CAST(player_id AS STRING)" in combined_sql
    assert "CAST(rating_value AS STRING)" in combined_sql
    assert "CAST(confidence_score AS STRING)" in combined_sql
    assert "CAST(player_status AS STRING)" in combined_sql


def test_build_athlete_sql_plan_for_player_registrations_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_athlete_sql_plan(
        config,
        context,
        target_table="player_registrations",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.player_registrations",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "REG_001" in plan.rejected_sql
    assert "REG_007" in plan.rejected_sql
    assert "REG_DUPLICATE" in plan.rejected_sql
    assert "registration_sequence" in plan.accepted_sql
    assert "current_registration_flag" in plan.accepted_sql


def test_build_athlete_sql_plan_for_player_registrations_uses_actual_bronze_columns() -> None:
    config, environment, context = _config_environment_context()

    plan = build_athlete_sql_plan(
        config,
        context,
        target_table="player_registrations",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.player_registrations",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
        source_columns={
            "id",
            "player_id",
            "batch_id",
            "registration_month",
            "registration_source",
            "assigned_region_id",
            "initial_rating_value",
            "initial_confidence_score",
            "_pipeline_run_id",
            "_pipeline_name",
            "_pipeline_version",
            "_release_name",
            "_source_file_name",
            "_source_file_path",
            "_source_file_size",
            "_source_file_modification_ts",
            "_ingested_ts",
            "_source_record_hash",
        },
    )
    combined_sql = "\n".join(
        [
            plan.accepted_sql,
            plan.rejected_sql,
            plan.exact_duplicate_count_sql,
            plan.business_key_duplicate_count_sql,
        ]
    )

    assert "COALESCE(registration_id, id)" not in combined_sql
    assert "COALESCE(batch_id, monthly_batch_id)" not in combined_sql
    assert "COALESCE(effective_start_date, start_date)" not in combined_sql
    assert "COALESCE(effective_end_date, end_date)" not in combined_sql
    assert "COALESCE(registration_status, status)" not in combined_sql
    assert "CAST(id AS STRING)" in combined_sql
    assert "CAST(batch_id AS STRING)" in combined_sql
    assert "CAST(registration_month AS STRING)" in combined_sql
    assert "CAST(registration_source AS STRING)" in combined_sql


def test_build_athlete_sql_plan_for_player_assessment_history_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_athlete_sql_plan(
        config,
        context,
        target_table="player_assessment_history",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.player_assessment_history",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "ASSESS_001" in plan.rejected_sql
    assert "ASSESS_006" in plan.rejected_sql
    assert "ASSESS_DUPLICATE" in plan.rejected_sql
    assert "assessment_confidence" in plan.accepted_sql
    assert "assessor_source" in plan.accepted_sql


def test_build_athlete_sql_plan_for_player_assessment_history_uses_actual_bronze_columns() -> None:
    config, environment, context = _config_environment_context()

    plan = build_athlete_sql_plan(
        config,
        context,
        target_table="player_assessment_history",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.player_assessment_history",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
        source_columns={
            "id",
            "player_id",
            "batch_id",
            "assessment_month",
            "assessment_type",
            "value",
            "confidence",
            "_pipeline_run_id",
            "_pipeline_name",
            "_pipeline_version",
            "_release_name",
            "_source_file_name",
            "_source_file_path",
            "_source_file_size",
            "_source_file_modification_ts",
            "_ingested_ts",
            "_source_record_hash",
        },
    )
    combined_sql = "\n".join(
        [
            plan.accepted_sql,
            plan.rejected_sql,
            plan.exact_duplicate_count_sql,
            plan.business_key_duplicate_count_sql,
        ]
    )

    assert "COALESCE(assessment_id, id)" not in combined_sql
    assert "COALESCE(batch_id, monthly_batch_id)" not in combined_sql
    assert "COALESCE(assessment_value, value)" not in combined_sql
    assert "COALESCE(assessment_confidence, confidence)" not in combined_sql
    assert "CAST(id AS STRING)" in combined_sql
    assert "CAST(batch_id AS STRING)" in combined_sql
    assert "CAST(assessment_month AS STRING)" in combined_sql
    assert "CAST(value AS STRING)" in combined_sql
    assert "CAST(confidence AS STRING)" in combined_sql


def test_execute_single_table_sql_publishes_player_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {
        "published": [],
        "quality": [],
        "schema": [],
        "append_tables": [],
    }

    scalar_values = iter([8, 1, 1])

    def fake_scalar_sql_value(_spark, _query):
        return next(scalar_values)

    def fake_publish_sql_table(_spark, table_fqn, select_sql):
        captured["published"].append((table_fqn, select_sql))
        return 5 if table_fqn.endswith(".players") else 2

    def fake_append_quality_results_for_reject_table(
        _spark,
        _context,
        *,
        target_table,
        evaluated_row_count,
        reject_table_fqn,
    ):
        captured["quality"].append((target_table, evaluated_row_count, reject_table_fqn))
        return []

    def fake_append_schema_snapshot_for_table(_spark, _context, *, layer_name, table_name, table_fqn):
        captured["schema"].append((layer_name, table_name, table_fqn))

    def fake_append_records(_spark, table_fqn, records):
        captured["append_tables"].append((table_fqn, records))

    monkeypatch.setattr(execute_module, "scalar_sql_value", fake_scalar_sql_value)
    monkeypatch.setattr(execute_module, "publish_sql_table", fake_publish_sql_table)
    monkeypatch.setattr(
        execute_module,
        "append_quality_results_for_reject_table",
        fake_append_quality_results_for_reject_table,
    )
    monkeypatch.setattr(
        execute_module,
        "append_schema_snapshot_for_table",
        fake_append_schema_snapshot_for_table,
    )
    monkeypatch.setattr(execute_module, "append_records", fake_append_records)
    monkeypatch.setattr(execute_module, "append_warning_message", lambda *args, **kwargs: None)

    metrics = execute_module._execute_single_table_sql(
        spark,
        config,
        environment,
        context,
        table_config=config.data["silver_tables"]["players"],
    )

    assert metrics["source_row_count"] == 8
    assert metrics["exact_duplicate_count"] == 1
    assert metrics["business_key_duplicate_count"] == 1
    assert metrics["accepted_row_count"] == 5
    assert metrics["rejected_row_count"] == 2
    assert metrics["warning_count"] == 0
    assert len(captured["published"]) == 2
    assert captured["published"][0][0].endswith(".players")
    assert captured["published"][1][0].endswith(".players_exceptions")
    assert captured["quality"][0][0] == "players"
    assert any(table_fqn.endswith(f".{RECONCILIATION_RESULTS_TABLE}") for table_fqn, _ in captured["append_tables"])


def test_execute_single_table_sql_publishes_registration_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {
        "published": [],
        "quality": [],
        "schema": [],
        "append_tables": [],
    }

    scalar_values = iter([6, 0, 1])

    def fake_scalar_sql_value(_spark, _query):
        return next(scalar_values)

    def fake_publish_sql_table(_spark, table_fqn, select_sql):
        captured["published"].append((table_fqn, select_sql))
        return 4 if table_fqn.endswith(".player_registrations") else 1

    def fake_append_quality_results_for_reject_table(
        _spark,
        _context,
        *,
        target_table,
        evaluated_row_count,
        reject_table_fqn,
    ):
        captured["quality"].append((target_table, evaluated_row_count, reject_table_fqn))
        return []

    monkeypatch.setattr(execute_module, "scalar_sql_value", fake_scalar_sql_value)
    monkeypatch.setattr(execute_module, "publish_sql_table", fake_publish_sql_table)
    monkeypatch.setattr(
        execute_module,
        "append_quality_results_for_reject_table",
        fake_append_quality_results_for_reject_table,
    )
    monkeypatch.setattr(
        execute_module,
        "append_schema_snapshot_for_table",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(execute_module, "append_records", lambda *args, **kwargs: captured["append_tables"].append(args[1:]))
    monkeypatch.setattr(execute_module, "append_warning_message", lambda *args, **kwargs: None)

    metrics = execute_module._execute_single_table_sql(
        spark,
        config,
        environment,
        context,
        table_config=config.data["silver_tables"]["player_registrations"],
    )

    assert metrics["source_row_count"] == 6
    assert metrics["exact_duplicate_count"] == 0
    assert metrics["business_key_duplicate_count"] == 1
    assert metrics["accepted_row_count"] == 4
    assert metrics["rejected_row_count"] == 1
    assert metrics["warning_count"] == 0
    assert captured["quality"][0][0] == "player_registrations"
