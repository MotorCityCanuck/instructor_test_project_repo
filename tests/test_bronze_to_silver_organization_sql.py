"""Tests for Bronze-to-Silver organization-stage Spark SQL execution."""

import napa_pipeline.bronze_to_silver.execute as execute_module
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context
from napa_pipeline.bronze_to_silver.organization_sql import (
    build_organization_sql_plan,
    supports_organization_sql_transform,
)


class DummySpark:
    pass


def _config_environment_context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment, pipeline_run_id="run-123")
    return config, environment, context


def test_supports_organization_sql_transform_for_parent_tables_only() -> None:
    assert supports_organization_sql_transform("build_clubs") is True
    assert supports_organization_sql_transform("build_teams") is True
    assert supports_organization_sql_transform("build_club_memberships") is True
    assert supports_organization_sql_transform("build_team_memberships") is True
    assert supports_organization_sql_transform("build_matches") is False


def test_build_organization_sql_plan_for_clubs_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_organization_sql_plan(
        config,
        context,
        target_table="clubs",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.clubs",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "CLUB_001" in plan.rejected_sql
    assert "CLUB_007" in plan.rejected_sql
    assert "CLUB_DUPLICATE" in plan.rejected_sql
    assert "region_sk" in plan.accepted_sql
    assert "'CANADA'" in plan.accepted_sql


def test_build_organization_sql_plan_for_teams_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_organization_sql_plan(
        config,
        context,
        target_table="teams",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.teams",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "TEAM_001" in plan.rejected_sql
    assert "TEAM_007" in plan.rejected_sql
    assert "TEAM_DUPLICATE" in plan.rejected_sql
    assert "team_age_days" in plan.accepted_sql
    assert "team_category" in plan.accepted_sql


def test_build_organization_sql_plan_for_club_memberships_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_organization_sql_plan(
        config,
        context,
        target_table="club_memberships",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.club_memberships",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "CLUB_MEMBERSHIP_001" in plan.rejected_sql
    assert "CLUB_MEMBERSHIP_006" in plan.rejected_sql
    assert "CLUB_MEMBERSHIP_DUPLICATE" in plan.rejected_sql
    assert "membership_overlap_flag" in plan.accepted_sql
    assert plan.warning_count_sql is not None


def test_build_organization_sql_plan_for_team_memberships_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_organization_sql_plan(
        config,
        context,
        target_table="team_memberships",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.team_memberships",
        silver_schema_fqn=f"{environment.catalog}.{environment.silver_schema}",
    )

    assert "TEAM_MEMBERSHIP_001" in plan.rejected_sql
    assert "TEAM_MEMBERSHIP_007" in plan.rejected_sql
    assert "TEAM_MEMBERSHIP_DUPLICATE" in plan.rejected_sql
    assert "player_position" in plan.accepted_sql
    assert plan.warning_count_sql is not None


def test_execute_single_table_sql_publishes_club_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {"published": [], "quality": [], "append_tables": []}

    scalar_values = iter([7, 1, 1])

    monkeypatch.setattr(execute_module, "scalar_sql_value", lambda *_args, **_kwargs: next(scalar_values))
    monkeypatch.setattr(
        execute_module,
        "publish_sql_table",
        lambda _spark, table_fqn, select_sql: captured["published"].append((table_fqn, select_sql)) or (4 if table_fqn.endswith(".clubs") else 2),
    )
    monkeypatch.setattr(
        execute_module,
        "append_quality_results_for_reject_table",
        lambda _spark, _context, *, target_table, evaluated_row_count, reject_table_fqn: captured["quality"].append((target_table, evaluated_row_count, reject_table_fqn)) or [],
    )
    monkeypatch.setattr(execute_module, "append_schema_snapshot_for_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(execute_module, "append_records", lambda *args, **kwargs: captured["append_tables"].append(args[1:]))
    monkeypatch.setattr(execute_module, "append_warning_message", lambda *args, **kwargs: None)

    metrics = execute_module._execute_single_table_sql(
        spark,
        config,
        environment,
        context,
        table_config=config.data["silver_tables"]["clubs"],
    )

    assert metrics["source_row_count"] == 7
    assert metrics["exact_duplicate_count"] == 1
    assert metrics["business_key_duplicate_count"] == 1
    assert metrics["accepted_row_count"] == 4
    assert metrics["rejected_row_count"] == 2
    assert captured["quality"][0][0] == "clubs"


def test_execute_single_table_sql_publishes_team_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {"published": [], "quality": []}

    scalar_values = iter([5, 0, 1])

    monkeypatch.setattr(execute_module, "scalar_sql_value", lambda *_args, **_kwargs: next(scalar_values))
    monkeypatch.setattr(
        execute_module,
        "publish_sql_table",
        lambda _spark, table_fqn, select_sql: captured["published"].append((table_fqn, select_sql)) or (3 if table_fqn.endswith(".teams") else 1),
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
        table_config=config.data["silver_tables"]["teams"],
    )

    assert metrics["source_row_count"] == 5
    assert metrics["exact_duplicate_count"] == 0
    assert metrics["business_key_duplicate_count"] == 1
    assert metrics["accepted_row_count"] == 3
    assert metrics["rejected_row_count"] == 1
    assert captured["quality"][0][0] == "teams"


def test_execute_single_table_sql_publishes_club_membership_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {"published": [], "quality": []}

    scalar_values = iter([6, 0, 1, 1])

    monkeypatch.setattr(execute_module, "scalar_sql_value", lambda *_args, **_kwargs: next(scalar_values))
    monkeypatch.setattr(
        execute_module,
        "publish_sql_table",
        lambda _spark, table_fqn, select_sql: captured["published"].append((table_fqn, select_sql)) or (4 if table_fqn.endswith(".club_memberships") else 1),
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
        table_config=config.data["silver_tables"]["club_memberships"],
    )

    assert metrics["source_row_count"] == 6
    assert metrics["exact_duplicate_count"] == 0
    assert metrics["business_key_duplicate_count"] == 1
    assert metrics["accepted_row_count"] == 4
    assert metrics["rejected_row_count"] == 1
    assert metrics["warning_count"] == 1
    assert captured["quality"][0][0] == "club_memberships"


def test_execute_single_table_sql_publishes_team_membership_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {"published": [], "quality": []}

    scalar_values = iter([7, 1, 1, 1])

    monkeypatch.setattr(execute_module, "scalar_sql_value", lambda *_args, **_kwargs: next(scalar_values))
    monkeypatch.setattr(
        execute_module,
        "publish_sql_table",
        lambda _spark, table_fqn, select_sql: captured["published"].append((table_fqn, select_sql)) or (5 if table_fqn.endswith(".team_memberships") else 2),
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
        table_config=config.data["silver_tables"]["team_memberships"],
    )

    assert metrics["source_row_count"] == 7
    assert metrics["exact_duplicate_count"] == 1
    assert metrics["business_key_duplicate_count"] == 1
    assert metrics["accepted_row_count"] == 5
    assert metrics["rejected_row_count"] == 2
    assert metrics["warning_count"] == 1
    assert captured["quality"][0][0] == "team_memberships"
