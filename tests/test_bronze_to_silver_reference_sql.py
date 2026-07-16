"""Tests for Bronze-to-Silver reference-stage Spark SQL execution."""

import napa_pipeline.bronze_to_silver.execute as execute_module
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context
from napa_pipeline.bronze_to_silver.reference_sql import (
    build_reference_sql_plan,
    supports_reference_sql_transform,
)


class DummySpark:
    pass


def _config_environment_context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment, pipeline_run_id="run-123")
    return config, environment, context


def test_supports_reference_sql_transform_only_for_reference_tables() -> None:
    assert supports_reference_sql_transform("build_monthly_batches") is True
    assert supports_reference_sql_transform("build_regions") is True
    assert supports_reference_sql_transform("build_players") is False


def test_build_reference_sql_plan_for_monthly_batches_contains_expected_rules() -> None:
    config, environment, context = _config_environment_context()

    plan = build_reference_sql_plan(
        config,
        context,
        target_table="monthly_batches",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.monthly_batches",
    )

    assert "BATCH_001" in plan.rejected_sql
    assert "BATCH_002" in plan.rejected_sql
    assert "BATCH_DUPLICATE" in plan.rejected_sql
    assert "batch_quarter" in plan.accepted_sql
    assert "CREATE OR REPLACE" not in plan.accepted_sql


def test_build_reference_sql_plan_for_regions_contains_country_normalization() -> None:
    config, environment, context = _config_environment_context()

    plan = build_reference_sql_plan(
        config,
        context,
        target_table="regions",
        source_table_fqn=f"{environment.catalog}.{environment.bronze_schema}.regions",
    )

    assert "REGION_001" in plan.rejected_sql
    assert "REGION_003" in plan.rejected_sql
    assert "'CANADA'" in plan.accepted_sql
    assert "'CAN'" in plan.accepted_sql


def test_execute_single_table_sql_publishes_reference_outputs(monkeypatch) -> None:
    config, environment, context = _config_environment_context()
    spark = DummySpark()
    captured = {
        "published": [],
        "quality": [],
        "schema": [],
        "append_tables": [],
    }

    scalar_values = iter([5, 1, 1])

    def fake_scalar_sql_value(_spark, _query):
        return next(scalar_values)

    def fake_publish_sql_table(_spark, table_fqn, select_sql):
        captured["published"].append((table_fqn, select_sql))
        return 3 if table_fqn.endswith(".monthly_batches") else 1

    def fake_append_quality_results_for_reject_table(
        _spark,
        _context,
        *,
        target_table,
        evaluated_row_count,
        reject_table_fqn,
    ):
        captured["quality"].append((target_table, evaluated_row_count, reject_table_fqn))
        return [
            {
                "severity": "WARNING",
                "failed_row_count": 1,
            }
        ]

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
        table_config=config.data["silver_tables"]["monthly_batches"],
    )

    assert metrics["source_row_count"] == 5
    assert metrics["exact_duplicate_count"] == 1
    assert metrics["business_key_duplicate_count"] == 1
    assert metrics["accepted_row_count"] == 3
    assert metrics["rejected_row_count"] == 1
    assert metrics["warning_count"] == 1
    assert len(captured["published"]) == 2
    assert captured["published"][0][0].endswith(".monthly_batches")
    assert captured["published"][1][0].endswith(".monthly_batches_exceptions")
    assert captured["quality"][0][0] == "monthly_batches"
    assert any(table_fqn.endswith(".reconciliation_results") for table_fqn, _ in captured["append_tables"])
