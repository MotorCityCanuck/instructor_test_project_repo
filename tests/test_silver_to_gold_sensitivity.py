"""Tests for Silver-to-Gold Phase 13 sensitivity builders."""

from datetime import date

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment
from napa_pipeline.silver_to_gold.sensitivity import (
    build_recommendation_explanations_sql,
    build_selection_sensitivity_results_sql,
    _scenario_weights,
)
from napa_pipeline.silver_to_gold.sensitivity_validation import (
    PHASE13_REQUIRED_SOURCE_COLUMNS,
    publish_phase13_sensitivity_products,
    validate_phase13_source_contract,
)


def _config():
    return load_silver_to_gold_config("napa_5k")


def _environment():
    return resolve_release_environment(_config())


class _FakeField:
    def __init__(self, name: str):
        self.name = name


class _FakeSchema:
    def __init__(self, field_names):
        self.fields = [_FakeField(name) for name in field_names]


class _FakeTable:
    def __init__(self, *, field_names=None):
        self.schema = _FakeSchema(field_names or [])


class _FakeSpark:
    def __init__(self, tables):
        self._tables = tables

    def table(self, table_name: str):
        return self._tables[table_name]


def test_scenario_weights_cover_all_approved_scenarios() -> None:
    base_weights = {
        "partnership": 0.35,
        "player_strength": 0.30,
        "prediction": 0.20,
        "confidence": 0.15,
    }

    balanced = _scenario_weights("BALANCED", base_weights=base_weights)
    confidence = _scenario_weights("CONFIDENCE_CONSERVATIVE", base_weights=base_weights)
    development = _scenario_weights("DEVELOPMENT_ORIENTED", base_weights=base_weights)

    assert balanced == base_weights
    assert confidence["confidence"] == 0.50
    assert development["player_strength"] == 0.45


def test_build_selection_sensitivity_results_sql_contains_required_scenarios_and_fields() -> None:
    config = _config()
    sql = build_selection_sensitivity_results_sql(
        _environment(),
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        release_name="napa_5k",
        release_role="development",
        authoritative_recommendation_flag=False,
        methodology_version="1.0.0",
        scorecards_config=config.data["scorecards"],
        eligibility_config=config.data["eligibility"],
        sensitivity_config=config.data["sensitivity"],
    )

    assert "PERFORMANCE_HEAVY" in sql
    assert "selection_frequency" in sql
    assert "recommendation_stability_score" in sql
    assert "scenario_recommendation_status" in sql


def test_build_recommendation_explanations_sql_contains_human_review_fields() -> None:
    sql = build_recommendation_explanations_sql(
        _environment(),
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        release_name="napa_5k",
        release_role="development",
        authoritative_recommendation_flag=False,
        methodology_version="1.0.0",
    )

    assert "human_review_status" in sql
    assert "human_override_flag" in sql
    assert "headline_rationale" in sql
    assert "sensitivity_summary" in sql


def test_validate_phase13_source_contract_accepts_required_columns() -> None:
    environment = _environment()
    spark = _FakeSpark(
        {
            f"{environment.catalog}.{environment.gold_schema}.{table_name}": _FakeTable(
                field_names=columns
            )
            for table_name, columns in PHASE13_REQUIRED_SOURCE_COLUMNS.items()
        }
    )

    validated_columns = validate_phase13_source_contract(spark, environment)

    assert validated_columns == PHASE13_REQUIRED_SOURCE_COLUMNS


def test_publish_phase13_sensitivity_products_delegates_to_builder(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_publish(
        spark,
        environment,
        *,
        analysis_as_of_date,
        scoring_scenario,
        release_name,
        release_role,
        authoritative_recommendation_flag,
        pipeline_version,
        scorecards_config,
        eligibility_config,
        sensitivity_config,
    ):
        captured.update(
            {
                "analysis_as_of_date": analysis_as_of_date,
                "scoring_scenario": scoring_scenario,
                "release_name": release_name,
                "release_role": release_role,
                "authoritative_recommendation_flag": authoritative_recommendation_flag,
                "pipeline_version": pipeline_version,
                "scorecards_config": scorecards_config,
                "eligibility_config": eligibility_config,
                "sensitivity_config": sensitivity_config,
            }
        )
        return "summary"

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.sensitivity_validation.publish_phase13_sensitivity_tables",
        _fake_publish,
    )

    config = _config()
    summary = publish_phase13_sensitivity_products(
        spark=object(),
        environment=_environment(),
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        release_name="napa_5k",
        release_role="development",
        authoritative_recommendation_flag=False,
        pipeline_version="1.0.0",
        scorecards_config=config.data["scorecards"],
        eligibility_config=config.data["eligibility"],
        sensitivity_config=config.data["sensitivity"],
    )

    assert summary == "summary"
    assert captured["sensitivity_config"] == config.data["sensitivity"]
