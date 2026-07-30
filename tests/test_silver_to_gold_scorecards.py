"""Tests for Silver-to-Gold Phase 10 player scorecard builders."""

from datetime import date

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment
from napa_pipeline.silver_to_gold.scorecards import (
    NATIONAL_PLAYER_RANKINGS_SCHEMA,
    PLAYER_EVALUATION_SCORECARDS_SCHEMA,
    build_national_player_rankings,
    build_player_evaluation_scorecards,
    publish_national_player_rankings,
    publish_phase10_player_tables,
    publish_player_evaluation_scorecards,
)
from napa_pipeline.silver_to_gold.scorecards_validation import (
    PHASE10_REQUIRED_SOURCE_COLUMNS,
    publish_phase10_scorecard_tables,
    validate_phase10_source_contract,
)


def _scorecards_config():
    return load_silver_to_gold_config("napa_5k").data["scorecards"]


def _eligibility_config():
    return load_silver_to_gold_config("napa_5k").data["eligibility"]


class _FakeField:
    def __init__(self, name: str):
        self.name = name


class _FakeSchema:
    def __init__(self, field_names):
        self.fields = [_FakeField(name) for name in field_names]


class _FakeTable:
    def __init__(self, *, field_names=None, row_count: int = 0):
        self.schema = _FakeSchema(field_names or [])
        self._row_count = row_count

    def count(self) -> int:
        return self._row_count

    def toLocalIterator(self):
        return iter(())


class _FakeSpark:
    def __init__(self, tables):
        self._tables = tables

    def table(self, table_name: str):
        return self._tables[table_name]

    def sql(self, _query: str):
        raise RuntimeError("sql should not be called in these tests")


def _sample_scorecard_inputs():
    players = [
        {"player_id": "player-1", "display_name": "Player One", "country_code": "USA", "gender": "M", "active_flag": True},
        {"player_id": "player-2", "display_name": "Player Two", "country_code": "USA", "gender": "F", "active_flag": True},
        {"player_id": "player-3", "display_name": "Player Three", "country_code": "CAN", "gender": "M", "active_flag": True},
    ]
    current = [
        {
            "player_id": "player-1",
            "source_rating_value": 1450.0,
            "source_confidence_score": 0.8,
            "analytical_rating_value": 1600.0,
            "rated_match_count": 20,
            "rating_reliability_score": 88.0,
            "rating_evidence_band": "HIGH",
            "rating_uncertainty_proxy": 20.0,
        },
        {
            "player_id": "player-2",
            "source_rating_value": 1400.0,
            "source_confidence_score": 0.7,
            "analytical_rating_value": 1500.0,
            "rated_match_count": 12,
            "rating_reliability_score": 75.0,
            "rating_evidence_band": "MODERATE",
            "rating_uncertainty_proxy": 35.0,
        },
        {
            "player_id": "player-3",
            "source_rating_value": 1425.0,
            "source_confidence_score": 0.75,
            "analytical_rating_value": 1550.0,
            "rated_match_count": 15,
            "rating_reliability_score": 80.0,
            "rating_evidence_band": "HIGH",
            "rating_uncertainty_proxy": 25.0,
        },
    ]
    performance = [
        {"player_id": "player-1", "evidence_window": "career", "match_count": 20, "performance_above_expectation": 0.12, "game_win_pct": 0.65, "avg_point_share": 0.58, "win_pct": 0.7, "recency_weighted_win_pct": 0.68, "consistency_score": 82.0, "strength_of_schedule": 1650.0},
        {"player_id": "player-1", "evidence_window": "trailing_90", "match_count": 6, "performance_above_expectation": 0.15, "game_win_pct": 0.67, "avg_point_share": 0.6, "win_pct": 0.75, "recency_weighted_win_pct": 0.72, "consistency_score": 84.0, "strength_of_schedule": 1675.0},
        {"player_id": "player-2", "evidence_window": "career", "match_count": 12, "performance_above_expectation": 0.03, "game_win_pct": 0.56, "avg_point_share": 0.52, "win_pct": 0.58, "recency_weighted_win_pct": 0.57, "consistency_score": 66.0, "strength_of_schedule": 1580.0},
        {"player_id": "player-2", "evidence_window": "trailing_90", "match_count": 5, "performance_above_expectation": 0.02, "game_win_pct": 0.55, "avg_point_share": 0.51, "win_pct": 0.56, "recency_weighted_win_pct": 0.55, "consistency_score": 64.0, "strength_of_schedule": 1590.0},
        {"player_id": "player-3", "evidence_window": "career", "match_count": 15, "performance_above_expectation": 0.08, "game_win_pct": 0.61, "avg_point_share": 0.55, "win_pct": 0.63, "recency_weighted_win_pct": 0.6, "consistency_score": 74.0, "strength_of_schedule": 1610.0},
        {"player_id": "player-3", "evidence_window": "trailing_90", "match_count": 4, "performance_above_expectation": 0.09, "game_win_pct": 0.62, "avg_point_share": 0.56, "win_pct": 0.64, "recency_weighted_win_pct": 0.61, "consistency_score": 75.0, "strength_of_schedule": 1625.0},
    ]
    development = [
        {"player_id": "player-1", "latest_assessment_confidence": 0.9, "development_momentum_score": 70.0, "feature_evidence_status": "SUFFICIENT"},
        {"player_id": "player-2", "latest_assessment_confidence": 0.8, "development_momentum_score": 55.0, "feature_evidence_status": "LIMITED"},
        {"player_id": "player-3", "latest_assessment_confidence": 0.85, "development_momentum_score": 62.0, "feature_evidence_status": "SUFFICIENT"},
    ]
    confidence = [
        {"entity_type": "PLAYER", "entity_id": "player-1", "data_quality_confidence_score": 90.0, "quality_confidence_band": "HIGH", "material_limitation_text": None},
        {"entity_type": "PLAYER", "entity_id": "player-2", "data_quality_confidence_score": 65.0, "quality_confidence_band": "MODERATE", "material_limitation_text": "limited match volume"},
        {"entity_type": "PLAYER", "entity_id": "player-3", "data_quality_confidence_score": 80.0, "quality_confidence_band": "HIGH", "material_limitation_text": None},
    ]
    return players, current, performance, development, confidence


def test_build_player_evaluation_scorecards_applies_confidence_adjustment() -> None:
    players, current, performance, development, confidence = _sample_scorecard_inputs()

    rows = build_player_evaluation_scorecards(
        players_rows=players,
        player_current_ratings_rows=current,
        player_performance_features_rows=performance,
        player_development_features_rows=development,
        entity_data_quality_confidence_rows=confidence,
        analysis_as_of_date=date(2026, 6, 30),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    assert len(rows) == 3
    first_row = next(row for row in rows if row["player_id"] == "player-1")
    assert first_row["confidence_adjusted_player_score"] <= 100.0
    assert first_row["raw_player_evaluation_score"] is not None
    assert first_row["confidence_adjusted_player_score"] >= first_row["raw_player_evaluation_score"] * 0.5
    assert first_row["top_strengths"] is not None


def test_build_player_evaluation_scorecards_reweights_missing_components() -> None:
    players, current, performance, development, confidence = _sample_scorecard_inputs()
    development = [row for row in development if row["player_id"] != "player-2"]

    rows = build_player_evaluation_scorecards(
        players_rows=players,
        player_current_ratings_rows=current,
        player_performance_features_rows=performance,
        player_development_features_rows=development,
        entity_data_quality_confidence_rows=confidence,
        analysis_as_of_date=date(2026, 6, 30),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    player_two = next(row for row in rows if row["player_id"] == "player-2")
    assert player_two["development_component_score"] is None
    assert player_two["raw_player_evaluation_score"] is not None


def test_build_national_player_rankings_creates_overall_and_gender_groups() -> None:
    players, current, performance, development, confidence = _sample_scorecard_inputs()
    scorecards = build_player_evaluation_scorecards(
        players_rows=players,
        player_current_ratings_rows=current,
        player_performance_features_rows=performance,
        player_development_features_rows=development,
        entity_data_quality_confidence_rows=confidence,
        analysis_as_of_date=date(2026, 6, 30),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    rankings = build_national_player_rankings(
        player_scorecard_rows=scorecards,
        scoring_scenario="BALANCED",
        eligibility_config=_eligibility_config(),
    )

    assert any(row["ranking_group"] == "OVERALL_CURRENT" for row in rankings)
    assert any(row["ranking_group"] == "OVERALL_DEVELOPMENT" for row in rankings)
    assert any(str(row["ranking_group"]).startswith("GENDER_CURRENT_") for row in rankings)


def test_validate_phase10_source_contract_checks_gold_and_silver_tables() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    tables = {}
    for logical_name, (layer, table_name, required_columns) in PHASE10_REQUIRED_SOURCE_COLUMNS.items():
        schema_name = environment.gold_schema if layer == "gold" else environment.silver_schema
        table_fqn = f"{environment.catalog}.{schema_name}.{table_name}"
        tables[table_fqn] = _FakeTable(field_names=required_columns)

    validated = validate_phase10_source_contract(_FakeSpark(tables), environment)

    assert set(validated) == set(PHASE10_REQUIRED_SOURCE_COLUMNS)


def test_publish_player_evaluation_scorecards_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.player_evaluation_scorecards"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.player_evaluation_scorecards"
    spark = _FakeSpark({target_fqn: _FakeTable(row_count=3)})

    published = {}

    def _fake_publish_stage_records_to_gold_table(*args, **kwargs):
        published["schema"] = kwargs.get("schema")
        return 3, 3

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.scorecards.publish_stage_records_to_gold_table",
        _fake_publish_stage_records_to_gold_table,
    )

    summary = publish_player_evaluation_scorecards(
        spark,
        environment,
        rows=({"player_id": "player-1", "scoring_scenario": "BALANCED"},),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.output_row_count == 3
    assert published["schema"] is PLAYER_EVALUATION_SCORECARDS_SCHEMA


def test_publish_national_player_rankings_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.national_player_rankings"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.national_player_rankings"
    spark = _FakeSpark({target_fqn: _FakeTable(row_count=8)})

    published = {}

    def _fake_publish_stage_records_to_gold_table(*args, **kwargs):
        published["schema"] = kwargs.get("schema")
        return 8, 8

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.scorecards.publish_stage_records_to_gold_table",
        _fake_publish_stage_records_to_gold_table,
    )

    summary = publish_national_player_rankings(
        spark,
        environment,
        rows=(
            {
                "country_code": "USA",
                "ranking_group": "OVERALL_CURRENT",
                "player_id": "player-1",
                "scoring_scenario": "BALANCED",
            },
        ),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.output_row_count == 8
    assert published["schema"] is NATIONAL_PLAYER_RANKINGS_SCHEMA


def test_publish_phase10_scorecard_tables_returns_two_summaries(monkeypatch) -> None:
    player_summary = object()
    ranking_summary = object()

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.scorecards_validation.publish_phase10_player_tables",
        lambda *args, **kwargs: type(
            "_Summary",
            (),
            {
                "player_evaluation_scorecards": player_summary,
                "national_player_rankings": ranking_summary,
            },
        )(),
    )

    summary = publish_phase10_scorecard_tables(
        spark=None,
        environment=None,
        analysis_as_of_date=date(2026, 6, 30),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    assert summary.player_evaluation_scorecards is player_summary
    assert summary.national_player_rankings is ranking_summary
