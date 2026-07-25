"""Tests for Silver-to-Gold Phase 6 player feature builders."""

from datetime import date

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment
from napa_pipeline.silver_to_gold.features import (
    build_player_performance_features_sql,
    build_player_development_features,
    build_player_performance_features,
    get_player_feature_registry,
    publish_player_performance_features,
)
from napa_pipeline.silver_to_gold.features_validation import (
    PHASE6_REQUIRED_SOURCE_COLUMNS,
    publish_phase6_feature_tables,
    validate_phase6_source_contract,
)


def _features_config():
    return load_silver_to_gold_config("napa_5k").data["features"]


def _evidence_windows_config():
    return load_silver_to_gold_config("napa_5k").data["evidence_windows"]


def _competition_player_matches_rows():
    return [
        {
            "match_id": "match-1",
            "match_date": "2026-06-10",
            "batch_sequence": 1,
            "player_id": "player-1",
            "partner_player_id": "player-2",
            "won_flag": True,
            "lost_flag": False,
            "games_won": 2,
            "games_lost": 1,
            "point_share": 0.60,
            "point_differential": 5,
            "pre_match_team_rating": 1500.0,
            "pre_match_opponent_team_rating": 1480.0,
        },
        {
            "match_id": "match-2",
            "match_date": "2026-04-01",
            "batch_sequence": 1,
            "player_id": "player-1",
            "partner_player_id": "player-3",
            "won_flag": False,
            "lost_flag": True,
            "games_won": 1,
            "games_lost": 2,
            "point_share": 0.45,
            "point_differential": -3,
            "pre_match_team_rating": 1490.0,
            "pre_match_opponent_team_rating": 1510.0,
        },
        {
            "match_id": "match-3",
            "match_date": "2025-12-15",
            "batch_sequence": 1,
            "player_id": "player-1",
            "partner_player_id": "player-2",
            "won_flag": True,
            "lost_flag": False,
            "games_won": 2,
            "games_lost": 0,
            "point_share": 0.70,
            "point_differential": 9,
            "pre_match_team_rating": 1475.0,
            "pre_match_opponent_team_rating": 1460.0,
        },
        {
            "match_id": "match-4",
            "match_date": "2026-06-20",
            "batch_sequence": 1,
            "player_id": "player-2",
            "partner_player_id": "player-1",
            "won_flag": False,
            "lost_flag": True,
            "games_won": 0,
            "games_lost": 2,
            "point_share": 0.35,
            "point_differential": -8,
            "pre_match_team_rating": 1450.0,
            "pre_match_opponent_team_rating": 1525.0,
        },
    ]


def _player_current_ratings_rows():
    return [
        {"player_id": "player-1", "analytical_rating_value": 1510.0, "rated_match_count": 3},
        {"player_id": "player-2", "analytical_rating_value": 1460.0, "rated_match_count": 1},
        {"player_id": "player-3", "analytical_rating_value": 1500.0, "rated_match_count": 0},
    ]


def _players_rows():
    return [
        {"player_id": "player-1", "display_name": "Player One", "country_code": "USA", "active_flag": True},
        {"player_id": "player-2", "display_name": "Player Two", "country_code": "USA", "active_flag": True},
        {"player_id": "player-3", "display_name": "Player Three", "country_code": "CAN", "active_flag": False},
    ]


def _player_rating_history_rows():
    return [
        {
            "player_id": "player-1",
            "rating_effective_date": "2026-01-01",
            "analytical_rating_value": 1450.0,
            "rated_match_count": 1,
        },
        {
            "player_id": "player-1",
            "rating_effective_date": "2026-03-01",
            "analytical_rating_value": 1475.0,
            "rated_match_count": 2,
        },
        {
            "player_id": "player-1",
            "rating_effective_date": "2026-06-15",
            "analytical_rating_value": 1510.0,
            "rated_match_count": 3,
        },
        {
            "player_id": "player-2",
            "rating_effective_date": "2026-06-20",
            "analytical_rating_value": 1460.0,
            "rated_match_count": 1,
        },
    ]


def _player_assessment_history_rows():
    return [
        {
            "player_id": "player-1",
            "assessment_date": "2026-01-15",
            "assessment_value": 0.40,
            "assessment_confidence": 0.50,
        },
        {
            "player_id": "player-1",
            "assessment_date": "2026-03-15",
            "assessment_value": 0.55,
            "assessment_confidence": 0.60,
        },
        {
            "player_id": "player-1",
            "assessment_date": "2026-06-10",
            "assessment_value": 0.75,
            "assessment_confidence": 0.80,
        },
        {
            "player_id": "player-2",
            "assessment_date": "2026-06-18",
            "assessment_value": 0.42,
            "assessment_confidence": 0.55,
        },
    ]


def _player_registrations_rows():
    return [
        {"player_id": "player-1", "registration_date": "2025-01-01", "current_registration_flag": True},
        {"player_id": "player-2", "registration_date": "2026-01-15", "current_registration_flag": True},
        {"player_id": "player-3", "registration_date": "2026-06-01", "current_registration_flag": False},
    ]


def test_build_player_performance_features_emits_all_windows_and_partner_context() -> None:
    result = build_player_performance_features(
        _competition_player_matches_rows(),
        _player_current_ratings_rows(),
        _players_rows(),
        analysis_as_of_date=date(2026, 6, 30),
        features_config=_features_config(),
        evidence_windows_config=_evidence_windows_config(),
    )

    assert len(result.rows) == 12
    player_one_career = next(
        row
        for row in result.rows
        if row["player_id"] == "player-1" and row["evidence_window"] == "career"
    )
    player_one_recent = next(
        row
        for row in result.rows
        if row["player_id"] == "player-1" and row["evidence_window"] == "trailing_90"
    )
    player_three_career = next(
        row
        for row in result.rows
        if row["player_id"] == "player-3" and row["evidence_window"] == "career"
    )

    assert player_one_career["match_count"] == 3
    assert player_one_career["win_count"] == 2
    assert player_one_career["distinct_partner_count"] == 2
    assert round(player_one_career["primary_partner_match_pct"], 6) == round(2 / 3, 6)
    assert player_one_career["performance_with_multiple_partners_flag"] is True
    assert player_one_career["consistency_score"] is None
    assert player_one_career["feature_evidence_status"] == "LIMITED"
    assert player_one_recent["match_count"] == 2
    assert player_one_recent["win_pct"] == 0.5
    assert player_three_career["match_count"] == 0
    assert player_three_career["feature_evidence_status"] == "NONE"


def test_build_player_development_features_calculates_trends_and_momentum() -> None:
    result = build_player_development_features(
        _player_rating_history_rows(),
        _player_assessment_history_rows(),
        _player_registrations_rows(),
        _players_rows(),
        analysis_as_of_date=date(2026, 6, 30),
        features_config=_features_config(),
        evidence_windows_config=_evidence_windows_config(),
    )

    assert len(result.rows) == 3
    player_one = next(row for row in result.rows if row["player_id"] == "player-1")
    player_three = next(row for row in result.rows if row["player_id"] == "player-3")

    assert player_one["rating_change_total"] == 60.0
    assert player_one["rating_change_180"] == 60.0
    assert player_one["assessment_change_180"] == 0.35
    assert round(player_one["confidence_change_180"], 6) == 0.30
    assert player_one["experience_growth_180"] == 2
    assert player_one["development_momentum_score"] > 0.0
    assert player_one["feature_evidence_status"] == "SUFFICIENT"
    assert player_three["feature_evidence_status"] == "NONE"


def test_get_player_feature_registry_contains_phase6_entries() -> None:
    registry = get_player_feature_registry()

    assert len(registry) >= 7
    assert any(entry["feature_name"] == "win_pct" for entry in registry)
    assert any(entry["feature_name"] == "development_momentum_score" for entry in registry)


class _FakeField:
    def __init__(self, name: str):
        self.name = name


class _FakeSchema:
    def __init__(self, field_names):
        self.fields = [_FakeField(name) for name in field_names]


class _FakeTable:
    def __init__(self, *, field_names=None, row_count: int = 0, rows=None):
        self.schema = _FakeSchema(field_names or [])
        self._row_count = row_count
        self._rows = list(rows or [])

    def count(self) -> int:
        return self._row_count

    def toLocalIterator(self):
        return iter(self._rows)


class _FakeSpark:
    def __init__(self, tables):
        self._tables = tables

    def table(self, table_name: str):
        return self._tables[table_name]


def test_validate_phase6_source_contract_checks_gold_and_silver_tables() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    tables = {}
    for logical_name, (layer, table_name, required_columns) in PHASE6_REQUIRED_SOURCE_COLUMNS.items():
        schema_name = environment.gold_schema if layer == "gold" else environment.silver_schema
        table_fqn = f"{environment.catalog}.{schema_name}.{table_name}"
        tables[table_fqn] = _FakeTable(field_names=required_columns)

    validated = validate_phase6_source_contract(_FakeSpark(tables), environment)

    assert set(validated) == set(PHASE6_REQUIRED_SOURCE_COLUMNS)


def test_publish_player_performance_features_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    competition_fqn = f"{environment.catalog}.{environment.gold_schema}.competition_player_matches"
    current_fqn = f"{environment.catalog}.{environment.gold_schema}.player_current_ratings"
    players_fqn = f"{environment.catalog}.{environment.silver_schema}.players"
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.player_performance_features"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.player_performance_features"
    spark = _FakeSpark(
        {
            competition_fqn: _FakeTable(rows=_competition_player_matches_rows()),
            current_fqn: _FakeTable(row_count=3, rows=_player_current_ratings_rows()),
            players_fqn: _FakeTable(rows=_players_rows()),
            target_fqn: _FakeTable(row_count=12),
        }
    )
    published = {}

    def _fake_publish_stage_to_gold_table(
        _spark,
        *,
        stage_table_fqn: str,
        target_table_fqn: str,
        stage_sql: str,
        validation_fn=None,
        count_fn=None,
    ):
        published["stage_table_fqn"] = stage_table_fqn
        published["target_table_fqn"] = target_table_fqn
        published["stage_sql"] = stage_sql
        return 12, 12

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.features.publish_stage_to_gold_table",
        _fake_publish_stage_to_gold_table,
    )

    summary = publish_player_performance_features(
        spark,
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        features_config=_features_config(),
        evidence_windows_config=_evidence_windows_config(),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 3
    assert summary.output_row_count == 12
    assert "competition_player_matches" in published["stage_sql"]
    assert "trailing_365" in published["stage_sql"]


def test_build_player_performance_features_sql_uses_spark_window_aggregations() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_player_performance_features_sql(
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        features_config=_features_config(),
        evidence_windows_config=_evidence_windows_config(),
    )

    assert "NTILE(4) OVER" in sql
    assert "STDDEV_POP(point_share)" in sql
    assert "CROSS JOIN windows" in sql
    assert "toLocalIterator" not in sql


def test_publish_phase6_feature_tables_returns_two_summaries(monkeypatch) -> None:
    performance_summary = object()
    development_summary = object()

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.features_validation.publish_player_performance_features",
        lambda *args, **kwargs: performance_summary,
    )
    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.features_validation.publish_player_development_features",
        lambda *args, **kwargs: development_summary,
    )

    summary = publish_phase6_feature_tables(
        spark=None,
        environment=None,
        analysis_as_of_date=date(2026, 6, 30),
        features_config=_features_config(),
        evidence_windows_config=_evidence_windows_config(),
    )

    assert summary.player_performance_features is performance_summary
    assert summary.player_development_features is development_summary
