"""Tests for Silver-to-Gold Phase 3 competition foundation builders."""

from datetime import date

from napa_pipeline.silver_to_gold.competition import (
    build_competition_match_sides,
    build_competition_match_sides_sql,
    build_competition_player_matches,
    build_competition_player_matches_sql,
    publish_competition_match_sides,
    publish_competition_player_matches,
)
from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment


def _matches_rows():
    return [
        {
            "match_id": "match-1",
            "batch_id": "batch-1",
            "region_id": "region-1",
            "match_date": "2026-06-10",
            "match_type": "TOURNAMENT",
            "competition_category": "TOURNAMENT",
            "winning_team_id": "team-1",
            "winning_team_number": 1,
            "completed_flag": True,
        },
        {
            "match_id": "match-invalid-winner",
            "batch_id": "batch-1",
            "region_id": "region-1",
            "match_date": "2026-06-11",
            "match_type": "LEAGUE",
            "competition_category": "LEAGUE",
            "winning_team_id": "team-x",
            "winning_team_number": 3,
            "completed_flag": True,
        },
        {
            "match_id": "match-future",
            "batch_id": "batch-1",
            "region_id": "region-1",
            "match_date": "2026-07-01",
            "match_type": "LEAGUE",
            "competition_category": "LEAGUE",
            "winning_team_id": "team-3",
            "winning_team_number": 1,
            "completed_flag": True,
        },
        {
            "match_id": "match-shared-player",
            "batch_id": "batch-1",
            "region_id": "region-1",
            "match_date": "2026-06-12",
            "match_type": "LEAGUE",
            "competition_category": "LEAGUE",
            "winning_team_id": "team-5",
            "winning_team_number": 1,
            "completed_flag": True,
        },
    ]


def _match_teams_rows():
    return [
        {
            "match_team_id": "mt-1a",
            "match_id": "match-1",
            "team_id": "team-1",
            "team_number": 1,
            "pre_match_team_rating": 4.25,
            "side_cardinality_warning_flag": False,
        },
        {
            "match_team_id": "mt-1b",
            "match_id": "match-1",
            "team_id": "team-2",
            "team_number": 2,
            "pre_match_team_rating": 4.0,
            "side_cardinality_warning_flag": False,
        },
        {
            "match_team_id": "mt-invalid-a",
            "match_id": "match-invalid-winner",
            "team_id": "team-x",
            "team_number": 1,
            "pre_match_team_rating": 3.5,
            "side_cardinality_warning_flag": False,
        },
        {
            "match_team_id": "mt-invalid-b",
            "match_id": "match-invalid-winner",
            "team_id": "team-y",
            "team_number": 2,
            "pre_match_team_rating": 3.6,
            "side_cardinality_warning_flag": False,
        },
        {
            "match_team_id": "mt-future-a",
            "match_id": "match-future",
            "team_id": "team-3",
            "team_number": 1,
            "pre_match_team_rating": 3.7,
            "side_cardinality_warning_flag": False,
        },
        {
            "match_team_id": "mt-future-b",
            "match_id": "match-future",
            "team_id": "team-4",
            "team_number": 2,
            "pre_match_team_rating": 3.8,
            "side_cardinality_warning_flag": False,
        },
        {
            "match_team_id": "mt-shared-a",
            "match_id": "match-shared-player",
            "team_id": "team-5",
            "team_number": 1,
            "pre_match_team_rating": 4.1,
            "side_cardinality_warning_flag": False,
        },
        {
            "match_team_id": "mt-shared-b",
            "match_id": "match-shared-player",
            "team_id": "team-6",
            "team_number": 2,
            "pre_match_team_rating": 4.0,
            "side_cardinality_warning_flag": False,
        },
    ]


def _match_team_players_rows():
    return [
        {"match_team_id": "mt-1a", "match_id": "match-1", "player_id": "player-2", "player_position": "RIGHT", "player_rating_at_match": 4.1, "membership_history_warning_flag": False},
        {"match_team_id": "mt-1a", "match_id": "match-1", "player_id": "player-1", "player_position": "LEFT", "player_rating_at_match": 4.4, "membership_history_warning_flag": False},
        {"match_team_id": "mt-1b", "match_id": "match-1", "player_id": "player-4", "player_position": "RIGHT", "player_rating_at_match": 3.9, "membership_history_warning_flag": True},
        {"match_team_id": "mt-1b", "match_id": "match-1", "player_id": "player-3", "player_position": "LEFT", "player_rating_at_match": 4.1, "membership_history_warning_flag": False},
        {"match_team_id": "mt-invalid-a", "match_id": "match-invalid-winner", "player_id": "player-5", "player_position": "LEFT", "player_rating_at_match": 3.5, "membership_history_warning_flag": False},
        {"match_team_id": "mt-invalid-a", "match_id": "match-invalid-winner", "player_id": "player-6", "player_position": "RIGHT", "player_rating_at_match": 3.4, "membership_history_warning_flag": False},
        {"match_team_id": "mt-invalid-b", "match_id": "match-invalid-winner", "player_id": "player-7", "player_position": "LEFT", "player_rating_at_match": 3.6, "membership_history_warning_flag": False},
        {"match_team_id": "mt-invalid-b", "match_id": "match-invalid-winner", "player_id": "player-8", "player_position": "RIGHT", "player_rating_at_match": 3.5, "membership_history_warning_flag": False},
        {"match_team_id": "mt-future-a", "match_id": "match-future", "player_id": "player-9", "player_position": "LEFT", "player_rating_at_match": 3.7, "membership_history_warning_flag": False},
        {"match_team_id": "mt-future-a", "match_id": "match-future", "player_id": "player-10", "player_position": "RIGHT", "player_rating_at_match": 3.7, "membership_history_warning_flag": False},
        {"match_team_id": "mt-future-b", "match_id": "match-future", "player_id": "player-11", "player_position": "LEFT", "player_rating_at_match": 3.8, "membership_history_warning_flag": False},
        {"match_team_id": "mt-future-b", "match_id": "match-future", "player_id": "player-12", "player_position": "RIGHT", "player_rating_at_match": 3.8, "membership_history_warning_flag": False},
        {"match_team_id": "mt-shared-a", "match_id": "match-shared-player", "player_id": "player-13", "player_position": "LEFT", "player_rating_at_match": 4.0, "membership_history_warning_flag": False},
        {"match_team_id": "mt-shared-a", "match_id": "match-shared-player", "player_id": "player-14", "player_position": "RIGHT", "player_rating_at_match": 4.2, "membership_history_warning_flag": False},
        {"match_team_id": "mt-shared-b", "match_id": "match-shared-player", "player_id": "player-13", "player_position": "LEFT", "player_rating_at_match": 4.0, "membership_history_warning_flag": False},
        {"match_team_id": "mt-shared-b", "match_id": "match-shared-player", "player_id": "player-15", "player_position": "RIGHT", "player_rating_at_match": 4.1, "membership_history_warning_flag": False},
    ]


def _match_games_rows():
    return [
        {"match_id": "match-1", "team_one_score": 11, "team_two_score": 8, "winning_team_number": 1, "close_game_flag": False},
        {"match_id": "match-1", "team_one_score": 9, "team_two_score": 11, "winning_team_number": 2, "close_game_flag": True},
        {"match_id": "match-1", "team_one_score": 11, "team_two_score": 9, "winning_team_number": 1, "close_game_flag": True},
        {"match_id": "match-invalid-winner", "team_one_score": 11, "team_two_score": 5, "winning_team_number": 1, "close_game_flag": False},
        {"match_id": "match-invalid-winner", "team_one_score": 11, "team_two_score": 4, "winning_team_number": 1, "close_game_flag": False},
        {"match_id": "match-future", "team_one_score": 11, "team_two_score": 7, "winning_team_number": 1, "close_game_flag": False},
        {"match_id": "match-shared-player", "team_one_score": 11, "team_two_score": 3, "winning_team_number": 1, "close_game_flag": False},
        {"match_id": "match-shared-player", "team_one_score": 11, "team_two_score": 7, "winning_team_number": 1, "close_game_flag": False},
    ]


def _regions_rows():
    return [{"region_id": "region-1", "country_code": "USA"}]


def _monthly_batches_rows():
    return [{"batch_id": "batch-1", "batch_sequence": 6, "batch_date": "2026-06-30"}]


def test_build_competition_match_sides_returns_two_valid_side_rows() -> None:
    result = build_competition_match_sides(
        _matches_rows(),
        _match_teams_rows(),
        _match_team_players_rows(),
        _match_games_rows(),
        _regions_rows(),
        _monthly_batches_rows(),
        analysis_as_of_date=date(2026, 6, 30),
    )

    assert result.included_match_count == 1
    assert result.excluded_match_count == 3
    assert len(result.rows) == 2

    side_one = result.rows[0]
    side_two = result.rows[1]

    assert side_one["match_id"] == "match-1"
    assert side_one["team_number"] == 1
    assert side_one["opponent_team_number"] == 2
    assert side_one["side_score"] == 2
    assert side_one["opponent_score"] == 1
    assert side_one["games_won"] == 2
    assert side_one["games_lost"] == 1
    assert side_one["points_for"] == 31
    assert side_one["points_against"] == 28
    assert round(side_one["point_share"], 6) == round(31 / 59, 6)
    assert side_one["close_game_count"] == 2
    assert side_one["deciding_game_flag"] is True
    assert side_one["canonical_player_pair_key"] == "player-1:player-2"
    assert side_one["won_flag"] is True
    assert side_two["won_flag"] is False
    assert side_two["membership_history_warning_flag"] is True


def test_build_competition_player_matches_emits_partner_and_rating_context() -> None:
    match_sides = build_competition_match_sides(
        _matches_rows(),
        _match_teams_rows(),
        _match_team_players_rows(),
        _match_games_rows(),
        _regions_rows(),
        _monthly_batches_rows(),
        analysis_as_of_date=date(2026, 6, 30),
    )

    result = build_competition_player_matches(match_sides.rows, _match_team_players_rows())

    assert len(result.rows) == 4
    first_row = result.rows[0]
    assert first_row["match_id"] == "match-1"
    assert first_row["team_number"] == 1
    assert first_row["player_id"] == "player-1"
    assert first_row["partner_player_id"] == "player-2"
    assert first_row["player_position"] == "LEFT"
    assert first_row["pre_match_player_rating"] == 4.4
    assert first_row["pre_match_partner_rating"] == 4.1
    assert first_row["pre_match_team_rating"] == 4.25
    assert first_row["pre_match_opponent_team_rating"] == 4.0
    assert first_row["canonical_player_pair_key"] == "player-1:player-2"


def test_build_competition_match_sides_sql_references_required_sources_and_as_of_date() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_competition_match_sides_sql(
        environment,
        analysis_as_of_date=date(2026, 6, 30),
    )

    assert f"{environment.catalog}.{environment.silver_schema}.matches" in sql
    assert f"{environment.catalog}.{environment.silver_schema}.match_teams" in sql
    assert f"{environment.catalog}.{environment.silver_schema}.match_team_players" in sql
    assert f"{environment.catalog}.{environment.silver_schema}.match_games" in sql
    assert f"{environment.catalog}.{environment.silver_schema}.regions" in sql
    assert f"{environment.catalog}.{environment.silver_schema}.monthly_batches" in sql
    assert "DATE('2026-06-30')" in sql
    assert "canonical_player_pair_key" in sql
    assert "opponent_pre_match_team_rating" in sql


def test_build_competition_player_matches_sql_reads_phase3_target() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_competition_player_matches_sql(environment)

    assert f"{environment.catalog}.{environment.gold_schema}.competition_match_sides" in sql
    assert f"{environment.catalog}.{environment.silver_schema}.match_team_players" in sql
    assert "partner_player_id" in sql
    assert "pre_match_opponent_team_rating" in sql


class _FakeTable:
    def __init__(self, row_count: int):
        self._row_count = row_count

    def count(self) -> int:
        return self._row_count


class _FakeSpark:
    def __init__(self, counts_by_table):
        self.counts_by_table = counts_by_table

    def table(self, table_name: str):
        return _FakeTable(self.counts_by_table[table_name])


def test_publish_competition_match_sides_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.competition_match_sides"
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.competition_match_sides"
    spark = _FakeSpark(
        {
            f"{environment.catalog}.{environment.silver_schema}.match_teams": 8,
            target_fqn: 2,
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
        return 2, 2

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.competition.publish_stage_to_gold_table",
        _fake_publish_stage_to_gold_table,
    )

    summary = publish_competition_match_sides(
        spark,
        environment,
        analysis_as_of_date=date(2026, 6, 30),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 8
    assert summary.output_row_count == 2
    assert "DATE('2026-06-30')" in published["stage_sql"]


def test_publish_competition_player_matches_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.competition_player_matches"
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.competition_player_matches"
    spark = _FakeSpark(
        {
            f"{environment.catalog}.{environment.gold_schema}.competition_match_sides": 2,
            target_fqn: 4,
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
        return 4, 4

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.competition.publish_stage_to_gold_table",
        _fake_publish_stage_to_gold_table,
    )

    summary = publish_competition_player_matches(spark, environment)

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 2
    assert summary.output_row_count == 4
    assert "partner_player_id" in published["stage_sql"]
