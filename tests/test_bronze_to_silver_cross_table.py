"""Tests for Bronze-to-Silver cross-table validation and convenience views."""

from napa_pipeline.bronze_to_silver.athlete import build_players
from napa_pipeline.bronze_to_silver.competition import (
    build_match_games,
    build_match_team_players,
    build_match_teams,
    build_matches,
)
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.cross_table import run_cross_table_validations
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context
from napa_pipeline.bronze_to_silver.organization import (
    build_team_memberships,
    build_teams,
)
from napa_pipeline.bronze_to_silver.reference import (
    build_monthly_batches,
    build_regions,
)
from napa_pipeline.bronze_to_silver.views import (
    build_vw_current_team_memberships,
    build_vw_match_results,
    build_vw_player_match_history,
    build_vw_players_current,
    build_vw_team_rosters,
)


def _context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    return config, create_pipeline_context(config, environment, pipeline_run_id="run-123")


def _pipeline_rows():
    config, context = _context()
    monthly_batches = build_monthly_batches(
        [{"id": "batch-2026-06", "batch_date": "2026-06-30", "batch_sequence": "6"}],
        config,
        context,
    )
    regions = build_regions(
        [{"id": "region-1", "name": "Ontario West", "country": "Canada"}],
        config,
        context,
    )
    players = build_players(
        [
            {"id": "player-1", "home_region_id": "region-1", "status": "active", "first_name": "Taylor", "last_name": "Ng"},
            {"id": "player-2", "home_region_id": "region-1", "status": "active", "first_name": "Alex", "last_name": "Li"},
            {"id": "player-3", "home_region_id": "region-1", "status": "inactive", "first_name": "Jordan", "last_name": "Kim"},
        ],
        config,
        context,
        regions_rows=regions.accepted_rows,
        monthly_batches_rows=monthly_batches.accepted_rows,
    )
    teams = build_teams(
        [
            {"id": "team-1", "name": "Northern Duo", "team_type": "mixed", "status": "active"},
            {"id": "team-2", "name": "Southern Duo", "team_type": "mixed", "status": "active"},
        ],
        config,
        context,
        monthly_batches_rows=monthly_batches.accepted_rows,
    )
    team_memberships = build_team_memberships(
        [
            {"id": "tm-1", "player_id": "player-1", "team_id": "team-1", "membership_start_date": "2026-01-01", "player_position": "left"},
            {"id": "tm-2", "player_id": "player-2", "team_id": "team-1", "membership_start_date": "2026-01-01", "player_position": "right"},
            {"id": "tm-3", "player_id": "player-3", "team_id": "team-2", "membership_start_date": "2026-01-01", "player_position": "left"},
        ],
        config,
        context,
        players_rows=players.accepted_rows,
        teams_rows=teams.accepted_rows,
        monthly_batches_rows=monthly_batches.accepted_rows,
    )
    matches = build_matches(
        [
            {
                "id": "match-1",
                "batch_id": "batch-2026-06",
                "region_id": "region-1",
                "match_date": "2026-06-15",
                "competition_category": "mixed",
                "status": "completed",
                "winning_team_number": "1",
            }
        ],
        config,
        context,
        monthly_batches_rows=monthly_batches.accepted_rows,
        regions_rows=regions.accepted_rows,
    )
    match_teams = build_match_teams(
        [
            {"id": "mt-1", "match_id": "match-1", "team_id": "team-1", "team_number": "1"},
            {"id": "mt-2", "match_id": "match-1", "team_id": "team-2", "team_number": "2"},
        ],
        config,
        context,
        matches_rows=matches.accepted_rows,
        teams_rows=teams.accepted_rows,
    )
    match_team_players = build_match_team_players(
        [
            {"id": "mtp-1", "match_team_id": "mt-1", "player_id": "player-1", "player_rating_at_match": "4.2"},
            {"id": "mtp-2", "match_team_id": "mt-1", "player_id": "player-2", "player_rating_at_match": "4.1"},
            {"id": "mtp-3", "match_team_id": "mt-2", "player_id": "player-3", "player_rating_at_match": "3.9"},
        ],
        config,
        context,
        match_teams_rows=match_teams.accepted_rows,
        players_rows=players.accepted_rows,
        team_memberships_rows=team_memberships.accepted_rows,
    )
    match_games = build_match_games(
        [
            {
                "id": "mg-1",
                "match_id": "match-1",
                "game_number": "1",
                "team_one_score": "11",
                "team_two_score": "9",
                "winning_team_number": "1",
                "target_score": "11",
                "actual_team_one_score_share": "0.55",
            }
        ],
        config,
        context,
        matches_rows=matches.accepted_rows,
    )
    return {
        "config": config,
        "context": context,
        "monthly_batches": monthly_batches.accepted_rows,
        "regions": regions.accepted_rows,
        "players": players.accepted_rows,
        "teams": teams.accepted_rows,
        "team_memberships": team_memberships.accepted_rows,
        "matches": matches.accepted_rows,
        "match_teams": match_teams.accepted_rows,
        "match_team_players": match_team_players.accepted_rows,
        "match_games": match_games.accepted_rows,
    }


def test_run_cross_table_validations_reports_expected_warnings() -> None:
    rows = _pipeline_rows()
    result = run_cross_table_validations(
        rows["context"],
        players_rows=rows["players"],
        teams_rows=rows["teams"],
        team_memberships_rows=rows["team_memberships"],
        matches_rows=rows["matches"],
        match_teams_rows=rows["match_teams"],
        match_team_players_rows=rows["match_team_players"],
        match_games_rows=rows["match_games"],
        expected_match_team_count=int(rows["config"].data["thresholds"]["expected_match_team_count"]),
        expected_match_team_player_count=int(rows["config"].data["thresholds"]["expected_match_team_player_count"]),
    )

    quality_by_rule = {row["rule_id"]: row for row in result.quality_results}
    assert quality_by_rule["CROSS_TEAM_001"]["failed_row_count"] == 1
    assert quality_by_rule["CROSS_MATCH_TEAM_001"]["failed_row_count"] == 1
    assert quality_by_rule["CROSS_WINNER_001"]["failed_row_count"] == 0
    assert result.warning_count == 2
    assert result.failure_count == 0


def test_run_cross_table_validations_detects_winner_inconsistency() -> None:
    rows = _pipeline_rows()
    inconsistent_match_games = [
        {**rows["match_games"][0], "winning_team_number": 2},
    ]

    result = run_cross_table_validations(
        rows["context"],
        players_rows=rows["players"],
        teams_rows=rows["teams"],
        team_memberships_rows=rows["team_memberships"],
        matches_rows=rows["matches"],
        match_teams_rows=rows["match_teams"],
        match_team_players_rows=rows["match_team_players"],
        match_games_rows=inconsistent_match_games,
        expected_match_team_count=int(rows["config"].data["thresholds"]["expected_match_team_count"]),
        expected_match_team_player_count=int(rows["config"].data["thresholds"]["expected_match_team_player_count"]),
    )

    quality_by_rule = {row["rule_id"]: row for row in result.quality_results}
    assert quality_by_rule["CROSS_WINNER_001"]["failed_row_count"] == 1
    assert result.failure_count == 1


def test_build_vw_players_current_filters_active_players() -> None:
    rows = _pipeline_rows()
    view_rows = build_vw_players_current(rows["players"])

    assert len(view_rows) == 2
    assert {row["player_id"] for row in view_rows} == {"player-1", "player-2"}


def test_build_vw_current_team_memberships_filters_current_rows() -> None:
    rows = _pipeline_rows()
    view_rows = build_vw_current_team_memberships(rows["team_memberships"])

    assert len(view_rows) == 3
    assert all(row["current_membership_flag"] is True for row in view_rows)


def test_build_vw_team_rosters_exposes_roster_status() -> None:
    rows = _pipeline_rows()
    view_rows = build_vw_team_rosters(
        rows["teams"],
        rows["players"],
        rows["team_memberships"],
        expected_roster_count=int(rows["config"].data["thresholds"]["expected_match_team_player_count"]),
    )

    roster_by_team = {row["team_id"]: row for row in view_rows}
    assert roster_by_team["team-1"]["roster_cardinality_status"] == "OK"
    assert roster_by_team["team-1"]["current_roster_count"] == 2
    assert roster_by_team["team-2"]["roster_cardinality_status"] == "WARNING"


def test_build_vw_match_results_aggregates_match_scores() -> None:
    rows = _pipeline_rows()
    view_rows = build_vw_match_results(
        rows["matches"],
        rows["match_teams"],
        rows["match_games"],
        rows["teams"],
        rows["regions"],
        rows["monthly_batches"],
    )

    assert len(view_rows) == 1
    row = view_rows[0]
    assert row["team_one_name"] == "Northern Duo"
    assert row["team_two_name"] == "Southern Duo"
    assert row["team_one_total_score"] == 11
    assert row["team_two_total_score"] == 9
    assert row["game_count"] == 1
    assert row["region_name"] == "Ontario West"


def test_build_vw_player_match_history_exposes_opponent_and_result() -> None:
    rows = _pipeline_rows()
    view_rows = build_vw_player_match_history(
        rows["match_team_players"],
        rows["match_teams"],
        rows["matches"],
        rows["players"],
        rows["teams"],
        rows["regions"],
        rows["monthly_batches"],
    )

    player_one_rows = [row for row in view_rows if row["player_id"] == "player-1"]
    assert len(player_one_rows) == 1
    row = player_one_rows[0]
    assert row["team_name"] == "Northern Duo"
    assert row["opponent_team_name"] == "Southern Duo"
    assert row["result"] == "WIN"
    assert row["region_name"] == "Ontario West"
