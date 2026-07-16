"""Tests for Bronze-to-Silver competition table builders."""

from napa_pipeline.bronze_to_silver.athlete import build_players
from napa_pipeline.bronze_to_silver.competition import (
    build_match_games,
    build_match_team_players,
    build_match_teams,
    build_matches,
)
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
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


def _context():
    config = load_bronze_to_silver_config("napa_5k")
    environment = resolve_release_environment(config)
    return config, create_pipeline_context(config, environment, pipeline_run_id="run-123")


def _parents():
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
            {"id": "player-1", "home_region_id": "region-1"},
            {"id": "player-2", "home_region_id": "region-1"},
            {"id": "player-3", "home_region_id": "region-1"},
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
            {
                "id": "tm-1",
                "player_id": "player-1",
                "team_id": "team-1",
                "membership_start_date": "2026-01-01",
            },
            {
                "id": "tm-2",
                "player_id": "player-2",
                "team_id": "team-1",
                "membership_start_date": "2026-01-01",
            },
            {
                "id": "tm-3",
                "player_id": "player-3",
                "team_id": "team-2",
                "membership_start_date": "2026-01-01",
            },
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
    return (
        config,
        context,
        monthly_batches.accepted_rows,
        regions.accepted_rows,
        players.accepted_rows,
        teams.accepted_rows,
        team_memberships.accepted_rows,
        matches.accepted_rows,
        match_teams.accepted_rows,
    )


def test_build_matches_accepts_valid_row_and_derives_calendar_fields() -> None:
    config, context, monthly_batches_rows, regions_rows, *_ = _parents()

    result = build_matches(
        [
            {
                "id": "match-2",
                "batch_id": "batch-2026-06",
                "region_id": "region-1",
                "match_date": "2026-06-20",
                "match_type": "league",
                "competition_category": "mixed",
                "status": "completed",
                "winning_team_number": "2",
            }
        ],
        config,
        context,
        monthly_batches_rows=monthly_batches_rows,
        regions_rows=regions_rows,
    )

    assert result.reconciliation.status == "PASSED"
    row = result.accepted_rows[0]
    assert row["match_id"] == "match-2"
    assert row["batch_sk"] == monthly_batches_rows[0]["batch_sk"]
    assert row["region_sk"] == regions_rows[0]["region_sk"]
    assert row["completed_flag"] is True
    assert row["match_year"] == 2026
    assert row["match_month"] == 6


def test_build_matches_rejects_missing_winner_for_completed_status() -> None:
    config, context, monthly_batches_rows, regions_rows, *_ = _parents()

    result = build_matches(
        [
            {
                "id": "match-2",
                "batch_id": "batch-2026-06",
                "region_id": "region-1",
                "status": "completed",
            }
        ],
        config,
        context,
        monthly_batches_rows=monthly_batches_rows,
        regions_rows=regions_rows,
    )

    assert len(result.accepted_rows) == 0
    assert result.rejected_rows[0]["reject_reason"] == "VALUE_OUT_OF_RANGE"


def test_build_match_teams_accepts_valid_rows_and_derives_winner_flag() -> None:
    config, context, _, _, _, teams_rows, _, matches_rows, _ = _parents()

    result = build_match_teams(
        [
            {
                "id": "mt-3",
                "match_id": "match-1",
                "team_id": "team-1",
                "team_number": "1",
                "pre_match_team_rating": "4.2",
                "post_match_team_rating": "4.3",
            },
            {
                "id": "mt-4",
                "match_id": "match-1",
                "team_id": "team-2",
                "team_number": "2",
                "pre_match_team_rating": "4.1",
                "post_match_team_rating": "4.0",
            },
        ],
        config,
        context,
        matches_rows=matches_rows,
        teams_rows=teams_rows,
    )

    assert result.reconciliation.status == "PASSED"
    assert len(result.accepted_rows) == 2
    winners = [row for row in result.accepted_rows if row["winner_flag"]]
    assert len(winners) == 1
    assert winners[0]["team_number"] == 1
    assert winners[0]["rating_change"] == 0.09999999999999964


def test_build_match_teams_flags_side_cardinality_warning() -> None:
    config, context, _, _, _, teams_rows, _, matches_rows, _ = _parents()

    result = build_match_teams(
        [{"id": "mt-3", "match_id": "match-1", "team_id": "team-1", "team_number": "1"}],
        config,
        context,
        matches_rows=matches_rows,
        teams_rows=teams_rows,
    )

    assert len(result.accepted_rows) == 1
    assert result.warning_count == 1
    assert result.accepted_rows[0]["side_cardinality_warning_flag"] is True


def test_build_match_team_players_rejects_player_on_both_sides() -> None:
    (
        config,
        context,
        _monthly_batches_rows,
        _regions_rows,
        players_rows,
        _teams_rows,
        team_memberships_rows,
        _matches_rows,
        match_teams_rows,
    ) = _parents()

    result = build_match_team_players(
        [
            {"id": "mtp-1", "match_team_id": "mt-1", "player_id": "player-1"},
            {"id": "mtp-2", "match_team_id": "mt-2", "player_id": "player-1"},
        ],
        config,
        context,
        match_teams_rows=match_teams_rows,
        players_rows=players_rows,
        team_memberships_rows=team_memberships_rows,
    )

    assert len(result.accepted_rows) == 1
    assert len(result.rejected_rows) == 1
    assert result.rejected_rows[0]["reject_reason"] == "PLAYER_ON_BOTH_MATCH_SIDES"


def test_build_match_team_players_sets_membership_warning_when_history_misses_match_date() -> None:
    (
        config,
        context,
        _monthly_batches_rows,
        _regions_rows,
        players_rows,
        _teams_rows,
        team_memberships_rows,
        _matches_rows,
        match_teams_rows,
    ) = _parents()

    stale_memberships = [
        {
            **team_memberships_rows[0],
            "membership_start_date": team_memberships_rows[0]["membership_start_date"],
            "membership_end_date": team_memberships_rows[0]["membership_start_date"],
        },
        team_memberships_rows[1],
    ]

    result = build_match_team_players(
        [
            {"id": "mtp-1", "match_team_id": "mt-1", "player_id": "player-1"},
            {"id": "mtp-2", "match_team_id": "mt-1", "player_id": "player-2"},
        ],
        config,
        context,
        match_teams_rows=match_teams_rows,
        players_rows=players_rows,
        team_memberships_rows=stale_memberships,
    )

    assert len(result.accepted_rows) == 2
    assert result.warning_count == 1
    flagged_rows = [row for row in result.accepted_rows if row["membership_history_warning_flag"]]
    assert len(flagged_rows) == 1
    assert flagged_rows[0]["player_id"] == "player-1"


def test_build_match_games_accepts_valid_row_and_derives_scoring_fields() -> None:
    config, context, *_parents_data, matches_rows, _match_teams_rows = _parents()

    result = build_match_games(
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
        matches_rows=matches_rows,
    )

    assert result.reconciliation.status == "PASSED"
    row = result.accepted_rows[0]
    assert row["score_margin"] == 2
    assert row["total_points"] == 20
    assert row["close_game_flag"] is True
    assert row["extended_game_flag"] is False


def test_build_match_games_rejects_winner_score_mismatch() -> None:
    config, context, *_parents_data, matches_rows, _match_teams_rows = _parents()

    result = build_match_games(
        [
            {
                "id": "mg-1",
                "match_id": "match-1",
                "game_number": "1",
                "team_one_score": "7",
                "team_two_score": "11",
                "winning_team_number": "1",
            }
        ],
        config,
        context,
        matches_rows=matches_rows,
    )

    assert len(result.accepted_rows) == 0
    assert result.rejected_rows[0]["reject_reason"] == "GAME_SCORE_WINNER_MISMATCH"
