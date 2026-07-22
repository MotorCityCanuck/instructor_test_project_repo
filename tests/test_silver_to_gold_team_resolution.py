"""Tests for Silver-to-Gold persistent-team resolution."""

from datetime import date

from napa_pipeline.silver_to_gold.team_resolution import (
    ACTIVE_MEMBERSHIP_PAIR,
    AMBIGUOUS,
    DIRECT_VALID_TEAM_ID,
    RESOLVED,
    UNIQUE_HISTORICAL_PAIR,
    UNRESOLVED,
    build_resolved_match_teams,
)


def _teams_rows():
    return [
        {
            "team_id": "team-direct",
            "team_name": "Direct Team",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2026-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-active-pair",
            "team_name": "Active Pair Team",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2026-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-historical",
            "team_name": "Historical Team",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2024-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-ambiguous-a",
            "team_name": "Ambiguous A",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2025-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-ambiguous-b",
            "team_name": "Ambiguous B",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2025-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-dissolved",
            "team_name": "Dissolved Team",
            "team_status": "DISSOLVED",
            "active_flag": False,
            "formation_date": "2024-01-01",
            "dissolution_date": "2026-05-01",
        },
    ]


def test_build_resolved_match_teams_direct_resolution_uses_valid_team_id() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-1",
                "match_id": "match-1",
                "team_id": "team-direct",
                "team_number": 1,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-1", "player_id": "player-2"},
            {"match_team_id": "mt-1", "player_id": "player-1"},
        ],
        team_memberships_rows=[],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["canonical_player_pair_key"] == "player-1:player-2"
    assert row["resolved_team_id"] == "team-direct"
    assert row["team_resolution_method"] == DIRECT_VALID_TEAM_ID
    assert row["team_resolution_status"] == RESOLVED
    assert row["candidate_attribution_allowed_flag"] is True
    assert result.direct_resolution_count == 1


def test_build_resolved_match_teams_uses_active_membership_pair_when_direct_team_missing() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-2",
                "match_id": "match-2",
                "team_id": None,
                "team_number": 2,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-2", "player_id": "player-3"},
            {"match_team_id": "mt-2", "player_id": "player-4"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-active-pair",
                "player_id": "player-3",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-active-pair",
                "player_id": "player-4",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["resolved_team_id"] == "team-active-pair"
    assert row["team_resolution_method"] == ACTIVE_MEMBERSHIP_PAIR
    assert row["team_resolution_status"] == RESOLVED
    assert row["team_resolution_confidence"] == 0.9
    assert result.active_pair_resolution_count == 1


def test_build_resolved_match_teams_uses_unique_historical_pair_when_no_active_pair_exists() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-3",
                "match_id": "match-3",
                "team_id": None,
                "team_number": 1,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-3", "player_id": "player-5"},
            {"match_team_id": "mt-3", "player_id": "player-6"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-historical",
                "player_id": "player-5",
                "membership_start_date": "2025-01-01",
                "membership_end_date": "2025-05-31",
            },
            {
                "team_id": "team-historical",
                "player_id": "player-6",
                "membership_start_date": "2025-01-15",
                "membership_end_date": "2025-06-15",
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["resolved_team_id"] == "team-historical"
    assert row["team_resolution_method"] == UNIQUE_HISTORICAL_PAIR
    assert row["team_resolution_status"] == RESOLVED
    assert row["team_resolution_confidence"] == 0.6
    assert result.historical_pair_resolution_count == 1


def test_build_resolved_match_teams_marks_active_pair_overlaps_as_ambiguous() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-4",
                "match_id": "match-4",
                "team_id": None,
                "team_number": 2,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-4", "player_id": "player-7"},
            {"match_team_id": "mt-4", "player_id": "player-8"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-ambiguous-a",
                "player_id": "player-7",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-ambiguous-a",
                "player_id": "player-8",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-ambiguous-b",
                "player_id": "player-7",
                "membership_start_date": "2026-02-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-ambiguous-b",
                "player_id": "player-8",
                "membership_start_date": "2026-02-01",
                "membership_end_date": None,
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["resolved_team_id"] is None
    assert row["team_resolution_method"] == AMBIGUOUS
    assert row["team_resolution_status"] == AMBIGUOUS
    assert row["candidate_attribution_allowed_flag"] is False
    assert result.ambiguous_count == 1


def test_build_resolved_match_teams_marks_historical_pair_multiple_teams_as_ambiguous() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-5",
                "match_id": "match-5",
                "team_id": None,
                "team_number": 1,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-5", "player_id": "player-9"},
            {"match_team_id": "mt-5", "player_id": "player-10"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-ambiguous-a",
                "player_id": "player-9",
                "membership_start_date": "2025-01-01",
                "membership_end_date": "2025-03-31",
            },
            {
                "team_id": "team-ambiguous-a",
                "player_id": "player-10",
                "membership_start_date": "2025-01-01",
                "membership_end_date": "2025-03-31",
            },
            {
                "team_id": "team-ambiguous-b",
                "player_id": "player-9",
                "membership_start_date": "2025-04-01",
                "membership_end_date": "2025-06-30",
            },
            {
                "team_id": "team-ambiguous-b",
                "player_id": "player-10",
                "membership_start_date": "2025-04-01",
                "membership_end_date": "2025-06-30",
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["team_resolution_status"] == AMBIGUOUS
    assert row["resolved_team_id"] is None


def test_build_resolved_match_teams_marks_missing_team_history_as_unresolved() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-6",
                "match_id": "match-6",
                "team_id": None,
                "team_number": 1,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-6", "player_id": "player-11"},
            {"match_team_id": "mt-6", "player_id": "player-12"},
        ],
        team_memberships_rows=[],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["team_resolution_method"] == UNRESOLVED
    assert row["team_resolution_status"] == UNRESOLVED
    assert result.unresolved_count == 1


def test_build_resolved_match_teams_resolves_dissolved_team_but_blocks_candidate_attribution() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-7",
                "match_id": "match-7",
                "team_id": None,
                "team_number": 1,
                "match_date": "2026-04-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-7", "player_id": "player-13"},
            {"match_team_id": "mt-7", "player_id": "player-14"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-dissolved",
                "player_id": "player-13",
                "membership_start_date": "2026-01-01",
                "membership_end_date": "2026-05-01",
            },
            {
                "team_id": "team-dissolved",
                "player_id": "player-14",
                "membership_start_date": "2026-01-01",
                "membership_end_date": "2026-05-01",
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["resolved_team_id"] == "team-dissolved"
    assert row["team_resolution_method"] == ACTIVE_MEMBERSHIP_PAIR
    assert row["candidate_attribution_allowed_flag"] is False


def test_build_resolved_match_teams_treats_membership_date_boundaries_as_inclusive() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-8",
                "match_id": "match-8",
                "team_id": None,
                "team_number": 2,
                "match_date": date(2026, 6, 15),
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-8", "player_id": "player-15"},
            {"match_team_id": "mt-8", "player_id": "player-16"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-active-pair",
                "player_id": "player-15",
                "membership_start_date": "2026-06-15",
                "membership_end_date": "2026-07-01",
            },
            {
                "team_id": "team-active-pair",
                "player_id": "player-16",
                "membership_start_date": "2026-01-01",
                "membership_end_date": "2026-06-15",
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["team_resolution_method"] == ACTIVE_MEMBERSHIP_PAIR
    assert row["resolved_team_id"] == "team-active-pair"


def test_build_resolved_match_teams_normalizes_reversed_player_order_and_duplicate_memberships() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-9",
                "match_id": "match-9",
                "team_id": None,
                "team_number": 1,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-9", "player_id": "player-18"},
            {"match_team_id": "mt-9", "player_id": "player-17"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-active-pair",
                "player_id": "player-17",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-active-pair",
                "player_id": "player-17",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-active-pair",
                "player_id": "player-18",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["canonical_player_pair_key"] == "player-17:player-18"
    assert row["team_resolution_method"] == ACTIVE_MEMBERSHIP_PAIR
    assert row["resolved_team_id"] == "team-active-pair"
