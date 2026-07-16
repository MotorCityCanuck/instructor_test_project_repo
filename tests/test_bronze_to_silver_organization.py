"""Tests for Bronze-to-Silver organization and partnership table builders."""

from napa_pipeline.bronze_to_silver.athlete import build_players
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context
from napa_pipeline.bronze_to_silver.organization import (
    build_club_memberships,
    build_clubs,
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
        [{"id": "player-1", "home_region_id": "region-1"}],
        config,
        context,
        regions_rows=regions.accepted_rows,
        monthly_batches_rows=monthly_batches.accepted_rows,
    )
    clubs = build_clubs(
        [{"id": "club-1", "name": "North Club", "region_id": "region-1"}],
        config,
        context,
        regions_rows=regions.accepted_rows,
    )
    teams = build_teams(
        [{"id": "team-1", "name": "Northern Duo", "team_type": "mixed", "status": "active"}],
        config,
        context,
        monthly_batches_rows=monthly_batches.accepted_rows,
    )
    return (
        config,
        context,
        monthly_batches.accepted_rows,
        regions.accepted_rows,
        players.accepted_rows,
        clubs.accepted_rows,
        teams.accepted_rows,
    )


def test_build_clubs_accepts_valid_row_and_derives_region_key() -> None:
    config, context, _, regions_rows, _, _, _ = _parents()

    result = build_clubs(
        [
            {
                "id": "club-2",
                "name": "Ontario East Club",
                "region_id": "region-1",
                "country": "Canada",
                "status": "active",
            }
        ],
        config,
        context,
        regions_rows=regions_rows,
    )

    assert result.reconciliation.status == "PASSED"
    assert len(result.accepted_rows) == 1
    row = result.accepted_rows[0]
    assert row["club_id"] == "club-2"
    assert row["club_name"] == "Ontario East Club"
    assert row["region_sk"] == regions_rows[0]["region_sk"]
    assert row["country_code"] == "CAN"


def test_build_clubs_rejects_orphan_region() -> None:
    config, context, _, _, _, _, _ = _parents()

    result = build_clubs(
        [{"id": "club-2", "name": "Broken Club", "region_id": "missing-region"}],
        config,
        context,
        regions_rows=(),
    )

    assert len(result.accepted_rows) == 0
    assert result.rejected_rows[0]["reject_reason"] == "ORPHAN_FOREIGN_KEY"


def test_build_teams_normalizes_type_and_status_and_derives_age() -> None:
    config, context, monthly_batches_rows, _, _, _, _ = _parents()

    result = build_teams(
        [
            {
                "id": "team-2",
                "name": "Maple Pair",
                "team_type": "womens",
                "country": "Canada",
                "status": "active",
                "formation_date": "2026-01-01",
            }
        ],
        config,
        context,
        monthly_batches_rows=monthly_batches_rows,
    )

    assert result.reconciliation.status == "PASSED"
    row = result.accepted_rows[0]
    assert row["team_category"] == "WOMENS"
    assert row["team_status"] == "ACTIVE"
    assert row["country_code"] == "CAN"
    assert row["active_flag"] is True
    assert row["team_age_days"] == 180


def test_build_teams_rejects_invalid_date_range() -> None:
    config, context, monthly_batches_rows, _, _, _, _ = _parents()

    result = build_teams(
        [
            {
                "id": "team-2",
                "team_type": "mixed",
                "formation_date": "2026-06-10",
                "dissolution_date": "2026-06-01",
            }
        ],
        config,
        context,
        monthly_batches_rows=monthly_batches_rows,
    )

    assert len(result.accepted_rows) == 0
    assert result.rejected_rows[0]["reject_reason"] == "INVALID_DATE_RANGE"


def test_build_club_memberships_derives_current_flag_and_overlap_warning() -> None:
    config, context, monthly_batches_rows, _, players_rows, clubs_rows, _ = _parents()

    result = build_club_memberships(
        [
            {
                "id": "club-mem-1",
                "player_id": "player-1",
                "club_id": "club-1",
                "membership_start_date": "2026-01-01",
                "membership_end_date": "2026-04-30",
            },
            {
                "id": "club-mem-2",
                "player_id": "player-1",
                "club_id": "club-1",
                "membership_start_date": "2026-04-15",
            },
        ],
        config,
        context,
        players_rows=players_rows,
        clubs_rows=clubs_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    assert result.reconciliation.status == "PASSED"
    assert len(result.accepted_rows) == 2
    assert result.warning_count == 1
    overlap_rows = [row for row in result.accepted_rows if row["membership_overlap_flag"]]
    assert len(overlap_rows) == 1
    current_rows = [row for row in result.accepted_rows if row["current_membership_flag"]]
    assert len(current_rows) == 1
    assert current_rows[0]["club_membership_id"] == "club-mem-2"


def test_build_club_memberships_rejects_orphan_player() -> None:
    config, context, monthly_batches_rows, _, _, clubs_rows, _ = _parents()

    result = build_club_memberships(
        [{"id": "club-mem-1", "player_id": "missing-player", "club_id": "club-1"}],
        config,
        context,
        players_rows=(),
        clubs_rows=clubs_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    assert len(result.accepted_rows) == 0
    assert result.rejected_rows[0]["reject_reason"] == "ORPHAN_FOREIGN_KEY"


def test_build_team_memberships_accepts_valid_row() -> None:
    config, context, monthly_batches_rows, _, players_rows, _, teams_rows = _parents()

    result = build_team_memberships(
        [
            {
                "id": "team-mem-1",
                "player_id": "player-1",
                "team_id": "team-1",
                "membership_start_date": "2026-01-01",
                "player_role": "captain",
                "player_position": "left",
            }
        ],
        config,
        context,
        players_rows=players_rows,
        teams_rows=teams_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    assert result.reconciliation.status == "PASSED"
    row = result.accepted_rows[0]
    assert row["team_sk"] == teams_rows[0]["team_sk"]
    assert row["player_sk"] == players_rows[0]["player_sk"]
    assert row["player_role"] == "CAPTAIN"
    assert row["player_position"] == "LEFT"
    assert row["current_membership_flag"] is True


def test_build_team_memberships_rejects_invalid_position() -> None:
    config, context, monthly_batches_rows, _, players_rows, _, teams_rows = _parents()

    result = build_team_memberships(
        [
            {
                "id": "team-mem-1",
                "player_id": "player-1",
                "team_id": "team-1",
                "player_position": "center",
            }
        ],
        config,
        context,
        players_rows=players_rows,
        teams_rows=teams_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    assert len(result.accepted_rows) == 0
    assert result.rejected_rows[0]["reject_reason"] == "INVALID_DOMAIN_VALUE"
