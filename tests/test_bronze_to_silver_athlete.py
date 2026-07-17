"""Tests for Bronze-to-Silver athlete table builders."""

from napa_pipeline.bronze_to_silver.athlete import (
    build_player_assessment_history,
    build_player_registrations,
    build_players,
)
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context
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
        [
            {
                "id": "batch-2026-06",
                "batch_date": "2026-06-30",
                "batch_sequence": "6",
            }
        ],
        config,
        context,
    )
    regions = build_regions(
        [
            {
                "id": "region-1",
                "name": "Ontario West",
                "country": "Canada",
            }
        ],
        config,
        context,
    )
    return config, context, monthly_batches.accepted_rows, regions.accepted_rows


def test_build_players_accepts_valid_row_and_derives_age_and_region_key() -> None:
    config, context, monthly_batches_rows, regions_rows = _parents()

    result = build_players(
        [
            {
                "id": "player-1",
                "first_name": " Taylor ",
                "last_name": "Ng",
                "birth_date": "2000-07-01",
                "gender": "female",
                "dominant_hand": "R",
                "preferred_side": "left",
                "home_region_id": "region-1",
                "country": "Canada",
                "status": "active",
            }
        ],
        config,
        context,
        regions_rows=regions_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    assert result.reconciliation.status == "PASSED"
    assert len(result.accepted_rows) == 1
    row = result.accepted_rows[0]
    assert row["player_id"] == "player-1"
    assert row["display_name"] == "Taylor Ng"
    assert row["gender"] == "F"
    assert row["dominant_hand"] == "RIGHT"
    assert row["preferred_side"] == "LEFT"
    assert row["country_code"] == "CAN"
    assert row["home_region_id"] == "region-1"
    assert row["home_region_sk"] == regions_rows[0]["region_sk"]
    assert row["age"] == 25
    assert row["age_group"] == "AGE_18_34"
    assert row["active_flag"] is True
    assert row["_source_table"] == "player_master"


def test_build_players_rejects_orphan_region() -> None:
    config, context, monthly_batches_rows, _ = _parents()

    result = build_players(
        [
            {
                "id": "player-1",
                "home_region_id": "missing-region",
            }
        ],
        config,
        context,
        regions_rows=(),
        monthly_batches_rows=monthly_batches_rows,
    )

    assert len(result.accepted_rows) == 0
    assert len(result.rejected_rows) == 1
    assert result.rejected_rows[0]["reject_reason"] == "ORPHAN_FOREIGN_KEY"


def test_build_player_registrations_derives_current_flag_and_sequence() -> None:
    config, context, monthly_batches_rows, regions_rows = _parents()
    players = build_players(
        [
            {
                "id": "player-1",
                "first_name": "Taylor",
                "last_name": "Ng",
                "home_region_id": "region-1",
            }
        ],
        config,
        context,
        regions_rows=regions_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    result = build_player_registrations(
        [
            {
                "id": "reg-1",
                "player_id": "player-1",
                "batch_id": "batch-2026-06",
                "registration_date": "2026-01-15",
                "effective_start_date": "2026-01-01",
                "effective_end_date": "2026-03-31",
                "status": "expired",
            },
            {
                "id": "reg-2",
                "player_id": "player-1",
                "batch_id": "batch-2026-06",
                "registration_date": "2026-04-15",
                "effective_start_date": "2026-04-01",
                "registration_status": "active",
            },
        ],
        config,
        context,
        players_rows=players.accepted_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    assert result.reconciliation.status == "PASSED"
    assert len(result.accepted_rows) == 2
    ordered = sorted(result.accepted_rows, key=lambda row: row["registration_sequence"])
    assert ordered[0]["registration_id"] == "reg-1"
    assert ordered[0]["current_registration_flag"] is False
    assert ordered[0]["registration_sequence"] == 1
    assert ordered[1]["registration_id"] == "reg-2"
    assert ordered[1]["current_registration_flag"] is True
    assert ordered[1]["registration_sequence"] == 2
    assert ordered[1]["player_sk"] == players.accepted_rows[0]["player_sk"]
    assert ordered[1]["batch_sk"] == monthly_batches_rows[0]["batch_sk"]


def test_build_player_registrations_rejects_invalid_date_range() -> None:
    config, context, monthly_batches_rows, regions_rows = _parents()
    players = build_players(
        [{"id": "player-1", "home_region_id": "region-1"}],
        config,
        context,
        regions_rows=regions_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    result = build_player_registrations(
        [
            {
                "id": "reg-1",
                "player_id": "player-1",
                "effective_start_date": "2026-06-10",
                "effective_end_date": "2026-06-01",
            }
        ],
        config,
        context,
        players_rows=players.accepted_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    assert len(result.accepted_rows) == 0
    assert len(result.rejected_rows) == 1
    assert result.rejected_rows[0]["reject_reason"] == "INVALID_DATE_RANGE"


def test_build_player_assessment_history_accepts_valid_row() -> None:
    config, context, monthly_batches_rows, regions_rows = _parents()
    players = build_players(
        [{"id": "player-1", "home_region_id": "region-1"}],
        config,
        context,
        regions_rows=regions_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    result = build_player_assessment_history(
        [
            {
                "id": "assess-1",
                "player_id": "player-1",
                "batch_id": "batch-2026-06",
                "assessment_date": "2026-06-15",
                "assessment_type": "rating",
                "assessment_value": "4.25",
                "assessment_confidence": "0.9",
                "assessor_source": "coach",
            }
        ],
        config,
        context,
        players_rows=players.accepted_rows,
        monthly_batches_rows=monthly_batches_rows,
    )

    assert result.reconciliation.status == "PASSED"
    assert len(result.accepted_rows) == 1
    row = result.accepted_rows[0]
    assert row["assessment_id"] == "assess-1"
    assert row["player_sk"] == players.accepted_rows[0]["player_sk"]
    assert row["batch_sk"] == monthly_batches_rows[0]["batch_sk"]
    assert row["assessment_type"] == "RATING"
    assert row["assessment_value"] == 4.25
    assert row["assessment_confidence"] == 0.9
    assert row["assessor_source"] == "coach"


def test_build_player_assessment_history_rejects_orphan_player() -> None:
    config, context, monthly_batches_rows, _ = _parents()

    result = build_player_assessment_history(
        [
            {
                "id": "assess-1",
                "player_id": "missing-player",
                "assessment_date": "2026-06-15",
            }
        ],
        config,
        context,
        players_rows=(),
        monthly_batches_rows=monthly_batches_rows,
    )

    assert len(result.accepted_rows) == 0
    assert len(result.rejected_rows) == 1
    assert result.rejected_rows[0]["reject_reason"] == "ORPHAN_FOREIGN_KEY"
