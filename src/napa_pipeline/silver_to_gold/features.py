"""Phase 6 player feature builders for the Silver-to-Gold pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import math
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_stage_table_fqn,
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.publish import publish_stage_records_to_gold_table
from napa_pipeline.silver_to_gold.ratings import expected_win_probability


NO_EVIDENCE = "NONE"
LIMITED_EVIDENCE = "LIMITED"
SUFFICIENT_EVIDENCE = "SUFFICIENT"


PLAYER_FEATURE_REGISTRY: tuple[dict[str, Any], ...] = (
    {
        "feature_name": "win_pct",
        "description": "Observed player win percentage within the evidence window.",
        "source": "competition_player_matches",
        "grain": "player_id,evidence_window",
        "window": "all",
        "calculation": "wins / match_count",
        "direction": "higher_is_better",
        "minimum_evidence": 1,
        "null_behavior": "null when match_count = 0",
        "version": "1.0.0",
    },
    {
        "feature_name": "performance_above_expectation",
        "description": "Observed win rate minus average pre-match expected win probability.",
        "source": "competition_player_matches",
        "grain": "player_id,evidence_window",
        "window": "all",
        "calculation": "win_pct - avg_expected_win_probability",
        "direction": "higher_is_better",
        "minimum_evidence": 1,
        "null_behavior": "null when match_count = 0",
        "version": "1.0.0",
    },
    {
        "feature_name": "recency_weighted_win_pct",
        "description": "Recency-weighted player win percentage using the configured half-life.",
        "source": "competition_player_matches",
        "grain": "player_id,evidence_window",
        "window": "all",
        "calculation": "weighted wins / weighted matches",
        "direction": "higher_is_better",
        "minimum_evidence": 1,
        "null_behavior": "null when weighted evidence = 0",
        "version": "1.0.0",
    },
    {
        "feature_name": "consistency_score",
        "description": "Composite stability measure combining win rate and negative-tail dispersion.",
        "source": "competition_player_matches",
        "grain": "player_id,evidence_window",
        "window": "all",
        "calculation": "bounded composite of win_pct, point_share_stddev, and worst quartile point share",
        "direction": "higher_is_better",
        "minimum_evidence": 5,
        "null_behavior": "null when below minimum consistency evidence",
        "version": "1.0.0",
    },
    {
        "feature_name": "rating_change_180",
        "description": "Analytical rating change over the trailing 180 days.",
        "source": "player_rating_history",
        "grain": "player_id",
        "window": "trailing_180",
        "calculation": "current analytical rating minus latest rating at or before analysis_as_of_date - 180 days",
        "direction": "higher_is_better",
        "minimum_evidence": 2,
        "null_behavior": "null when insufficient dated rating observations exist",
        "version": "1.0.0",
    },
    {
        "feature_name": "assessment_slope_per_30_days",
        "description": "Deterministic linear slope of assessment values per 30 days.",
        "source": "player_assessment_history",
        "grain": "player_id",
        "window": "trailing_180",
        "calculation": "linear regression slope * 30",
        "direction": "higher_is_better",
        "minimum_evidence": 2,
        "null_behavior": "null when fewer than two assessment points exist",
        "version": "1.0.0",
    },
    {
        "feature_name": "development_momentum_score",
        "description": "Composite future-potential signal combining rating, assessment, confidence, and activity trends.",
        "source": "player_rating_history,player_assessment_history,player_registrations",
        "grain": "player_id",
        "window": "analysis_as_of_date",
        "calculation": "bounded weighted average of positive trend components",
        "direction": "higher_is_better",
        "minimum_evidence": 1,
        "null_behavior": "falls back to partial evidence with visible evidence_status",
        "version": "1.0.0",
    },
)


@dataclass(frozen=True)
class PlayerPerformanceFeaturesResult:
    """Built player performance feature rows."""

    rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class PlayerDevelopmentFeaturesResult:
    """Built player development feature rows."""

    rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class PlayerPerformanceFeaturesPublicationSummary:
    """Published-table summary for player_performance_features."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class PlayerDevelopmentFeaturesPublicationSummary:
    """Published-table summary for player_development_features."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


def get_player_feature_registry() -> tuple[dict[str, Any], ...]:
    """Return the Phase 6 feature registry."""
    return PLAYER_FEATURE_REGISTRY


def build_player_performance_features(
    competition_player_matches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    player_current_ratings_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
    features_config: dict[str, Any],
    evidence_windows_config: dict[str, Any],
) -> PlayerPerformanceFeaturesResult:
    """Build windowed player performance features as of the analysis date."""
    current_by_player = {
        _normalize_required_string(row.get("player_id")): row
        for row in player_current_ratings_rows
    }
    players_by_id = {
        _normalize_required_string(row.get("player_id")): row
        for row in players_rows
    }
    matches_by_player = _group_rows_by_key(
        [
            row
            for row in competition_player_matches_rows
            if (_parse_date_value(row.get("match_date")) or analysis_as_of_date) <= analysis_as_of_date
        ],
        "player_id",
    )
    recency_half_life_days = float(features_config.get("recency_half_life_days", 60))
    minimum_matches_for_consistency = int(features_config.get("minimum_matches_for_consistency", 5))

    rows: list[dict[str, Any]] = []
    for player_id in sorted(current_by_player):
        player_row = players_by_id.get(player_id, {})
        current_row = current_by_player[player_id]
        player_match_rows = sorted(
            matches_by_player.get(player_id, []),
            key=lambda row: (
                _parse_required_date(row.get("match_date")),
                _coerce_int(row.get("batch_sequence")) or 0,
                _normalize_optional_string(row.get("match_id")) or "",
            ),
        )
        for evidence_window, window_days in _resolve_evidence_windows(evidence_windows_config).items():
            window_rows = _filter_rows_to_window(
                player_match_rows,
                analysis_as_of_date=analysis_as_of_date,
                window_days=window_days,
                date_field="match_date",
            )
            match_count = len(window_rows)
            win_count = sum(1 for row in window_rows if _coerce_bool(row.get("won_flag")))
            loss_count = sum(1 for row in window_rows if _coerce_bool(row.get("lost_flag")))
            total_games_won = sum(_coerce_int(row.get("games_won")) or 0 for row in window_rows)
            total_games_lost = sum(_coerce_int(row.get("games_lost")) or 0 for row in window_rows)
            point_shares = [_coerce_float(row.get("point_share")) for row in window_rows if _coerce_float(row.get("point_share")) is not None]
            point_differentials = [
                _coerce_float(row.get("point_differential"))
                for row in window_rows
                if _coerce_float(row.get("point_differential")) is not None
            ]
            expected_values = [_expected_from_match_row(row) for row in window_rows]
            upset_wins = sum(
                1
                for row, expected_value in zip(window_rows, expected_values)
                if _coerce_bool(row.get("won_flag")) and expected_value is not None and expected_value < 0.5
            )
            favorite_losses = sum(
                1
                for row, expected_value in zip(window_rows, expected_values)
                if _coerce_bool(row.get("lost_flag")) and expected_value is not None and expected_value > 0.5
            )
            partner_counts = _partner_frequency(window_rows)
            primary_partner_match_count = max(partner_counts.values()) if partner_counts else 0
            weighted_match_total, weighted_win_total = _recency_weighted_outcomes(
                window_rows,
                analysis_as_of_date=analysis_as_of_date,
                half_life_days=recency_half_life_days,
            )
            worst_quartile_point_share = _worst_quartile_average(point_shares)
            consistency_score = _consistency_score(
                win_count=win_count,
                match_count=match_count,
                point_shares=point_shares,
                worst_quartile_point_share=worst_quartile_point_share,
                minimum_matches_for_consistency=minimum_matches_for_consistency,
            )
            evidence_status = _performance_evidence_status(
                match_count=match_count,
                minimum_matches_for_consistency=minimum_matches_for_consistency,
            )
            rows.append(
                {
                    "player_id": player_id,
                    "evidence_window": evidence_window,
                    "analysis_as_of_date": analysis_as_of_date,
                    "display_name": _normalize_optional_string(player_row.get("display_name")),
                    "country_code": _normalize_optional_string(player_row.get("country_code")),
                    "active_flag": _coerce_bool(player_row.get("active_flag")),
                    "analytical_rating_value": _coerce_float(current_row.get("analytical_rating_value")),
                    "rated_match_count_current": _coerce_int(current_row.get("rated_match_count")) or 0,
                    "match_count": match_count,
                    "win_count": win_count,
                    "loss_count": loss_count,
                    "win_pct": _safe_divide(win_count, match_count),
                    "game_win_pct": _safe_divide(total_games_won, total_games_won + total_games_lost),
                    "avg_point_share": _mean(point_shares),
                    "avg_point_differential": _mean(point_differentials),
                    "avg_expected_win_probability": _mean([value for value in expected_values if value is not None]),
                    "performance_above_expectation": (
                        None
                        if match_count == 0
                        else _safe_divide(win_count, match_count)
                        - (_mean([value for value in expected_values if value is not None]) or 0.0)
                    ),
                    "avg_opponent_analytical_rating": _mean(
                        [
                            _coerce_float(row.get("pre_match_opponent_team_rating"))
                            for row in window_rows
                            if _coerce_float(row.get("pre_match_opponent_team_rating")) is not None
                        ]
                    ),
                    "strength_of_schedule": _mean(
                        [
                            _coerce_float(row.get("pre_match_opponent_team_rating"))
                            for row in window_rows
                            if _coerce_float(row.get("pre_match_opponent_team_rating")) is not None
                        ]
                    ),
                    "upset_win_pct": _safe_divide(upset_wins, win_count),
                    "favorite_loss_pct": _safe_divide(favorite_losses, loss_count),
                    "recency_weighted_win_pct": _safe_divide(weighted_win_total, weighted_match_total),
                    "distinct_partner_count": len(partner_counts),
                    "primary_partner_match_pct": _safe_divide(primary_partner_match_count, match_count),
                    "performance_with_multiple_partners_flag": len(partner_counts) > 1,
                    "partner_adjusted_performance": (
                        None
                        if len(partner_counts) <= 1 or match_count == 0
                        else (_safe_divide(win_count, match_count) or 0.0)
                        * min(1.0, len(partner_counts) / 3.0)
                    ),
                    "point_share_stddev": _stddev(point_shares),
                    "point_differential_stddev": _stddev(point_differentials),
                    "worst_quartile_point_share": worst_quartile_point_share,
                    "consistency_score": consistency_score,
                    "consistency_evidence_status": evidence_status,
                    "feature_evidence_status": evidence_status,
                }
            )

    return PlayerPerformanceFeaturesResult(rows=tuple(rows))


def build_player_development_features(
    player_rating_history_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    player_assessment_history_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    player_registrations_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
    features_config: dict[str, Any],
    evidence_windows_config: dict[str, Any],
) -> PlayerDevelopmentFeaturesResult:
    """Build one player development feature row per player as of the analysis date."""
    trend_window_days = int(evidence_windows_config.get("trend_window_days", 180))
    trend_window_start = analysis_as_of_date - timedelta(days=trend_window_days)

    ratings_by_player = _group_rows_by_key(
        [
            row
            for row in player_rating_history_rows
            if (_parse_date_value(row.get("rating_effective_date")) or analysis_as_of_date) <= analysis_as_of_date
        ],
        "player_id",
    )
    assessments_by_player = _group_rows_by_key(
        [
            row
            for row in player_assessment_history_rows
            if (_parse_date_value(row.get("assessment_date")) or analysis_as_of_date) <= analysis_as_of_date
        ],
        "player_id",
    )
    registrations_by_player = _group_rows_by_key(player_registrations_rows, "player_id")
    rows: list[dict[str, Any]] = []

    for player_row in sorted(players_rows, key=lambda row: _normalize_required_string(row.get("player_id"))):
        player_id = _normalize_required_string(player_row.get("player_id"))
        rating_rows = sorted(
            ratings_by_player.get(player_id, []),
            key=lambda row: _parse_required_date(row.get("rating_effective_date")),
        )
        assessment_rows = sorted(
            assessments_by_player.get(player_id, []),
            key=lambda row: _parse_required_date(row.get("assessment_date")),
        )
        registration_rows = sorted(
            registrations_by_player.get(player_id, []),
            key=lambda row: _parse_date_value(row.get("registration_date")) or analysis_as_of_date,
        )

        latest_rating_row = rating_rows[-1] if rating_rows else None
        latest_assessment_row = assessment_rows[-1] if assessment_rows else None
        earliest_registration_date = next(
            (
                _parse_date_value(row.get("registration_date"))
                for row in registration_rows
                if _parse_date_value(row.get("registration_date")) is not None
            ),
            None,
        )
        rating_change_90 = _dated_value_change(
            rating_rows,
            date_field="rating_effective_date",
            value_field="analytical_rating_value",
            analysis_as_of_date=analysis_as_of_date,
            days=90,
        )
        rating_change_180 = _dated_value_change(
            rating_rows,
            date_field="rating_effective_date",
            value_field="analytical_rating_value",
            analysis_as_of_date=analysis_as_of_date,
            days=trend_window_days,
        )
        rating_change_total = _total_value_change(
            rating_rows,
            value_field="analytical_rating_value",
        )
        recent_rating_rows = _filter_rows_to_window(
            rating_rows,
            analysis_as_of_date=analysis_as_of_date,
            window_days=trend_window_days,
            date_field="rating_effective_date",
        )
        recent_assessment_rows = _filter_rows_to_window(
            assessment_rows,
            analysis_as_of_date=analysis_as_of_date,
            window_days=trend_window_days,
            date_field="assessment_date",
        )
        rating_slope_per_30_days = _linear_slope_per_30_days(
            recent_rating_rows,
            date_field="rating_effective_date",
            value_field="analytical_rating_value",
        )
        assessment_change_180 = _dated_value_change(
            assessment_rows,
            date_field="assessment_date",
            value_field="assessment_value",
            analysis_as_of_date=analysis_as_of_date,
            days=trend_window_days,
        )
        assessment_slope_per_30_days = _linear_slope_per_30_days(
            recent_assessment_rows,
            date_field="assessment_date",
            value_field="assessment_value",
        )
        confidence_change_180 = _dated_value_change(
            assessment_rows,
            date_field="assessment_date",
            value_field="assessment_confidence",
            analysis_as_of_date=analysis_as_of_date,
            days=trend_window_days,
        )
        rated_match_count = _coerce_int(latest_rating_row.get("rated_match_count")) if latest_rating_row else 0
        prior_trend_row = _latest_row_on_or_before(
            rating_rows,
            trend_window_start,
            date_field="rating_effective_date",
        )
        experience_growth = (
            None
            if latest_rating_row is None or prior_trend_row is None
            else (_coerce_int(latest_rating_row.get("rated_match_count")) or 0)
            - (_coerce_int(prior_trend_row.get("rated_match_count")) or 0)
        )
        development_momentum_score = _development_momentum_score(
            rating_change_180=rating_change_180,
            assessment_change_180=assessment_change_180,
            confidence_change_180=confidence_change_180,
            experience_growth=experience_growth,
        )
        evidence_status = _development_evidence_status(
            rating_rows=recent_rating_rows,
            assessment_rows=recent_assessment_rows,
        )

        rows.append(
            {
                "player_id": player_id,
                "analysis_as_of_date": analysis_as_of_date,
                "display_name": _normalize_optional_string(player_row.get("display_name")),
                "country_code": _normalize_optional_string(player_row.get("country_code")),
                "active_flag": _coerce_bool(player_row.get("active_flag")),
                "latest_analytical_rating_value": _coerce_float(
                    latest_rating_row.get("analytical_rating_value") if latest_rating_row else None
                ),
                "latest_assessment_value": _coerce_float(
                    latest_assessment_row.get("assessment_value") if latest_assessment_row else None
                ),
                "latest_assessment_confidence": _coerce_float(
                    latest_assessment_row.get("assessment_confidence") if latest_assessment_row else None
                ),
                "rating_change_90": rating_change_90,
                "rating_change_180": rating_change_180,
                "rating_change_total": rating_change_total,
                "rating_slope_per_30_days": rating_slope_per_30_days,
                "assessment_change_180": assessment_change_180,
                "assessment_slope_per_30_days": assessment_slope_per_30_days,
                "confidence_change_180": confidence_change_180,
                "rated_match_count": rated_match_count or 0,
                "experience_growth_180": experience_growth,
                "days_since_registration": (
                    None
                    if earliest_registration_date is None
                    else max((analysis_as_of_date - earliest_registration_date).days, 0)
                ),
                "current_registration_flag": any(
                    _coerce_bool(row.get("current_registration_flag")) for row in registration_rows
                ),
                "development_momentum_score": development_momentum_score,
                "feature_evidence_status": evidence_status,
            }
        )

    return PlayerDevelopmentFeaturesResult(rows=tuple(rows))


def publish_player_performance_features(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    features_config: dict[str, Any],
    evidence_windows_config: dict[str, Any],
) -> PlayerPerformanceFeaturesPublicationSummary:
    """Build and publish player_performance_features."""
    competition_matches_fqn = get_gold_target_table_fqn(environment, "competition_player_matches")
    current_ratings_fqn = get_gold_target_table_fqn(environment, "player_current_ratings")
    players_fqn = get_silver_source_table_fqn(environment, "players")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "player_performance_features")
    target_table_fqn = get_gold_target_table_fqn(environment, "player_performance_features")
    result = build_player_performance_features(
        _collect_table_rows(spark, competition_matches_fqn),
        _collect_table_rows(spark, current_ratings_fqn),
        _collect_table_rows(spark, players_fqn),
        analysis_as_of_date=analysis_as_of_date,
        features_config=features_config,
        evidence_windows_config=evidence_windows_config,
    )
    publish_stage_records_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        records=result.rows,
        validation_fn=lambda _spark, table_fqn: _validate_key_constraints(
            _spark,
            table_fqn,
            key_columns=("player_id", "evidence_window"),
            label="player performance features",
        ),
    )
    return PlayerPerformanceFeaturesPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=int(spark.table(current_ratings_fqn).count()),
        output_row_count=int(spark.table(target_table_fqn).count()),
    )


def publish_player_development_features(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    features_config: dict[str, Any],
    evidence_windows_config: dict[str, Any],
) -> PlayerDevelopmentFeaturesPublicationSummary:
    """Build and publish player_development_features."""
    rating_history_fqn = get_gold_target_table_fqn(environment, "player_rating_history")
    assessment_fqn = get_silver_source_table_fqn(environment, "player_assessment_history")
    registrations_fqn = get_silver_source_table_fqn(environment, "player_registrations")
    players_fqn = get_silver_source_table_fqn(environment, "players")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "player_development_features")
    target_table_fqn = get_gold_target_table_fqn(environment, "player_development_features")
    result = build_player_development_features(
        _collect_table_rows(spark, rating_history_fqn),
        _collect_table_rows(spark, assessment_fqn),
        _collect_table_rows(spark, registrations_fqn),
        _collect_table_rows(spark, players_fqn),
        analysis_as_of_date=analysis_as_of_date,
        features_config=features_config,
        evidence_windows_config=evidence_windows_config,
    )
    publish_stage_records_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        records=result.rows,
        validation_fn=lambda _spark, table_fqn: _validate_key_constraints(
            _spark,
            table_fqn,
            key_columns=("player_id",),
            label="player development features",
        ),
    )
    return PlayerDevelopmentFeaturesPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=int(spark.table(players_fqn).count()),
        output_row_count=int(spark.table(target_table_fqn).count()),
    )


def _resolve_evidence_windows(evidence_windows_config: dict[str, Any]) -> dict[str, int | None]:
    return {
        "career": None,
        "trailing_365": int(evidence_windows_config.get("primary_window_days", 365)),
        "trailing_180": int(evidence_windows_config.get("trend_window_days", 180)),
        "trailing_90": int(evidence_windows_config.get("recent_window_days", 90)),
    }


def _filter_rows_to_window(
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
    window_days: int | None,
    date_field: str,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        row_date = _parse_date_value(row.get(date_field))
        if row_date is None or row_date > analysis_as_of_date:
            continue
        if window_days is not None and (analysis_as_of_date - row_date).days > window_days:
            continue
        filtered.append(row)
    return filtered


def _expected_from_match_row(row: dict[str, Any]) -> float | None:
    team_rating = _coerce_float(row.get("pre_match_team_rating"))
    opponent_rating = _coerce_float(row.get("pre_match_opponent_team_rating"))
    if team_rating is None or opponent_rating is None:
        return None
    return expected_win_probability(team_rating, opponent_rating)


def _partner_frequency(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        partner_id = _normalize_optional_string(row.get("partner_player_id"))
        if partner_id is None:
            continue
        counts[partner_id] = counts.get(partner_id, 0) + 1
    return counts


def _recency_weighted_outcomes(
    rows: list[dict[str, Any]],
    *,
    analysis_as_of_date: date,
    half_life_days: float,
) -> tuple[float, float]:
    weighted_match_total = 0.0
    weighted_win_total = 0.0
    for row in rows:
        match_date = _parse_date_value(row.get("match_date"))
        if match_date is None:
            continue
        age_days = max((analysis_as_of_date - match_date).days, 0)
        weight = 0.5 ** (age_days / max(half_life_days, 1.0))
        weighted_match_total += weight
        if _coerce_bool(row.get("won_flag")):
            weighted_win_total += weight
    return weighted_match_total, weighted_win_total


def _performance_evidence_status(*, match_count: int, minimum_matches_for_consistency: int) -> str:
    if match_count == 0:
        return NO_EVIDENCE
    if match_count < minimum_matches_for_consistency:
        return LIMITED_EVIDENCE
    return SUFFICIENT_EVIDENCE


def _consistency_score(
    *,
    win_count: int,
    match_count: int,
    point_shares: list[float | None],
    worst_quartile_point_share: float | None,
    minimum_matches_for_consistency: int,
) -> float | None:
    if match_count < minimum_matches_for_consistency:
        return None
    win_pct = _safe_divide(win_count, match_count) or 0.0
    point_share_stddev = _stddev(point_shares) or 0.25
    point_stability = max(0.0, min(1.0, 1.0 - (point_share_stddev / 0.25)))
    tail_quality = max(0.0, min(1.0, (worst_quartile_point_share or 0.0)))
    return round(100.0 * ((0.45 * win_pct) + (0.35 * point_stability) + (0.20 * tail_quality)), 4)


def _worst_quartile_average(values: list[float | None]) -> float | None:
    clean_values = sorted(value for value in values if value is not None)
    if not clean_values:
        return None
    quartile_size = max(1, math.ceil(len(clean_values) * 0.25))
    subset = clean_values[:quartile_size]
    return sum(subset) / len(subset)


def _dated_value_change(
    rows: list[dict[str, Any]],
    *,
    date_field: str,
    value_field: str,
    analysis_as_of_date: date,
    days: int,
) -> float | None:
    latest_row = _latest_row_on_or_before(rows, analysis_as_of_date, date_field=date_field)
    window_rows = _filter_rows_to_window(
        rows,
        analysis_as_of_date=analysis_as_of_date,
        window_days=days,
        date_field=date_field,
    )
    prior_row = min(
        window_rows,
        key=lambda row: _parse_required_date(row.get(date_field)),
    ) if window_rows else None
    latest_value = _coerce_float(latest_row.get(value_field)) if latest_row else None
    prior_value = _coerce_float(prior_row.get(value_field)) if prior_row else None
    if latest_value is None or prior_value is None:
        return None
    return latest_value - prior_value


def _total_value_change(
    rows: list[dict[str, Any]],
    *,
    value_field: str,
) -> float | None:
    if len(rows) < 2:
        return None
    first_value = _coerce_float(rows[0].get(value_field))
    last_value = _coerce_float(rows[-1].get(value_field))
    if first_value is None or last_value is None:
        return None
    return last_value - first_value


def _linear_slope_per_30_days(
    rows: list[dict[str, Any]],
    *,
    date_field: str,
    value_field: str,
) -> float | None:
    points = [
        (_parse_date_value(row.get(date_field)), _coerce_float(row.get(value_field)))
        for row in rows
    ]
    clean_points = [(point_date, point_value) for point_date, point_value in points if point_date is not None and point_value is not None]
    if len(clean_points) < 2:
        return None
    origin = clean_points[0][0]
    x_values = [(point_date - origin).days for point_date, _ in clean_points]
    y_values = [point_value for _, point_value in clean_points]
    mean_x = sum(x_values) / len(x_values)
    mean_y = sum(y_values) / len(y_values)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, y_values))
    denominator = sum((x - mean_x) ** 2 for x in x_values)
    if denominator == 0:
        return None
    return (numerator / denominator) * 30.0


def _latest_row_on_or_before(
    rows: list[dict[str, Any]],
    boundary_date: date,
    *,
    date_field: str,
) -> dict[str, Any] | None:
    eligible_rows = [
        row
        for row in rows
        if (_parse_date_value(row.get(date_field)) or boundary_date) <= boundary_date
    ]
    if not eligible_rows:
        return None
    return max(eligible_rows, key=lambda row: _parse_required_date(row.get(date_field)))


def _development_evidence_status(
    *,
    rating_rows: list[dict[str, Any]],
    assessment_rows: list[dict[str, Any]],
) -> str:
    if not rating_rows and not assessment_rows:
        return NO_EVIDENCE
    if len(rating_rows) < 2 or len(assessment_rows) < 2:
        return LIMITED_EVIDENCE
    return SUFFICIENT_EVIDENCE


def _development_momentum_score(
    *,
    rating_change_180: float | None,
    assessment_change_180: float | None,
    confidence_change_180: float | None,
    experience_growth: int | None,
) -> float:
    rating_component = _bounded_positive(rating_change_180, scale=100.0)
    assessment_component = _bounded_positive(assessment_change_180, scale=1.0)
    confidence_component = _bounded_positive(confidence_change_180, scale=0.5)
    experience_component = _bounded_positive(
        float(experience_growth) if experience_growth is not None else None,
        scale=10.0,
    )
    return round(
        100.0
        * (
            (0.40 * rating_component)
            + (0.25 * assessment_component)
            + (0.20 * confidence_component)
            + (0.15 * experience_component)
        ),
        4,
    )


def _bounded_positive(value: float | None, *, scale: float) -> float:
    if value is None:
        return 0.0
    return max(0.0, min(1.0, value / scale))


def _collect_table_rows(spark: Any, table_fqn: str) -> list[dict[str, Any]]:
    return [
        row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        for row in spark.table(table_fqn).toLocalIterator()
    ]


def _group_rows_by_key(
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    key_name: str,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key_value = _normalize_optional_string(row.get(key_name))
        if key_value is None:
            continue
        grouped.setdefault(key_value, []).append(row)
    return grouped


def _validate_key_constraints(
    spark: Any,
    table_fqn: str,
    *,
    key_columns: tuple[str, ...],
    label: str,
) -> None:
    null_conditions = " OR ".join(f"{column} IS NULL" for column in key_columns)
    grouping = ", ".join(key_columns)
    validation_row = spark.sql(
        f"""
SELECT
    SUM(CASE WHEN {null_conditions} THEN 1 ELSE 0 END) AS null_key_count,
    SUM(CASE WHEN duplicate_key_count > 1 THEN 1 ELSE 0 END) AS duplicate_group_count
FROM (
    SELECT
        {grouping},
        COUNT(*) AS duplicate_key_count
    FROM {table_fqn}
    GROUP BY {grouping}
)
""".strip()
    ).collect()[0]
    mapping = validation_row.asDict(recursive=True) if hasattr(validation_row, "asDict") else dict(validation_row)
    if int(mapping["null_key_count"] or 0) != 0 or int(mapping["duplicate_group_count"] or 0) != 0:
        raise ValueError(
            f"{label.title()} validation failed for {table_fqn}: "
            f"null_key_count={int(mapping['null_key_count'] or 0)}, "
            f"duplicate_group_count={int(mapping['duplicate_group_count'] or 0)}."
        )


def _mean(values: list[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _stddev(values: list[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    if len(clean) < 2:
        return None
    mean_value = sum(clean) / len(clean)
    variance = sum((value - mean_value) ** 2 for value in clean) / len(clean)
    return math.sqrt(variance)


def _safe_divide(numerator: float | int, denominator: float | int) -> float | None:
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)


def _normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_required_string(value: Any) -> str:
    normalized = _normalize_optional_string(value)
    if normalized is None:
        raise ValueError("Expected a non-empty string value.")
    return normalized


def _parse_date_value(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _parse_required_date(value: Any) -> date:
    parsed = _parse_date_value(value)
    if parsed is None:
        raise ValueError("Expected a valid ISO date value.")
    return parsed


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().upper() in {"TRUE", "1", "YES", "Y"}


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
