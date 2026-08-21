"""Phase 5 analytical rating-engine builders for the Silver-to-Gold pipeline."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime
import math
from typing import Any

try:
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )
except ModuleNotFoundError:  # pragma: no cover - local test fallback when pyspark is unavailable
    class _FallbackType:
        def __repr__(self) -> str:
            return self.__class__.__name__

    class BooleanType(_FallbackType):
        pass

    class DateType(_FallbackType):
        pass

    class DoubleType(_FallbackType):
        pass

    class LongType(_FallbackType):
        pass

    class StringType(_FallbackType):
        pass

    class StructField:
        def __init__(self, name: str, dataType: Any, nullable: bool):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable

    class StructType(list):
        def __init__(self, fields: list[StructField]):
            super().__init__(fields)

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_stage_table_fqn,
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.publish import (
    publish_sql_table,
    publish_stage_to_gold_table,
)


VERY_HIGH = "VERY_HIGH"
HIGH = "HIGH"
MODERATE = "MODERATE"
LOW = "LOW"
VERY_LOW = "VERY_LOW"

VOLUME_SCALE_DEFAULT = 10.0
RECENCY_HALF_LIFE_DAYS = 180.0
MAX_UNCERTAINTY_DEFAULT = 200.0
DEFAULT_RATING_SCALE = 400.0
RATING_EVENT_BATCH_SIZE = 10000
RATING_EVENT_SOURCE_PAGE_SIZE = 50000


PLAYER_RATING_EVENTS_SCHEMA = StructType(
    [
        StructField("match_id", StringType(), False),
        StructField("match_date", DateType(), False),
        StructField("batch_id", StringType(), True),
        StructField("batch_sequence", LongType(), True),
        StructField("batch_date", DateType(), True),
        StructField("team_number", LongType(), True),
        StructField("player_id", StringType(), False),
        StructField("partner_player_id", StringType(), False),
        StructField("opponent_player_one_id", StringType(), False),
        StructField("opponent_player_two_id", StringType(), False),
        StructField("source_pre_match_player_rating", DoubleType(), True),
        StructField("pre_match_rating", DoubleType(), False),
        StructField("team_pre_match_rating", DoubleType(), False),
        StructField("opponent_team_pre_match_rating", DoubleType(), False),
        StructField("expected_win_probability", DoubleType(), False),
        StructField("actual_result", DoubleType(), False),
        StructField("won_flag", BooleanType(), False),
        StructField("lost_flag", BooleanType(), False),
        StructField("k_factor", DoubleType(), False),
        StructField("margin_multiplier", DoubleType(), False),
        StructField("rating_delta", DoubleType(), False),
        StructField("post_match_rating", DoubleType(), False),
        StructField("prior_match_count", LongType(), False),
        StructField("post_match_count", LongType(), False),
        StructField("wins_to_date", LongType(), False),
        StructField("losses_to_date", LongType(), False),
        StructField("event_sequence", LongType(), False),
    ]
)

PLAYER_RATING_HISTORY_SCHEMA = StructType(
    [
        StructField("player_id", StringType(), False),
        StructField("rating_effective_date", DateType(), False),
        StructField("latest_match_id", StringType(), False),
        StructField("latest_event_sequence", LongType(), False),
        StructField("batch_id", StringType(), True),
        StructField("batch_sequence", LongType(), True),
        StructField("batch_date", DateType(), True),
        StructField("analytical_rating_value", DoubleType(), False),
        StructField("rating_change_from_prior", DoubleType(), True),
        StructField("rated_match_count", LongType(), False),
        StructField("wins_to_date", LongType(), False),
        StructField("losses_to_date", LongType(), False),
        StructField("last_rated_match_date", DateType(), False),
        StructField("rating_reliability_score", DoubleType(), False),
        StructField("rating_evidence_band", StringType(), False),
        StructField("rating_uncertainty_proxy", DoubleType(), False),
        StructField("is_current_flag", BooleanType(), False),
    ]
)

PLAYER_CURRENT_RATINGS_SCHEMA = StructType(
    [
        StructField("player_id", StringType(), False),
        StructField("display_name", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("active_flag", BooleanType(), False),
        StructField("source_rating_value", DoubleType(), True),
        StructField("source_confidence_score", DoubleType(), True),
        StructField("analytical_rating_value", DoubleType(), False),
        StructField("rating_difference_from_source", DoubleType(), True),
        StructField("rating_reliability_score", DoubleType(), False),
        StructField("rating_evidence_band", StringType(), False),
        StructField("rating_uncertainty_proxy", DoubleType(), False),
        StructField("rated_match_count", LongType(), False),
        StructField("wins_to_date", LongType(), False),
        StructField("losses_to_date", LongType(), False),
        StructField("last_rated_match_date", DateType(), True),
        StructField("current_rating_effective_date", DateType(), True),
        StructField("analytical_rating_rank_overall", LongType(), False),
    ]
)


@dataclass(frozen=True)
class PlayerRatingEventsResult:
    """Built Phase 5 player rating-event rows."""

    rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class PlayerRatingHistoryResult:
    """Built Phase 5 player rating-history rows."""

    rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class PlayerCurrentRatingsResult:
    """Built Phase 5 current-player rating rows."""

    rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class PlayerRatingEventsPublicationSummary:
    """Published-table summary for player_rating_events."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class PlayerRatingHistoryPublicationSummary:
    """Published-table summary for player_rating_history."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class PlayerCurrentRatingsPublicationSummary:
    """Published-table summary for player_current_ratings."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


def build_player_rating_events(
    competition_player_matches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
) -> PlayerRatingEventsResult:
    """Build deterministic Elo-style player rating events in chronological match order."""
    default_rating = float(ratings_config.get("default_rating", 1500.0))
    base_k_factor = float(ratings_config.get("k_factor", 32.0))
    margin_multiplier = float(ratings_config.get("margin_multiplier", 1.0))
    rating_floor = float(ratings_config.get("rating_floor", 1000.0))
    rating_ceiling = float(ratings_config.get("rating_ceiling", 3000.0))

    player_state: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    matches_by_id = _group_rows_by_key(competition_player_matches_rows, "match_id")
    ordered_match_ids = sorted(
        matches_by_id,
        key=lambda match_id: _match_sort_key(matches_by_id[match_id]),
    )
    event_sequence = 0

    for match_id in ordered_match_ids:
        match_rows = matches_by_id[match_id]
        event_sequence, built_rows = _build_rating_event_rows_for_match(
            match_rows,
            analysis_as_of_date=analysis_as_of_date,
            default_rating=default_rating,
            base_k_factor=base_k_factor,
            margin_multiplier=margin_multiplier,
            rating_floor=rating_floor,
            rating_ceiling=rating_ceiling,
            player_state=player_state,
            event_sequence=event_sequence,
        )
        rows.extend(built_rows)

    rows.sort(
        key=lambda row: (
            row["match_date"],
            row.get("batch_sequence") if row.get("batch_sequence") is not None else 0,
            row["match_id"],
            int(row["team_number"]),
            row["player_id"],
        )
    )
    return PlayerRatingEventsResult(rows=tuple(rows))


def build_player_rating_history(
    player_rating_events_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
) -> PlayerRatingHistoryResult:
    """Build end-of-day player rating history snapshots from rating events."""
    player_rows_by_id = {
        _normalize_optional_string(row.get("player_id")): row
        for row in players_rows
        if _normalize_optional_string(row.get("player_id")) is not None
    }
    rows: list[dict[str, Any]] = []
    latest_by_player_date: dict[tuple[str, date], dict[str, Any]] = {}

    for event_row in sorted(
        player_rating_events_rows,
        key=lambda row: (
            row["match_date"],
            row.get("batch_sequence") if row.get("batch_sequence") is not None else 0,
            row["match_id"],
            row["event_sequence"],
            row["player_id"],
        ),
    ):
        player_id = _normalize_required_string(event_row.get("player_id"))
        rating_effective_date = _parse_date_value(event_row.get("match_date"))
        if rating_effective_date is None or rating_effective_date > analysis_as_of_date:
            continue
        latest_by_player_date[(player_id, rating_effective_date)] = dict(event_row)

    current_effective_date_by_player: dict[str, date] = {}
    for player_id, rating_effective_date in latest_by_player_date:
        current_effective_date_by_player[player_id] = max(
            rating_effective_date,
            current_effective_date_by_player.get(player_id, rating_effective_date),
        )

    prior_rating_by_player: dict[str, float] = {}
    for player_id, rating_effective_date in sorted(
        latest_by_player_date,
        key=lambda item: (item[0], item[1]),
    ):
        event_row = latest_by_player_date[(player_id, rating_effective_date)]
        player_row = player_rows_by_id.get(player_id, {})
        analytical_rating_value = float(event_row["post_match_rating"])
        prior_rating = prior_rating_by_player.get(player_id)
        reliability_score = rating_reliability_score(
            rated_match_count=int(event_row["post_match_count"]),
            days_since_last_match=max((analysis_as_of_date - rating_effective_date).days, 0),
            source_confidence_score=_coerce_float(player_row.get("rating_confidence")),
            minimum_matches_for_reliability=int(
                ratings_config.get("minimum_matches_for_reliability", VOLUME_SCALE_DEFAULT)
            ),
        )
        rows.append(
            {
                "player_id": player_id,
                "rating_effective_date": rating_effective_date,
                "latest_match_id": event_row["match_id"],
                "latest_event_sequence": event_row["event_sequence"],
                "batch_id": event_row.get("batch_id"),
                "batch_sequence": event_row.get("batch_sequence"),
                "batch_date": event_row.get("batch_date"),
                "analytical_rating_value": analytical_rating_value,
                "rating_change_from_prior": (
                    None if prior_rating is None else analytical_rating_value - prior_rating
                ),
                "rated_match_count": int(event_row["post_match_count"]),
                "wins_to_date": int(event_row["wins_to_date"]),
                "losses_to_date": int(event_row["losses_to_date"]),
                "last_rated_match_date": rating_effective_date,
                "rating_reliability_score": reliability_score,
                "rating_evidence_band": rating_evidence_band(reliability_score),
                "rating_uncertainty_proxy": rating_uncertainty_proxy(reliability_score),
                "is_current_flag": rating_effective_date == current_effective_date_by_player[player_id],
            }
        )
        prior_rating_by_player[player_id] = analytical_rating_value

    return PlayerRatingHistoryResult(rows=tuple(rows))


def build_player_current_ratings(
    player_rating_history_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
) -> PlayerCurrentRatingsResult:
    """Build one current analytical rating row per player in the Silver player universe."""
    default_rating = float(ratings_config.get("default_rating", 1500.0))
    history_by_player: dict[str, dict[str, Any]] = {}
    for history_row in sorted(
        player_rating_history_rows,
        key=lambda row: (
            row["player_id"],
            row["rating_effective_date"],
            row.get("latest_event_sequence") if row.get("latest_event_sequence") is not None else 0,
        ),
    ):
        history_by_player[_normalize_required_string(history_row.get("player_id"))] = dict(history_row)

    rows: list[dict[str, Any]] = []
    for player_row in sorted(
        players_rows,
        key=lambda row: _normalize_required_string(row.get("player_id")),
    ):
        player_id = _normalize_required_string(player_row.get("player_id"))
        history_row = history_by_player.get(player_id)
        source_rating_value = _coerce_float(player_row.get("rating"))
        source_confidence_score = _coerce_float(player_row.get("rating_confidence"))

        if history_row is None:
            reliability_score = rating_reliability_score(
                rated_match_count=0,
                days_since_last_match=None,
                source_confidence_score=source_confidence_score,
                minimum_matches_for_reliability=int(
                    ratings_config.get("minimum_matches_for_reliability", VOLUME_SCALE_DEFAULT)
                ),
            )
            current_row = {
                "player_id": player_id,
                "display_name": _normalize_optional_string(player_row.get("display_name")),
                "country_code": _normalize_optional_string(player_row.get("country_code")),
                "active_flag": _coerce_bool(player_row.get("active_flag")),
                "source_rating_value": source_rating_value,
                "source_confidence_score": source_confidence_score,
                "analytical_rating_value": default_rating,
                "rating_difference_from_source": (
                    None
                    if source_rating_value is None
                    else default_rating - source_rating_value
                ),
                "rating_reliability_score": reliability_score,
                "rating_evidence_band": rating_evidence_band(reliability_score),
                "rating_uncertainty_proxy": rating_uncertainty_proxy(reliability_score),
                "rated_match_count": 0,
                "wins_to_date": 0,
                "losses_to_date": 0,
                "last_rated_match_date": None,
                "current_rating_effective_date": None,
            }
        else:
            current_row = {
                "player_id": player_id,
                "display_name": _normalize_optional_string(player_row.get("display_name")),
                "country_code": _normalize_optional_string(player_row.get("country_code")),
                "active_flag": _coerce_bool(player_row.get("active_flag")),
                "source_rating_value": source_rating_value,
                "source_confidence_score": source_confidence_score,
                "analytical_rating_value": float(history_row["analytical_rating_value"]),
                "rating_difference_from_source": (
                    None
                    if source_rating_value is None
                    else float(history_row["analytical_rating_value"]) - source_rating_value
                ),
                "rating_reliability_score": float(history_row["rating_reliability_score"]),
                "rating_evidence_band": history_row["rating_evidence_band"],
                "rating_uncertainty_proxy": float(history_row["rating_uncertainty_proxy"]),
                "rated_match_count": int(history_row["rated_match_count"]),
                "wins_to_date": int(history_row["wins_to_date"]),
                "losses_to_date": int(history_row["losses_to_date"]),
                "last_rated_match_date": history_row["last_rated_match_date"],
                "current_rating_effective_date": history_row["rating_effective_date"],
            }
        rows.append(current_row)

    for rank_index, row in enumerate(
        sorted(
            rows,
            key=lambda row: (-float(row["analytical_rating_value"]), row["player_id"]),
        ),
        start=1,
    ):
        row["analytical_rating_rank_overall"] = rank_index

    rows.sort(key=lambda row: row["player_id"])
    return PlayerCurrentRatingsResult(rows=tuple(rows))


def publish_player_rating_events(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
) -> PlayerRatingEventsPublicationSummary:
    """Build and publish player_rating_events from competition_player_matches."""
    source_table_fqn = get_gold_target_table_fqn(environment, "competition_player_matches")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "player_rating_events")
    target_table_fqn = get_gold_target_table_fqn(environment, "player_rating_events")
    _publish_player_rating_events_streaming(
        spark,
        source_table_fqn=source_table_fqn,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        analysis_as_of_date=analysis_as_of_date,
        ratings_config=ratings_config,
        validation_fn=lambda _spark, table_fqn: _validate_key_constraints(
            _spark,
            table_fqn,
            key_columns=("match_id", "player_id"),
            label="player rating events",
        ),
    )
    return PlayerRatingEventsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=int(spark.table(source_table_fqn).count()),
        output_row_count=int(spark.table(target_table_fqn).count()),
    )


def publish_player_rating_history(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
) -> PlayerRatingHistoryPublicationSummary:
    """Build and publish player_rating_history from player_rating_events."""
    events_table_fqn = get_gold_target_table_fqn(environment, "player_rating_events")
    players_table_fqn = get_silver_source_table_fqn(environment, "players")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "player_rating_history")
    target_table_fqn = get_gold_target_table_fqn(environment, "player_rating_history")
    publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_player_rating_history_sql(
            events_table_fqn=events_table_fqn,
            players_table_fqn=players_table_fqn,
            analysis_as_of_date=analysis_as_of_date,
            ratings_config=ratings_config,
        ),
        validation_fn=lambda _spark, table_fqn: _validate_key_constraints(
            _spark,
            table_fqn,
            key_columns=("player_id", "rating_effective_date"),
            label="player rating history",
        ),
    )
    return PlayerRatingHistoryPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=int(spark.table(events_table_fqn).count()),
        output_row_count=int(spark.table(target_table_fqn).count()),
    )


def publish_player_current_ratings(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
) -> PlayerCurrentRatingsPublicationSummary:
    """Build and publish player_current_ratings from history plus the Silver player universe."""
    history_table_fqn = get_gold_target_table_fqn(environment, "player_rating_history")
    players_table_fqn = get_silver_source_table_fqn(environment, "players")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "player_current_ratings")
    target_table_fqn = get_gold_target_table_fqn(environment, "player_current_ratings")
    publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_player_current_ratings_sql(
            history_table_fqn=history_table_fqn,
            players_table_fqn=players_table_fqn,
            analysis_as_of_date=analysis_as_of_date,
            ratings_config=ratings_config,
        ),
        validation_fn=lambda _spark, table_fqn: _validate_key_constraints(
            _spark,
            table_fqn,
            key_columns=("player_id",),
            label="player current ratings",
        ),
    )
    return PlayerCurrentRatingsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=int(spark.table(players_table_fqn).count()),
        output_row_count=int(spark.table(target_table_fqn).count()),
    )


def build_player_rating_history_sql(
    *,
    events_table_fqn: str,
    players_table_fqn: str,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
) -> str:
    """Return the Spark SQL used to build player_rating_history."""
    analysis_date_literal = analysis_as_of_date.isoformat()
    minimum_matches_for_reliability = int(
        ratings_config.get("minimum_matches_for_reliability", VOLUME_SCALE_DEFAULT)
    )
    reliability_sql = _rating_reliability_score_sql(
        rated_match_count_sql="collapsed.rated_match_count",
        days_since_last_match_sql=f"DATEDIFF(DATE('{analysis_date_literal}'), collapsed.rating_effective_date)",
        source_confidence_score_sql="players.rating_confidence",
        minimum_matches_for_reliability=minimum_matches_for_reliability,
        null_days_since_last_match=False,
    )

    return f"""
WITH daily_latest_events AS (
    SELECT
        event.player_id,
        CAST(event.match_date AS DATE) AS rating_effective_date,
        event.match_id AS latest_match_id,
        event.event_sequence AS latest_event_sequence,
        event.batch_id,
        event.batch_sequence,
        CAST(event.batch_date AS DATE) AS batch_date,
        event.post_match_rating AS analytical_rating_value,
        event.post_match_count AS rated_match_count,
        event.wins_to_date,
        event.losses_to_date,
        ROW_NUMBER() OVER (
            PARTITION BY event.player_id, CAST(event.match_date AS DATE)
            ORDER BY event.event_sequence DESC, event.match_id DESC
        ) AS event_rank_within_day
    FROM {events_table_fqn} AS event
),
collapsed AS (
    SELECT
        player_id,
        rating_effective_date,
        latest_match_id,
        latest_event_sequence,
        batch_id,
        batch_sequence,
        batch_date,
        analytical_rating_value,
        rated_match_count,
        wins_to_date,
        losses_to_date
    FROM daily_latest_events
    WHERE event_rank_within_day = 1
),
history_base AS (
    SELECT
        collapsed.player_id,
        collapsed.rating_effective_date,
        collapsed.latest_match_id,
        collapsed.latest_event_sequence,
        collapsed.batch_id,
        collapsed.batch_sequence,
        collapsed.batch_date,
        collapsed.analytical_rating_value,
        LAG(collapsed.analytical_rating_value) OVER (
            PARTITION BY collapsed.player_id
            ORDER BY collapsed.rating_effective_date, collapsed.latest_event_sequence
        ) AS prior_analytical_rating_value,
        collapsed.rated_match_count,
        collapsed.wins_to_date,
        collapsed.losses_to_date,
        collapsed.rating_effective_date AS last_rated_match_date,
        {reliability_sql} AS rating_reliability_score,
        MAX(collapsed.rating_effective_date) OVER (
            PARTITION BY collapsed.player_id
        ) AS current_rating_effective_date
    FROM collapsed
    LEFT JOIN {players_table_fqn} AS players
        ON players.player_id = collapsed.player_id
)
SELECT
    player_id,
    rating_effective_date,
    latest_match_id,
    latest_event_sequence,
    batch_id,
    batch_sequence,
    batch_date,
    analytical_rating_value,
    CASE
        WHEN prior_analytical_rating_value IS NULL THEN NULL
        ELSE analytical_rating_value - prior_analytical_rating_value
    END AS rating_change_from_prior,
    rated_match_count,
    wins_to_date,
    losses_to_date,
    last_rated_match_date,
    rating_reliability_score,
    CASE
        WHEN rating_reliability_score >= 90.0 THEN '{VERY_HIGH}'
        WHEN rating_reliability_score >= 75.0 THEN '{HIGH}'
        WHEN rating_reliability_score >= 50.0 THEN '{MODERATE}'
        WHEN rating_reliability_score >= 25.0 THEN '{LOW}'
        ELSE '{VERY_LOW}'
    END AS rating_evidence_band,
    {MAX_UNCERTAINTY_DEFAULT} * (
        1.0 - (GREATEST(0.0, LEAST(rating_reliability_score, 100.0)) / 100.0)
    ) AS rating_uncertainty_proxy,
    rating_effective_date = current_rating_effective_date AS is_current_flag
FROM history_base
""".strip()


def build_player_current_ratings_sql(
    *,
    history_table_fqn: str,
    players_table_fqn: str,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
) -> str:
    """Return the Spark SQL used to build player_current_ratings."""
    default_rating = float(ratings_config.get("default_rating", 1500.0))
    minimum_matches_for_reliability = int(
        ratings_config.get("minimum_matches_for_reliability", VOLUME_SCALE_DEFAULT)
    )
    reliability_sql = _rating_reliability_score_sql(
        rated_match_count_sql="0",
        days_since_last_match_sql="NULL",
        source_confidence_score_sql="players.rating_confidence",
        minimum_matches_for_reliability=minimum_matches_for_reliability,
        null_days_since_last_match=True,
    )

    return f"""
WITH history_current AS (
    SELECT
        history.player_id,
        history.rating_effective_date,
        history.analytical_rating_value,
        history.rating_reliability_score,
        history.rating_evidence_band,
        history.rating_uncertainty_proxy,
        history.rated_match_count,
        history.wins_to_date,
        history.losses_to_date,
        history.last_rated_match_date
    FROM {history_table_fqn} AS history
    WHERE history.is_current_flag = true
),
current_rows AS (
    SELECT
        players.player_id,
        players.display_name,
        players.country_code,
        players.active_flag,
        CAST(players.rating AS DOUBLE) AS source_rating_value,
        CAST(players.rating_confidence AS DOUBLE) AS source_confidence_score,
        COALESCE(history_current.analytical_rating_value, {default_rating}) AS analytical_rating_value,
        CASE
            WHEN players.rating IS NULL THEN NULL
            ELSE COALESCE(history_current.analytical_rating_value, {default_rating}) - CAST(players.rating AS DOUBLE)
        END AS rating_difference_from_source,
        COALESCE(history_current.rating_reliability_score, {reliability_sql}) AS rating_reliability_score,
        COALESCE(
            history_current.rating_evidence_band,
            CASE
                WHEN {reliability_sql} >= 90.0 THEN '{VERY_HIGH}'
                WHEN {reliability_sql} >= 75.0 THEN '{HIGH}'
                WHEN {reliability_sql} >= 50.0 THEN '{MODERATE}'
                WHEN {reliability_sql} >= 25.0 THEN '{LOW}'
                ELSE '{VERY_LOW}'
            END
        ) AS rating_evidence_band,
        COALESCE(
            history_current.rating_uncertainty_proxy,
            {MAX_UNCERTAINTY_DEFAULT} * (
                1.0 - (GREATEST(0.0, LEAST({reliability_sql}, 100.0)) / 100.0)
            )
        ) AS rating_uncertainty_proxy,
        COALESCE(history_current.rated_match_count, 0) AS rated_match_count,
        COALESCE(history_current.wins_to_date, 0) AS wins_to_date,
        COALESCE(history_current.losses_to_date, 0) AS losses_to_date,
        history_current.last_rated_match_date,
        history_current.rating_effective_date AS current_rating_effective_date
    FROM {players_table_fqn} AS players
    LEFT JOIN history_current
        ON history_current.player_id = players.player_id
)
SELECT
    player_id,
    display_name,
    country_code,
    active_flag,
    source_rating_value,
    source_confidence_score,
    analytical_rating_value,
    rating_difference_from_source,
    rating_reliability_score,
    rating_evidence_band,
    rating_uncertainty_proxy,
    rated_match_count,
    wins_to_date,
    losses_to_date,
    last_rated_match_date,
    current_rating_effective_date,
    ROW_NUMBER() OVER (
        ORDER BY analytical_rating_value DESC, player_id ASC
    ) AS analytical_rating_rank_overall
FROM current_rows
""".strip()


def expected_win_probability(
    team_rating_a: float,
    team_rating_b: float,
    *,
    rating_scale: float = DEFAULT_RATING_SCALE,
) -> float:
    """Return the Elo expected win probability for team A."""
    return 1.0 / (1.0 + 10 ** ((team_rating_b - team_rating_a) / rating_scale))


def experience_multiplier(prior_match_count: int) -> float:
    """Return the configured experience-based K-factor multiplier."""
    if prior_match_count < 10:
        return 1.50
    if prior_match_count < 25:
        return 1.25
    if prior_match_count < 75:
        return 1.00
    return 0.80


def rating_reliability_score(
    *,
    rated_match_count: int,
    days_since_last_match: int | None,
    source_confidence_score: float | None,
    minimum_matches_for_reliability: int,
) -> float:
    """Return a 0-100 reliability score for one analytical rating."""
    volume_scale = max(float(minimum_matches_for_reliability), VOLUME_SCALE_DEFAULT)
    volume_component = 1.0 - math.exp(-(max(rated_match_count, 0) / volume_scale))
    if days_since_last_match is None:
        recency_component = 0.25
    else:
        recency_component = 0.5 ** (max(days_since_last_match, 0) / RECENCY_HALF_LIFE_DAYS)
    quality_component = _normalize_confidence_component(source_confidence_score)
    weighted_log_sum = (
        0.5 * math.log(max(volume_component, 1e-9))
        + 0.3 * math.log(max(recency_component, 1e-9))
        + 0.2 * math.log(max(quality_component, 1e-9))
    )
    return max(0.0, min(100.0, 100.0 * math.exp(weighted_log_sum)))


def rating_evidence_band(score: float) -> str:
    """Return the reliability band label for a score."""
    if score >= 90.0:
        return VERY_HIGH
    if score >= 75.0:
        return HIGH
    if score >= 50.0:
        return MODERATE
    if score >= 25.0:
        return LOW
    return VERY_LOW


def rating_uncertainty_proxy(score: float) -> float:
    """Return a simple uncertainty proxy that declines as reliability increases."""
    return MAX_UNCERTAINTY_DEFAULT * (1.0 - max(0.0, min(score, 100.0)) / 100.0)


def _publish_player_rating_events_streaming(
    spark: Any,
    *,
    source_table_fqn: str,
    stage_table_fqn: str,
    target_table_fqn: str,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
    validation_fn: Any | None = None,
) -> tuple[int, int]:
    player_state: dict[str, dict[str, Any]] = {}
    event_batches = _build_player_rating_event_batches(
        _iter_competition_player_match_rows(
            spark,
            table_fqn=source_table_fqn,
            analysis_as_of_date=analysis_as_of_date,
        ),
        analysis_as_of_date=analysis_as_of_date,
        ratings_config=ratings_config,
        player_state=player_state,
    )
    stage_row_count = _write_record_batches_table(
        spark,
        table_fqn=stage_table_fqn,
        schema=PLAYER_RATING_EVENTS_SCHEMA,
        record_batches=event_batches,
    )
    if validation_fn is not None:
        validation_fn(spark, stage_table_fqn)
    target_row_count = publish_sql_table(
        spark,
        target_table_fqn,
        f"SELECT * FROM {stage_table_fqn}",
    )
    verified_row_count = int(spark.table(target_table_fqn).count())
    if target_row_count != verified_row_count:
        raise ValueError(
            f"Published Gold table {target_table_fqn} did not verify: "
            f"published_row_count={target_row_count}, verified_row_count={verified_row_count}."
        )
    return stage_row_count, verified_row_count


def _iter_competition_player_match_rows(
    spark: Any,
    *,
    table_fqn: str,
    analysis_as_of_date: date,
    page_size: int = RATING_EVENT_SOURCE_PAGE_SIZE,
) -> Iterator[dict[str, Any]]:
    analysis_date_literal = analysis_as_of_date.isoformat()
    last_sort_key: tuple[date, int, str, str, int, str, str] | None = None

    while True:
        continuation_predicate = ""
        if last_sort_key is not None:
            (
                last_match_date,
                last_batch_sequence,
                last_batch_id,
                last_match_id,
                last_team_number,
                last_player_position,
                last_player_id,
            ) = last_sort_key
            last_match_date_literal = last_match_date.isoformat()
            last_batch_id_literal = _sql_string_literal(last_batch_id)
            last_match_id_literal = _sql_string_literal(last_match_id)
            last_player_position_literal = _sql_string_literal(last_player_position)
            last_player_id_literal = _sql_string_literal(last_player_id)
            continuation_predicate = f"""
  AND (
      CAST(match_date AS DATE) > DATE('{last_match_date_literal}')
      OR (
          CAST(match_date AS DATE) = DATE('{last_match_date_literal}')
          AND COALESCE(batch_sequence, 0) > {last_batch_sequence}
      )
      OR (
          CAST(match_date AS DATE) = DATE('{last_match_date_literal}')
          AND COALESCE(batch_sequence, 0) = {last_batch_sequence}
          AND COALESCE(batch_id, '') > '{last_batch_id_literal}'
      )
      OR (
          CAST(match_date AS DATE) = DATE('{last_match_date_literal}')
          AND COALESCE(batch_sequence, 0) = {last_batch_sequence}
          AND COALESCE(batch_id, '') = '{last_batch_id_literal}'
          AND match_id > '{last_match_id_literal}'
      )
      OR (
          CAST(match_date AS DATE) = DATE('{last_match_date_literal}')
          AND COALESCE(batch_sequence, 0) = {last_batch_sequence}
          AND COALESCE(batch_id, '') = '{last_batch_id_literal}'
          AND match_id = '{last_match_id_literal}'
          AND COALESCE(team_number, 0) > {last_team_number}
      )
      OR (
          CAST(match_date AS DATE) = DATE('{last_match_date_literal}')
          AND COALESCE(batch_sequence, 0) = {last_batch_sequence}
          AND COALESCE(batch_id, '') = '{last_batch_id_literal}'
          AND match_id = '{last_match_id_literal}'
          AND COALESCE(team_number, 0) = {last_team_number}
          AND COALESCE(player_position, '') > '{last_player_position_literal}'
      )
      OR (
          CAST(match_date AS DATE) = DATE('{last_match_date_literal}')
          AND COALESCE(batch_sequence, 0) = {last_batch_sequence}
          AND COALESCE(batch_id, '') = '{last_batch_id_literal}'
          AND match_id = '{last_match_id_literal}'
          AND COALESCE(team_number, 0) = {last_team_number}
          AND COALESCE(player_position, '') = '{last_player_position_literal}'
          AND player_id > '{last_player_id_literal}'
      )
  )
""".rstrip()
        query = f"""
SELECT
    match_id,
    CAST(match_date AS DATE) AS match_date,
    batch_id,
    batch_sequence,
    CAST(batch_date AS DATE) AS batch_date,
    team_number,
    player_id,
    partner_player_id,
    pre_match_player_rating,
    won_flag,
    lost_flag,
    player_position
FROM {table_fqn}
WHERE CAST(match_date AS DATE) <= DATE('{analysis_date_literal}')
{continuation_predicate}
ORDER BY
    CAST(match_date AS DATE),
    COALESCE(batch_sequence, 0),
    COALESCE(batch_id, ''),
    match_id,
    COALESCE(team_number, 0),
    COALESCE(player_position, ''),
    player_id
LIMIT {int(page_size)}
""".strip()
        rows = spark.sql(query).collect()
        if not rows:
            break

        last_record: dict[str, Any] | None = None
        for row in rows:
            record = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
            last_record = record
            yield record

        if len(rows) < page_size or last_record is None:
            break
        last_sort_key = _competition_player_match_sort_key(last_record)


def _build_player_rating_event_batches(
    competition_player_matches_rows: Iterable[dict[str, Any]],
    *,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
    player_state: dict[str, dict[str, Any]],
    batch_size: int = RATING_EVENT_BATCH_SIZE,
) -> Iterator[list[dict[str, Any]]]:
    default_rating = float(ratings_config.get("default_rating", 1500.0))
    base_k_factor = float(ratings_config.get("k_factor", 32.0))
    margin_multiplier = float(ratings_config.get("margin_multiplier", 1.0))
    rating_floor = float(ratings_config.get("rating_floor", 1000.0))
    rating_ceiling = float(ratings_config.get("rating_ceiling", 3000.0))

    current_match_rows: list[dict[str, Any]] = []
    current_match_id: str | None = None
    event_sequence = 0
    batch: list[dict[str, Any]] = []

    for row in competition_player_matches_rows:
        match_id = _normalize_required_string(row.get("match_id"))
        if current_match_id is None:
            current_match_id = match_id
        if match_id != current_match_id:
            event_sequence, built_rows = _build_rating_event_rows_for_match(
                current_match_rows,
                analysis_as_of_date=analysis_as_of_date,
                default_rating=default_rating,
                base_k_factor=base_k_factor,
                margin_multiplier=margin_multiplier,
                rating_floor=rating_floor,
                rating_ceiling=rating_ceiling,
                player_state=player_state,
                event_sequence=event_sequence,
            )
            batch.extend(built_rows)
            if len(batch) >= batch_size:
                yield batch
                batch = []
            current_match_rows = []
            current_match_id = match_id
        current_match_rows.append(row)

    if current_match_rows:
        event_sequence, built_rows = _build_rating_event_rows_for_match(
            current_match_rows,
            analysis_as_of_date=analysis_as_of_date,
            default_rating=default_rating,
            base_k_factor=base_k_factor,
            margin_multiplier=margin_multiplier,
            rating_floor=rating_floor,
            rating_ceiling=rating_ceiling,
            player_state=player_state,
            event_sequence=event_sequence,
        )
        batch.extend(built_rows)
    if batch:
        yield batch


def _build_rating_event_rows_for_match(
    match_rows: list[dict[str, Any]],
    *,
    analysis_as_of_date: date,
    default_rating: float,
    base_k_factor: float,
    margin_multiplier: float,
    rating_floor: float,
    rating_ceiling: float,
    player_state: dict[str, dict[str, Any]],
    event_sequence: int,
) -> tuple[int, list[dict[str, Any]]]:
    match_date = _parse_date_value(match_rows[0].get("match_date"))
    if match_date is None or match_date > analysis_as_of_date:
        return event_sequence, []

    team_rows = _normalize_match_player_teams(match_rows)
    if team_rows is None:
        return event_sequence, []

    team_one_rows = team_rows[1]
    team_two_rows = team_rows[2]
    team_one_player_ids = [_normalize_required_string(row.get("player_id")) for row in team_one_rows]
    team_two_player_ids = [_normalize_required_string(row.get("player_id")) for row in team_two_rows]

    team_one_pre_ratings = [
        _get_player_state(player_state, player_id, default_rating)["rating"]
        for player_id in team_one_player_ids
    ]
    team_two_pre_ratings = [
        _get_player_state(player_state, player_id, default_rating)["rating"]
        for player_id in team_two_player_ids
    ]
    team_one_pre_match_rating = sum(team_one_pre_ratings) / 2.0
    team_two_pre_match_rating = sum(team_two_pre_ratings) / 2.0
    team_one_expected = expected_win_probability(
        team_one_pre_match_rating,
        team_two_pre_match_rating,
    )
    team_two_expected = 1.0 - team_one_expected

    team_one_prior_counts = [
        int(_get_player_state(player_state, player_id, default_rating)["match_count"])
        for player_id in team_one_player_ids
    ]
    team_two_prior_counts = [
        int(_get_player_state(player_state, player_id, default_rating)["match_count"])
        for player_id in team_two_player_ids
    ]
    match_k_factor = base_k_factor * (
        (
            sum(experience_multiplier(count) for count in team_one_prior_counts)
            + sum(experience_multiplier(count) for count in team_two_prior_counts)
        )
        / 4.0
    )

    team_one_won = _coerce_bool(team_one_rows[0].get("won_flag"))
    team_two_won = _coerce_bool(team_two_rows[0].get("won_flag"))
    if team_one_won == team_two_won:
        return event_sequence, []

    team_one_actual = 1.0 if team_one_won else 0.0
    team_two_actual = 1.0 if team_two_won else 0.0
    team_one_delta = match_k_factor * margin_multiplier * (team_one_actual - team_one_expected)
    team_two_delta = -team_one_delta

    next_event_sequence = event_sequence + 1
    built_rows = _build_match_player_event_rows(
        match_rows=team_one_rows,
        opponent_player_ids=team_two_player_ids,
        expected_win=team_one_expected,
        actual_result=team_one_actual,
        team_pre_match_rating=team_one_pre_match_rating,
        opponent_pre_match_rating=team_two_pre_match_rating,
        rating_delta=team_one_delta,
        k_factor=match_k_factor,
        margin_multiplier=margin_multiplier,
        player_state=player_state,
        default_rating=default_rating,
        rating_floor=rating_floor,
        rating_ceiling=rating_ceiling,
        event_sequence=next_event_sequence,
    )
    built_rows.extend(
        _build_match_player_event_rows(
            match_rows=team_two_rows,
            opponent_player_ids=team_one_player_ids,
            expected_win=team_two_expected,
            actual_result=team_two_actual,
            team_pre_match_rating=team_two_pre_match_rating,
            opponent_pre_match_rating=team_one_pre_match_rating,
            rating_delta=team_two_delta,
            k_factor=match_k_factor,
            margin_multiplier=margin_multiplier,
            player_state=player_state,
            default_rating=default_rating,
            rating_floor=rating_floor,
            rating_ceiling=rating_ceiling,
            event_sequence=next_event_sequence,
        )
    )
    return next_event_sequence, built_rows


def _write_record_batches_table(
    spark: Any,
    *,
    table_fqn: str,
    schema: Any,
    record_batches: Iterable[list[dict[str, Any]]],
) -> int:
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    total_row_count = 0
    wrote_rows = False
    for batch_index, records in enumerate(record_batches):
        if not records:
            continue
        write_mode = "overwrite" if batch_index == 0 else "append"
        dataframe = spark.createDataFrame(records, schema=schema)
        dataframe.write.format("delta").mode(write_mode).saveAsTable(table_fqn)
        wrote_rows = True
        total_row_count += len(records)
    if not wrote_rows:
        spark.createDataFrame([], schema=schema).write.format("delta").mode("overwrite").saveAsTable(
            table_fqn
        )
    return total_row_count


def _rating_reliability_score_sql(
    *,
    rated_match_count_sql: str,
    days_since_last_match_sql: str,
    source_confidence_score_sql: str,
    minimum_matches_for_reliability: int,
    null_days_since_last_match: bool,
) -> str:
    volume_scale = max(float(minimum_matches_for_reliability), VOLUME_SCALE_DEFAULT)
    volume_component_sql = (
        f"(1.0 - EXP(-(CAST(GREATEST({rated_match_count_sql}, 0) AS DOUBLE) / {volume_scale})))"
    )
    if null_days_since_last_match:
        recency_component_sql = "0.25"
    else:
        recency_component_sql = (
            f"POWER(0.5, CAST(GREATEST({days_since_last_match_sql}, 0) AS DOUBLE) / {RECENCY_HALF_LIFE_DAYS})"
        )
    quality_component_sql = (
        "CASE "
        f"WHEN {source_confidence_score_sql} IS NULL THEN 0.5 "
        f"WHEN {source_confidence_score_sql} <= 1.0 THEN GREATEST(0.0, LEAST(1.0, CAST({source_confidence_score_sql} AS DOUBLE))) "
        f"ELSE GREATEST(0.0, LEAST(1.0, CAST({source_confidence_score_sql} AS DOUBLE) / 100.0)) "
        "END"
    )
    weighted_log_sum_sql = (
        f"(0.5 * LN(GREATEST({volume_component_sql}, 1e-9))"
        f" + 0.3 * LN(GREATEST({recency_component_sql}, 1e-9))"
        f" + 0.2 * LN(GREATEST({quality_component_sql}, 1e-9)))"
    )
    return (
        f"GREATEST(0.0, LEAST(100.0, 100.0 * EXP({weighted_log_sum_sql})))"
    )


def _build_match_player_event_rows(
    *,
    match_rows: list[dict[str, Any]],
    opponent_player_ids: list[str],
    expected_win: float,
    actual_result: float,
    team_pre_match_rating: float,
    opponent_pre_match_rating: float,
    rating_delta: float,
    k_factor: float,
    margin_multiplier: float,
    player_state: dict[str, dict[str, Any]],
    default_rating: float,
    rating_floor: float,
    rating_ceiling: float,
    event_sequence: int,
) -> list[dict[str, Any]]:
    built_rows: list[dict[str, Any]] = []
    ordered_opponent_player_ids = sorted(opponent_player_ids)
    ordered_rows = sorted(
        match_rows,
        key=lambda row: (
            _player_position_sort_key(row.get("player_position")),
            _normalize_required_string(row.get("player_id")),
        ),
    )
    player_ids = [_normalize_required_string(row.get("player_id")) for row in ordered_rows]

    for index, row in enumerate(ordered_rows):
        player_id = player_ids[index]
        partner_player_id = player_ids[1 - index]
        state = _get_player_state(player_state, player_id, default_rating)
        prior_match_count = int(state["match_count"])
        pre_match_rating = float(state["rating"])
        wins_to_date = int(state["wins"])
        losses_to_date = int(state["losses"])
        post_match_rating = _clamp(pre_match_rating + rating_delta, rating_floor, rating_ceiling)
        post_match_count = prior_match_count + 1
        if actual_result == 1.0:
            wins_to_date += 1
        else:
            losses_to_date += 1

        event_row = {
            "match_id": _normalize_required_string(row.get("match_id")),
            "match_date": _parse_required_date(row.get("match_date")),
            "batch_id": _normalize_optional_string(row.get("batch_id")),
            "batch_sequence": _coerce_int(row.get("batch_sequence")),
            "batch_date": _parse_date_value(row.get("batch_date")),
            "team_number": _coerce_int(row.get("team_number")),
            "player_id": player_id,
            "partner_player_id": partner_player_id,
            "opponent_player_one_id": ordered_opponent_player_ids[0],
            "opponent_player_two_id": ordered_opponent_player_ids[1],
            "source_pre_match_player_rating": _coerce_float(row.get("pre_match_player_rating")),
            "pre_match_rating": pre_match_rating,
            "team_pre_match_rating": team_pre_match_rating,
            "opponent_team_pre_match_rating": opponent_pre_match_rating,
            "expected_win_probability": expected_win,
            "actual_result": actual_result,
            "won_flag": actual_result == 1.0,
            "lost_flag": actual_result == 0.0,
            "k_factor": k_factor,
            "margin_multiplier": margin_multiplier,
            "rating_delta": rating_delta,
            "post_match_rating": post_match_rating,
            "prior_match_count": prior_match_count,
            "post_match_count": post_match_count,
            "wins_to_date": wins_to_date,
            "losses_to_date": losses_to_date,
            "event_sequence": event_sequence,
        }
        built_rows.append(event_row)
        state["rating"] = post_match_rating
        state["match_count"] = post_match_count
        state["wins"] = wins_to_date
        state["losses"] = losses_to_date
        state["last_match_date"] = event_row["match_date"]

    return built_rows


def _normalize_match_player_teams(
    match_rows: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]] | None:
    team_rows: dict[int, list[dict[str, Any]]] = {1: [], 2: []}
    for row in match_rows:
        team_number = _coerce_int(row.get("team_number"))
        if team_number not in (1, 2):
            return None
        team_rows[team_number].append(row)
    if len(team_rows[1]) != 2 or len(team_rows[2]) != 2:
        return None
    if {
        _normalize_required_string(row.get("player_id"))
        for row in team_rows[1] + team_rows[2]
    }.__len__() != 4:
        return None
    return team_rows


def _match_sort_key(match_rows: list[dict[str, Any]]) -> tuple[Any, ...]:
    anchor = match_rows[0]
    return (
        _parse_required_date(anchor.get("match_date")),
        _coerce_int(anchor.get("batch_sequence")) or 0,
        _normalize_required_string(anchor.get("batch_id")) or "",
        _normalize_required_string(anchor.get("match_id")),
    )


def _competition_player_match_sort_key(
    row: dict[str, Any],
) -> tuple[date, int, str, str, int, str, str]:
    return (
        _parse_required_date(row.get("match_date")),
        _coerce_int(row.get("batch_sequence")) or 0,
        _normalize_optional_string(row.get("batch_id")) or "",
        _normalize_required_string(row.get("match_id")),
        _coerce_int(row.get("team_number")) or 0,
        _normalize_optional_string(row.get("player_position")) or "",
        _normalize_required_string(row.get("player_id")),
    )


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


def _get_player_state(
    player_state: dict[str, dict[str, Any]],
    player_id: str,
    default_rating: float,
) -> dict[str, Any]:
    return player_state.setdefault(
        player_id,
        {
            "rating": default_rating,
            "match_count": 0,
            "wins": 0,
            "losses": 0,
            "last_match_date": None,
        },
    )


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
    null_key_count = int(mapping["null_key_count"] or 0)
    duplicate_group_count = int(mapping["duplicate_group_count"] or 0)
    if null_key_count != 0 or duplicate_group_count != 0:
        raise ValueError(
            f"{label.title()} validation failed for {table_fqn}: "
            f"null_key_count={null_key_count}, duplicate_group_count={duplicate_group_count}."
        )


def _normalize_confidence_component(value: float | None) -> float:
    if value is None:
        return 0.50
    if value <= 1.0:
        return max(0.0, min(1.0, value))
    return max(0.0, min(1.0, value / 100.0))


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


def _sql_string_literal(value: str) -> str:
    return value.replace("'", "''")


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


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return min(max(value, minimum), maximum)


def _player_position_sort_key(value: Any) -> tuple[int, str]:
    normalized = (_normalize_optional_string(value) or "").upper()
    mapping = {
        "LEFT": 1,
        "RIGHT": 2,
    }
    return mapping.get(normalized, 9), normalized
