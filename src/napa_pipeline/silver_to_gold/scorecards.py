"""Phase 10 player scorecard builders for the Silver-to-Gold pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

try:
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
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

    class IntegerType(_FallbackType):
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
from napa_pipeline.silver_to_gold.publish import publish_stage_records_to_gold_table


@dataclass(frozen=True)
class PlayerEvaluationScorecardsPublicationSummary:
    """Published-table summary for player_evaluation_scorecards."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class NationalPlayerRankingsPublicationSummary:
    """Published-table summary for national_player_rankings."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class Phase10PublicationSummary:
    """Published-table summary for the two Phase 10 target tables."""

    player_evaluation_scorecards: PlayerEvaluationScorecardsPublicationSummary
    national_player_rankings: NationalPlayerRankingsPublicationSummary


PLAYER_EVALUATION_SCORECARDS_SCHEMA = StructType(
    [
        StructField("player_id", StringType(), False),
        StructField("scoring_scenario", StringType(), False),
        StructField("analysis_as_of_date", DateType(), False),
        StructField("display_name", StringType(), True),
        StructField("country_code", StringType(), False),
        StructField("gender_code", StringType(), True),
        StructField("active_flag", BooleanType(), False),
        StructField("eligible_player_flag", BooleanType(), False),
        StructField("source_rating_value", DoubleType(), True),
        StructField("source_confidence_score", DoubleType(), True),
        StructField("analytical_rating_value", DoubleType(), True),
        StructField("rated_match_count_current", IntegerType(), False),
        StructField("rating_reliability_score", DoubleType(), True),
        StructField("rating_evidence_band", StringType(), True),
        StructField("rating_uncertainty_proxy", DoubleType(), True),
        StructField("career_match_count", IntegerType(), False),
        StructField("recent_match_count", IntegerType(), False),
        StructField("performance_above_expectation_raw", DoubleType(), True),
        StructField("game_performance_raw", DoubleType(), True),
        StructField("recent_form_raw", DoubleType(), True),
        StructField("consistency_raw", DoubleType(), True),
        StructField("strength_of_schedule_raw", DoubleType(), True),
        StructField("development_momentum_raw", DoubleType(), True),
        StructField("latest_assessment_confidence", DoubleType(), True),
        StructField("development_feature_evidence_status", StringType(), True),
        StructField("combined_confidence_score", DoubleType(), True),
        StructField("quality_confidence_band", StringType(), True),
        StructField("material_limitation_text", StringType(), True),
        StructField("rating_strength_score", DoubleType(), True),
        StructField("adjusted_performance_score", DoubleType(), True),
        StructField("game_performance_score", DoubleType(), True),
        StructField("recent_form_score", DoubleType(), True),
        StructField("consistency_score", DoubleType(), True),
        StructField("strength_of_schedule_score", DoubleType(), True),
        StructField("development_trend_component_score", DoubleType(), True),
        StructField("development_headroom_component_score", DoubleType(), True),
        StructField("performance_component_score", DoubleType(), True),
        StructField("rating_component_score", DoubleType(), True),
        StructField("consistency_component_score", DoubleType(), True),
        StructField("development_component_score", DoubleType(), True),
        StructField("confidence_component_score", DoubleType(), True),
        StructField("raw_player_evaluation_score", DoubleType(), True),
        StructField("confidence_factor", DoubleType(), False),
        StructField("confidence_adjusted_player_score", DoubleType(), True),
        StructField("development_confidence_component_score", DoubleType(), True),
        StructField("development_potential_score", DoubleType(), True),
        StructField("development_candidate_flag", BooleanType(), False),
        StructField("top_strengths", StringType(), True),
        StructField("top_risks", StringType(), True),
        StructField("evidence_band", StringType(), True),
        StructField("ranking_rationale", StringType(), False),
    ]
)

NATIONAL_PLAYER_RANKINGS_SCHEMA = StructType(
    [
        StructField("country_code", StringType(), False),
        StructField("ranking_group", StringType(), False),
        StructField("scoring_scenario", StringType(), False),
        StructField("player_id", StringType(), False),
        StructField("display_name", StringType(), True),
        StructField("gender_code", StringType(), True),
        StructField("active_flag", BooleanType(), False),
        StructField("rank_metric_name", StringType(), False),
        StructField("rank_metric_value", DoubleType(), False),
        StructField("rank", IntegerType(), False),
        StructField("dense_rank", IntegerType(), False),
        StructField("score_difference_from_next", DoubleType(), True),
        StructField("top_25_flag", BooleanType(), False),
        StructField("confidence_adjusted_player_score", DoubleType(), True),
        StructField("development_potential_score", DoubleType(), True),
        StructField("analytical_rating_value", DoubleType(), True),
        StructField("rated_match_count_current", IntegerType(), False),
        StructField("combined_confidence_score", DoubleType(), True),
        StructField("top_strengths", StringType(), True),
        StructField("top_risks", StringType(), True),
        StructField("evidence_band", StringType(), True),
        StructField("ranking_rationale", StringType(), False),
    ]
)


def publish_phase10_player_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
) -> Phase10PublicationSummary:
    """Publish the Gold Phase 10 player scorecard and ranking tables."""
    scorecard_rows = build_player_evaluation_scorecards(
        players_rows=_collect_table_rows(spark, get_silver_source_table_fqn(environment, "players")),
        player_current_ratings_rows=_collect_table_rows(
            spark,
            get_gold_target_table_fqn(environment, "player_current_ratings"),
        ),
        player_performance_features_rows=_collect_table_rows(
            spark,
            get_gold_target_table_fqn(environment, "player_performance_features"),
        ),
        player_development_features_rows=_collect_table_rows(
            spark,
            get_gold_target_table_fqn(environment, "player_development_features"),
        ),
        entity_data_quality_confidence_rows=_collect_table_rows(
            spark,
            get_gold_target_table_fqn(environment, "entity_data_quality_confidence"),
        ),
        analysis_as_of_date=analysis_as_of_date,
        scoring_scenario=scoring_scenario,
        scorecards_config=scorecards_config,
        eligibility_config=eligibility_config,
    )
    scorecards_summary = publish_player_evaluation_scorecards(
        spark,
        environment,
        rows=scorecard_rows,
    )
    ranking_rows = build_national_player_rankings(
        player_scorecard_rows=scorecard_rows,
        scoring_scenario=scoring_scenario,
        eligibility_config=eligibility_config,
    )
    rankings_summary = publish_national_player_rankings(
        spark,
        environment,
        rows=ranking_rows,
    )
    return Phase10PublicationSummary(
        player_evaluation_scorecards=scorecards_summary,
        national_player_rankings=rankings_summary,
    )


def build_player_evaluation_scorecards(
    *,
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    player_current_ratings_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    player_performance_features_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    player_development_features_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    entity_data_quality_confidence_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    analysis_as_of_date: date,
    scoring_scenario: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Build one player evaluation scorecard row per eligible player."""
    countries = {str(country).upper() for country in eligibility_config["countries"]}
    require_active_player = bool(eligibility_config.get("require_active_player", True))
    player_weights = {
        str(component): float(weight)
        for component, weight in scorecards_config["player_weights"].items()
    }
    development_weights = {
        str(component): float(weight)
        for component, weight in scorecards_config["development_weights"].items()
    }

    players_by_id = {
        _normalize_required_string(row.get("player_id")): row
        for row in players_rows
        if _normalize_optional_string(row.get("player_id")) is not None
    }
    current_by_id = {
        _normalize_required_string(row.get("player_id")): row
        for row in player_current_ratings_rows
        if _normalize_optional_string(row.get("player_id")) is not None
    }
    performance_by_key = {
        (
            _normalize_required_string(row.get("player_id")),
            _normalize_required_string(row.get("evidence_window")),
        ): row
        for row in player_performance_features_rows
        if _normalize_optional_string(row.get("player_id")) is not None
        and _normalize_optional_string(row.get("evidence_window")) is not None
    }
    development_by_id = {
        _normalize_required_string(row.get("player_id")): row
        for row in player_development_features_rows
        if _normalize_optional_string(row.get("player_id")) is not None
    }
    confidence_by_player_id = {
        _normalize_required_string(row.get("entity_id")): row
        for row in entity_data_quality_confidence_rows
        if _normalize_optional_string(row.get("entity_id")) is not None
        and _normalize_required_string(row.get("entity_type")) == "PLAYER"
    }

    base_rows: list[dict[str, Any]] = []
    for player_id, player_row in sorted(players_by_id.items()):
        country_code = (_normalize_optional_string(player_row.get("country_code")) or "").upper()
        active_flag = _coerce_bool(player_row.get("active_flag"))
        if country_code not in countries:
            continue
        if require_active_player and not active_flag:
            continue

        current_row = current_by_id.get(player_id, {})
        performance_career = performance_by_key.get((player_id, "career"), {})
        performance_recent = performance_by_key.get((player_id, "trailing_90"), {})
        development_row = development_by_id.get(player_id, {})
        confidence_row = confidence_by_player_id.get(player_id, {})

        raw_game_performance = _weighted_average(
            (
                (_coerce_float(performance_career.get("game_win_pct")), 0.6),
                (_coerce_float(performance_career.get("avg_point_share")), 0.4),
            )
        )
        raw_recent_form = _first_non_null(
            _coerce_float(performance_recent.get("win_pct")),
            _coerce_float(performance_recent.get("recency_weighted_win_pct")),
            _coerce_float(performance_career.get("recency_weighted_win_pct")),
        )

        base_rows.append(
            {
                "player_id": player_id,
                "scoring_scenario": scoring_scenario,
                "analysis_as_of_date": analysis_as_of_date,
                "display_name": _normalize_optional_string(player_row.get("display_name")),
                "country_code": country_code,
                "gender_code": _normalize_optional_string(player_row.get("gender")),
                "active_flag": active_flag,
                "eligible_player_flag": True,
                "source_rating_value": _coerce_float(current_row.get("source_rating_value")),
                "source_confidence_score": _coerce_float(current_row.get("source_confidence_score")),
                "analytical_rating_value": _coerce_float(current_row.get("analytical_rating_value")),
                "rated_match_count_current": _coerce_int(current_row.get("rated_match_count")) or 0,
                "rating_reliability_score": _coerce_float(current_row.get("rating_reliability_score")),
                "rating_evidence_band": _normalize_optional_string(current_row.get("rating_evidence_band")),
                "rating_uncertainty_proxy": _coerce_float(current_row.get("rating_uncertainty_proxy")),
                "career_match_count": _coerce_int(performance_career.get("match_count")) or 0,
                "recent_match_count": _coerce_int(performance_recent.get("match_count")) or 0,
                "performance_above_expectation_raw": _coerce_float(
                    performance_career.get("performance_above_expectation")
                ),
                "game_performance_raw": raw_game_performance,
                "recent_form_raw": raw_recent_form,
                "consistency_raw": _coerce_float(performance_career.get("consistency_score")),
                "strength_of_schedule_raw": _coerce_float(performance_career.get("strength_of_schedule")),
                "development_momentum_raw": _coerce_float(
                    development_row.get("development_momentum_score")
                ),
                "latest_assessment_confidence": _coerce_float(
                    development_row.get("latest_assessment_confidence")
                ),
                "development_feature_evidence_status": _normalize_optional_string(
                    development_row.get("feature_evidence_status")
                ),
                "combined_confidence_score": _coerce_float(
                    confidence_row.get("data_quality_confidence_score")
                ),
                "quality_confidence_band": _normalize_optional_string(
                    confidence_row.get("quality_confidence_band")
                ),
                "material_limitation_text": _normalize_optional_string(
                    confidence_row.get("material_limitation_text")
                ),
            }
        )

    _apply_country_percentiles(base_rows, "country_code", "analytical_rating_value", "rating_strength_score")
    _apply_country_percentiles(
        base_rows,
        "country_code",
        "performance_above_expectation_raw",
        "adjusted_performance_score",
    )
    _apply_country_percentiles(base_rows, "country_code", "game_performance_raw", "game_performance_score")
    _apply_country_percentiles(base_rows, "country_code", "recent_form_raw", "recent_form_score")
    _apply_country_percentiles(base_rows, "country_code", "consistency_raw", "consistency_score")
    _apply_country_percentiles(
        base_rows,
        "country_code",
        "strength_of_schedule_raw",
        "strength_of_schedule_score",
    )
    _apply_country_percentiles(
        base_rows,
        "country_code",
        "development_momentum_raw",
        "development_trend_component_score",
    )
    _apply_country_percentiles(
        base_rows,
        "country_code",
        "rating_uncertainty_proxy",
        "development_headroom_component_score",
    )

    final_rows: list[dict[str, Any]] = []
    for row in base_rows:
        performance_component_score = _weighted_average(
            (
                (_coerce_float(row.get("adjusted_performance_score")), 0.35),
                (_coerce_float(row.get("game_performance_score")), 0.25),
                (_coerce_float(row.get("recent_form_score")), 0.25),
                (_coerce_float(row.get("strength_of_schedule_score")), 0.15),
            )
        )
        rating_component_score = _coerce_float(row.get("rating_strength_score"))
        consistency_component_score = _coerce_float(row.get("consistency_score"))
        development_component_score = _coerce_float(row.get("development_trend_component_score"))
        confidence_component_score = _coerce_float(row.get("combined_confidence_score"))
        raw_player_evaluation_score = _reweighted_component_score(
            {
                "performance": performance_component_score,
                "rating": rating_component_score,
                "consistency": consistency_component_score,
                "development": development_component_score,
                "confidence": confidence_component_score,
            },
            player_weights,
        )
        combined_confidence_score = _coerce_float(row.get("combined_confidence_score")) or 0.0
        confidence_factor = round(0.5 + (0.5 * combined_confidence_score / 100.0), 6)
        confidence_adjusted_player_score = (
            None
            if raw_player_evaluation_score is None
            else round(raw_player_evaluation_score * confidence_factor, 4)
        )

        development_confidence_component_score = confidence_component_score
        development_potential_score = _reweighted_component_score(
            {
                "trend": development_component_score,
                "headroom": _coerce_float(row.get("development_headroom_component_score")),
                "confidence": development_confidence_component_score,
            },
            development_weights,
        )

        component_scores = {
            "rating_strength": rating_component_score,
            "performance": performance_component_score,
            "consistency": consistency_component_score,
            "development": development_component_score,
            "confidence": confidence_component_score,
        }
        top_strengths = ",".join(
            name
            for name, _value in sorted(
                [
                    (name, value)
                    for name, value in component_scores.items()
                    if value is not None
                ],
                key=lambda item: (-float(item[1]), item[0]),
            )[:3]
        )
        risk_codes: list[str] = []
        for name, value in component_scores.items():
            if value is not None and value < 40.0:
                risk_codes.append(name)
        if row.get("material_limitation_text"):
            risk_codes.append("quality_limitations")
        if row.get("development_feature_evidence_status") in {"NONE", "LIMITED"}:
            risk_codes.append("limited_development_evidence")
        top_risks = ",".join(risk_codes[:3]) or None
        evidence_band = _normalize_optional_string(row.get("rating_evidence_band")) or _normalize_optional_string(
            row.get("quality_confidence_band")
        )
        ranking_rationale = (
            f"Current={confidence_adjusted_player_score if confidence_adjusted_player_score is not None else 'NA'}; "
            f"Development={development_potential_score if development_potential_score is not None else 'NA'}; "
            f"Confidence={round(combined_confidence_score, 4)}"
        )

        final_row = dict(row)
        final_row.update(
            {
                "performance_component_score": performance_component_score,
                "rating_component_score": rating_component_score,
                "consistency_component_score": consistency_component_score,
                "development_component_score": development_component_score,
                "confidence_component_score": confidence_component_score,
                "raw_player_evaluation_score": raw_player_evaluation_score,
                "confidence_factor": confidence_factor,
                "confidence_adjusted_player_score": confidence_adjusted_player_score,
                "development_confidence_component_score": development_confidence_component_score,
                "development_potential_score": development_potential_score,
                "development_candidate_flag": bool(
                    development_potential_score is not None and development_potential_score >= 75.0
                ),
                "top_strengths": top_strengths or None,
                "top_risks": top_risks,
                "evidence_band": evidence_band,
                "ranking_rationale": ranking_rationale,
            }
        )
        final_rows.append(final_row)

    return tuple(final_rows)


def build_national_player_rankings(
    *,
    player_scorecard_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    scoring_scenario: str,
    eligibility_config: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Build country and gender player rankings from published scorecards."""
    countries = {str(country).upper() for country in eligibility_config["countries"]}
    ranking_rows: list[dict[str, Any]] = []

    for country_code in sorted(countries):
        country_rows = [
            row for row in player_scorecard_rows if str(row.get("country_code") or "").upper() == country_code
        ]
        ranking_rows.extend(
            _build_rank_group_rows(
                country_rows,
                ranking_group="OVERALL_CURRENT",
                score_field="confidence_adjusted_player_score",
                scoring_scenario=scoring_scenario,
            )
        )
        ranking_rows.extend(
            _build_rank_group_rows(
                country_rows,
                ranking_group="OVERALL_DEVELOPMENT",
                score_field="development_potential_score",
                scoring_scenario=scoring_scenario,
            )
        )
        for gender_code in sorted(
            {
                _normalize_optional_string(row.get("gender_code"))
                for row in country_rows
                if _normalize_optional_string(row.get("gender_code")) is not None
            }
        ):
            gender_rows = [row for row in country_rows if _normalize_optional_string(row.get("gender_code")) == gender_code]
            ranking_rows.extend(
                _build_rank_group_rows(
                    gender_rows,
                    ranking_group=f"GENDER_CURRENT_{gender_code}",
                    score_field="confidence_adjusted_player_score",
                    scoring_scenario=scoring_scenario,
                )
            )
            ranking_rows.extend(
                _build_rank_group_rows(
                    gender_rows,
                    ranking_group=f"GENDER_DEVELOPMENT_{gender_code}",
                    score_field="development_potential_score",
                    scoring_scenario=scoring_scenario,
                )
            )

    return tuple(ranking_rows)


def publish_player_evaluation_scorecards(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> PlayerEvaluationScorecardsPublicationSummary:
    """Publish player_evaluation_scorecards from Python-built records."""
    target_table_fqn = get_gold_target_table_fqn(environment, "player_evaluation_scorecards")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "player_evaluation_scorecards")
    publish_stage_records_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        records=rows,
        schema=PLAYER_EVALUATION_SCORECARDS_SCHEMA,
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("player_id", "scoring_scenario"),
            label="player_evaluation_scorecards",
        ),
    )
    output_row_count = int(spark.table(target_table_fqn).count())
    return PlayerEvaluationScorecardsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=len(rows),
        output_row_count=output_row_count,
    )


def publish_national_player_rankings(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> NationalPlayerRankingsPublicationSummary:
    """Publish national_player_rankings from Python-built records."""
    target_table_fqn = get_gold_target_table_fqn(environment, "national_player_rankings")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "national_player_rankings")
    publish_stage_records_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        records=rows,
        schema=NATIONAL_PLAYER_RANKINGS_SCHEMA,
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("country_code", "ranking_group", "player_id", "scoring_scenario"),
            label="national_player_rankings",
        ),
    )
    output_row_count = int(spark.table(target_table_fqn).count())
    return NationalPlayerRankingsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=len(rows),
        output_row_count=output_row_count,
    )


def _build_rank_group_rows(
    rows: list[dict[str, Any]],
    *,
    ranking_group: str,
    score_field: str,
    scoring_scenario: str,
) -> list[dict[str, Any]]:
    eligible_rows = [row for row in rows if _coerce_float(row.get(score_field)) is not None]
    ordered = sorted(
        eligible_rows,
        key=lambda row: (
            -float(_coerce_float(row.get(score_field)) or 0.0),
            -float(_coerce_float(row.get("analytical_rating_value")) or 0.0),
            -(int(_coerce_int(row.get("rated_match_count_current")) or 0)),
            str(row.get("player_id") or ""),
        ),
    )

    ranked_rows: list[dict[str, Any]] = []
    previous_score = None
    dense_rank = 0
    for index, row in enumerate(ordered, start=1):
        current_score = float(_coerce_float(row.get(score_field)) or 0.0)
        if previous_score != current_score:
            dense_rank += 1
        rank = index
        next_score = (
            float(_coerce_float(ordered[index].get(score_field)) or 0.0)
            if index < len(ordered)
            else None
        )
        ranked_rows.append(
            {
                "country_code": row.get("country_code"),
                "ranking_group": ranking_group,
                "scoring_scenario": scoring_scenario,
                "player_id": row.get("player_id"),
                "display_name": row.get("display_name"),
                "gender_code": row.get("gender_code"),
                "active_flag": row.get("active_flag"),
                "rank_metric_name": "current_strength"
                if "CURRENT" in ranking_group
                else "development_potential",
                "rank_metric_value": current_score,
                "rank": rank,
                "dense_rank": dense_rank,
                "score_difference_from_next": None if next_score is None else round(current_score - next_score, 4),
                "top_25_flag": rank <= 25,
                "confidence_adjusted_player_score": row.get("confidence_adjusted_player_score"),
                "development_potential_score": row.get("development_potential_score"),
                "analytical_rating_value": row.get("analytical_rating_value"),
                "rated_match_count_current": row.get("rated_match_count_current"),
                "combined_confidence_score": row.get("combined_confidence_score"),
                "top_strengths": row.get("top_strengths"),
                "top_risks": row.get("top_risks"),
                "evidence_band": row.get("evidence_band"),
                "ranking_rationale": row.get("ranking_rationale"),
            }
        )
        previous_score = current_score
    return ranked_rows


def _apply_country_percentiles(
    rows: list[dict[str, Any]],
    group_field: str,
    raw_field: str,
    output_field: str,
) -> None:
    grouped_values: dict[str, list[float]] = {}
    for row in rows:
        group_value = str(row.get(group_field) or "")
        raw_value = _coerce_float(row.get(raw_field))
        if not group_value or raw_value is None:
            continue
        grouped_values.setdefault(group_value, []).append(raw_value)

    percentile_maps = {
        group_value: _percentile_map(values)
        for group_value, values in grouped_values.items()
    }
    for row in rows:
        group_value = str(row.get(group_field) or "")
        raw_value = _coerce_float(row.get(raw_field))
        if not group_value or raw_value is None:
            row[output_field] = None
            continue
        row[output_field] = percentile_maps[group_value].get(raw_value)


def _percentile_map(values: list[float]) -> dict[float, float]:
    unique_values = sorted(set(values))
    if len(unique_values) == 1:
        return {unique_values[0]: 100.0}
    mapping: dict[float, float] = {}
    for index, value in enumerate(unique_values):
        mapping[value] = round((index / (len(unique_values) - 1)) * 100.0, 4)
    return mapping


def _reweighted_component_score(
    components: dict[str, float | None],
    weights: dict[str, float],
) -> float | None:
    available = [
        (float(value), float(weights[name]))
        for name, value in components.items()
        if value is not None and name in weights
    ]
    total_weight = sum(weight for _value, weight in available)
    if total_weight == 0.0:
        return None
    return round(sum(value * weight for value, weight in available) / total_weight, 4)


def _weighted_average(items: tuple[tuple[float | None, float], ...]) -> float | None:
    available = [(float(value), float(weight)) for value, weight in items if value is not None]
    total_weight = sum(weight for _value, weight in available)
    if total_weight == 0.0:
        return None
    return round(sum(value * weight for value, weight in available) / total_weight, 4)


def _first_non_null(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None


def _collect_table_rows(spark: Any, table_fqn: str) -> list[dict[str, Any]]:
    return [
        row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        for row in spark.table(table_fqn).toLocalIterator()
    ]


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
    COALESCE(SUM(CASE WHEN {null_conditions} THEN 1 ELSE 0 END), 0) AS null_key_count,
    COALESCE(SUM(CASE WHEN duplicate_key_count > 1 THEN 1 ELSE 0 END), 0) AS duplicate_group_count
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
            f"{label} validation failed for {table_fqn}: "
            f"null_key_count={int(mapping['null_key_count'] or 0)}, "
            f"duplicate_group_count={int(mapping['duplicate_group_count'] or 0)}."
        )


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
