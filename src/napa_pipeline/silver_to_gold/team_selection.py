"""Phase 11 team selection scorecard builders for the Silver-to-Gold pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_stage_table_fqn,
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.publish import (
    publish_sql_table,
    publish_stage_records_to_gold_table,
    publish_stage_to_gold_table,
)


ELIGIBLE_STATUS = "ELIGIBLE"
INELIGIBLE_STATUS = "INELIGIBLE"
REVIEW_REQUIRED_STATUS = "REVIEW_REQUIRED"

SUFFICIENT_EVIDENCE = "SUFFICIENT"
LIMITED_EVIDENCE = "LIMITED"
NONE_EVIDENCE = "NONE"


@dataclass(frozen=True)
class TeamSelectionScorecardsPublicationSummary:
    """Published-table summary for team_selection_scorecards."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class OlympicTeamCandidatesPublicationSummary:
    """Published-table summary for olympic_team_candidates."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class Phase11PublicationSummary:
    """Published-table summary for the two Phase 11 target tables."""

    team_selection_scorecards: TeamSelectionScorecardsPublicationSummary
    olympic_team_candidates: OlympicTeamCandidatesPublicationSummary


def publish_phase11_team_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
) -> Phase11PublicationSummary:
    """Publish the Gold Phase 11 team scorecard and candidate tables."""
    scorecards_summary = publish_team_selection_scorecards_from_sql(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        scoring_scenario=scoring_scenario,
        scorecards_config=scorecards_config,
        eligibility_config=eligibility_config,
    )
    candidates_summary = publish_olympic_team_candidates_from_sql(
        spark,
        environment,
        scoring_scenario=scoring_scenario,
        eligibility_config=eligibility_config,
    )
    return Phase11PublicationSummary(
        team_selection_scorecards=scorecards_summary,
        olympic_team_candidates=candidates_summary,
    )


def build_team_selection_scorecards(
    *,
    teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    team_performance_features_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    partnership_effectiveness_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    player_scorecard_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    entity_data_quality_confidence_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    resolved_match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_outcome_predictions_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    analysis_as_of_date: date,
    scoring_scenario: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Build one team selection scorecard row per relevant team and scenario."""
    countries = {str(country).upper() for country in eligibility_config["countries"]}
    categories = {str(category).upper() for category in eligibility_config["categories"]}
    require_active_team = bool(eligibility_config.get("require_active_team", True))
    team_weights = {
        str(component): float(weight)
        for component, weight in scorecards_config["team_weights"].items()
    }

    current_members_by_team = _current_members_by_team(
        team_memberships_rows,
        analysis_as_of_date=analysis_as_of_date,
    )
    overlap_by_team = _membership_overlap_by_team(
        team_memberships_rows,
        analysis_as_of_date=analysis_as_of_date,
    )
    player_scorecards_by_id = {
        _normalize_required_string(row.get("player_id")): row
        for row in player_scorecard_rows
        if _normalize_optional_string(row.get("player_id")) is not None
        and _normalize_required_string(row.get("scoring_scenario")) == scoring_scenario
    }
    team_perf_career_by_id = {
        _normalize_required_string(row.get("team_id")): row
        for row in team_performance_features_rows
        if _normalize_optional_string(row.get("team_id")) is not None
        and _normalize_required_string(row.get("evidence_window")) == "career"
    }
    partnership_by_team_id = {
        _normalize_required_string(row.get("team_id")): row
        for row in partnership_effectiveness_rows
        if _normalize_optional_string(row.get("team_id")) is not None
    }
    team_quality_by_id = {
        _normalize_required_string(row.get("entity_id")): row
        for row in entity_data_quality_confidence_rows
        if _normalize_optional_string(row.get("entity_id")) is not None
        and _normalize_required_string(row.get("entity_type")) == "TEAM"
    }
    team_resolution_confidence_by_id = _team_resolution_confidence_by_id(
        resolved_match_teams_rows,
        analysis_as_of_date=analysis_as_of_date,
    )
    prediction_strength_by_id = _prediction_strength_by_team_id(
        match_outcome_predictions_rows,
        analysis_as_of_date=analysis_as_of_date,
    )

    base_rows: list[dict[str, Any]] = []
    for team_row in teams_rows:
        team_id = _normalize_optional_string(team_row.get("team_id"))
        if team_id is None:
            continue
        country_code = (_normalize_optional_string(team_row.get("country_code")) or "").upper()
        category_code = (_normalize_optional_string(team_row.get("team_category")) or "").upper()
        if country_code not in countries or category_code not in categories:
            continue

        active_flag = _team_is_active_as_of_date(
            team_row,
            analysis_as_of_date=analysis_as_of_date,
        )
        if require_active_team and not active_flag:
            continue
        team_status = _normalize_optional_string(team_row.get("team_status"))
        current_member_ids = current_members_by_team.get(team_id, ())
        current_member_count = len(current_member_ids)
        player_one_id = current_member_ids[0] if current_member_count >= 1 else None
        player_two_id = current_member_ids[1] if current_member_count >= 2 else None
        player_one_row = player_scorecards_by_id.get(player_one_id or "", {})
        player_two_row = player_scorecards_by_id.get(player_two_id or "", {})
        player_one_score = _coerce_float(player_one_row.get("confidence_adjusted_player_score"))
        player_two_score = _coerce_float(player_two_row.get("confidence_adjusted_player_score"))
        player_one_confidence = _coerce_float(player_one_row.get("combined_confidence_score"))
        player_two_confidence = _coerce_float(player_two_row.get("combined_confidence_score"))

        team_perf_row = team_perf_career_by_id.get(team_id, {})
        partnership_row = partnership_by_team_id.get(team_id, {})
        team_quality_row = team_quality_by_id.get(team_id, {})
        team_resolution_confidence = team_resolution_confidence_by_id.get(team_id)
        prediction_strength_raw = prediction_strength_by_id.get(team_id)

        partnership_strength_raw = _weighted_average(
            (
                (_scale_unit_interval(partnership_row.get("team_adjusted_win_rate")), 0.5),
                (_scale_signed_signal(partnership_row.get("synergy_proxy")), 0.3),
                (_scale_days(partnership_row.get("partnership_duration_days")), 0.2),
            )
        )
        average_player_score = _weighted_average(
            (
                (player_one_score, 1.0),
                (player_two_score, 1.0),
            )
        )
        minimum_player_score = _minimum_non_null(player_one_score, player_two_score)
        player_score_balance = _score_balance(player_one_score, player_two_score)
        player_confidence_raw = _weighted_average(
            (
                (player_one_confidence, 1.0),
                (player_two_confidence, 1.0),
            )
        )
        team_feature_confidence_raw = _coerce_float(
            team_perf_row.get("evidence_reliability_score")
        )
        data_quality_confidence_raw = _coerce_float(
            team_quality_row.get("data_quality_confidence_score")
        )

        evidence_sufficiency_status = _derive_evidence_sufficiency_status(
            team_perf_row,
            partnership_row,
            player_one_row,
            player_two_row,
        )

        candidate_attribution_allowed_flag = (
            _coerce_bool(team_perf_row.get("candidate_attribution_allowed_flag"))
            and _coerce_bool(partnership_row.get("candidate_attribution_allowed_flag"))
            and current_member_count == 2
            and not overlap_by_team.get(team_id, False)
        )

        eligibility_reason_codes = _derive_eligibility_reason_codes(
            team_row=team_row,
            team_id=team_id,
            current_member_count=current_member_count,
            player_one_id=player_one_id,
            player_two_id=player_two_id,
            player_one_row=player_one_row,
            player_two_row=player_two_row,
            candidate_attribution_allowed_flag=candidate_attribution_allowed_flag,
            evidence_sufficiency_status=evidence_sufficiency_status,
            require_active_team=require_active_team,
            analysis_as_of_date=analysis_as_of_date,
            overlap_warning_flag=overlap_by_team.get(team_id, False),
            active_as_of_date=active_flag,
        )
        eligibility_status = _derive_eligibility_status(
            reason_codes=eligibility_reason_codes,
            evidence_sufficiency_status=evidence_sufficiency_status,
        )

        base_rows.append(
            {
                "team_id": team_id,
                "scoring_scenario": scoring_scenario,
                "analysis_as_of_date": analysis_as_of_date,
                "team_category": category_code,
                "country_code": country_code,
                "team_status": team_status,
                "active_flag": active_flag,
                "formation_date": team_row.get("formation_date"),
                "dissolution_date": team_row.get("dissolution_date"),
                "player_one_id": player_one_id,
                "player_two_id": player_two_id,
                "player_one_display_name": _normalize_optional_string(
                    player_one_row.get("display_name")
                ),
                "player_two_display_name": _normalize_optional_string(
                    player_two_row.get("display_name")
                ),
                "current_member_count": current_member_count,
                "membership_overlap_warning_flag": overlap_by_team.get(team_id, False),
                "eligible_team_flag": not eligibility_reason_codes,
                "eligibility_status": eligibility_status,
                "eligibility_reason_codes": ",".join(eligibility_reason_codes) or None,
                "evidence_sufficiency_status": evidence_sufficiency_status,
                "candidate_attribution_allowed_flag": candidate_attribution_allowed_flag,
                "partnership_key": _normalize_optional_string(partnership_row.get("partnership_key")),
                "player_one_score": player_one_score,
                "player_two_score": player_two_score,
                "average_player_score": average_player_score,
                "minimum_player_score": minimum_player_score,
                "player_score_balance": player_score_balance,
                "partnership_strength_raw": partnership_strength_raw,
                "prediction_strength_raw": prediction_strength_raw,
                "team_feature_confidence_raw": team_feature_confidence_raw,
                "player_confidence_raw": player_confidence_raw,
                "data_quality_confidence_raw": data_quality_confidence_raw,
                "team_resolution_confidence_raw": team_resolution_confidence,
                "material_limitation_text": _normalize_optional_string(
                    team_quality_row.get("material_limitation_text")
                ),
            }
        )

    _apply_group_percentiles(
        base_rows,
        group_fields=("country_code", "team_category"),
        raw_field="partnership_strength_raw",
        output_field="partnership_score",
    )
    _apply_group_percentiles(
        base_rows,
        group_fields=("country_code", "team_category"),
        raw_field="average_player_score",
        output_field="player_strength_score",
    )
    _apply_group_percentiles(
        base_rows,
        group_fields=("country_code", "team_category"),
        raw_field="prediction_strength_raw",
        output_field="prediction_score",
    )

    final_rows: list[dict[str, Any]] = []
    for row in base_rows:
        combined_team_confidence = _weighted_average(
            (
                (_coerce_float(row.get("team_feature_confidence_raw")), 0.30),
                (_coerce_float(row.get("player_confidence_raw")), 0.25),
                (_coerce_float(row.get("data_quality_confidence_raw")), 0.25),
                (_coerce_float(row.get("team_resolution_confidence_raw")), 0.20),
            )
        )
        confidence_component_score = combined_team_confidence
        raw_team_selection_score = _reweighted_component_score(
            {
                "partnership": _coerce_float(row.get("partnership_score")),
                "player_strength": _coerce_float(row.get("player_strength_score")),
                "prediction": _coerce_float(row.get("prediction_score")),
                "confidence": confidence_component_score,
            },
            team_weights,
        )
        confidence_factor = (
            None
            if combined_team_confidence is None
            else round(0.5 + (0.5 * combined_team_confidence / 100.0), 6)
        )
        confidence_adjusted_team_score = (
            None
            if raw_team_selection_score is None or confidence_factor is None
            else round(raw_team_selection_score * confidence_factor, 4)
        )
        risk_penalty_score = _risk_penalty_score(row, combined_team_confidence)
        final_team_selection_score = (
            None
            if confidence_adjusted_team_score is None
            else round(confidence_adjusted_team_score - risk_penalty_score, 4)
        )
        top_strengths = _top_strengths(
            {
                "partnership": _coerce_float(row.get("partnership_score")),
                "player_strength": _coerce_float(row.get("player_strength_score")),
                "prediction": _coerce_float(row.get("prediction_score")),
                "confidence": confidence_component_score,
            }
        )
        top_risks = _top_risks(row, combined_team_confidence)
        ranking_rationale = (
            f"Final={final_team_selection_score if final_team_selection_score is not None else 'NA'}; "
            f"Confidence={round(combined_team_confidence or 0.0, 4)}; "
            f"Eligibility={row['eligibility_status']}"
        )

        final_row = dict(row)
        final_row.update(
            {
                "confidence_component_score": confidence_component_score,
                "combined_team_confidence": combined_team_confidence,
                "raw_team_selection_score": raw_team_selection_score,
                "confidence_factor": confidence_factor,
                "confidence_adjusted_team_score": confidence_adjusted_team_score,
                "risk_penalty_score": risk_penalty_score,
                "final_team_selection_score": final_team_selection_score,
                "top_strengths": top_strengths,
                "top_risks": top_risks,
                "ranking_rationale": ranking_rationale,
            }
        )
        final_rows.append(final_row)

    return tuple(final_rows)


def build_team_selection_scorecards_sql(
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
) -> str:
    """Return the Spark SQL used to build team_selection_scorecards."""
    analysis_date_literal = analysis_as_of_date.isoformat()
    teams_fqn = get_silver_source_table_fqn(environment, "teams")
    memberships_fqn = get_silver_source_table_fqn(environment, "team_memberships")
    team_perf_fqn = get_gold_target_table_fqn(environment, "team_performance_features")
    partnership_fqn = get_gold_target_table_fqn(environment, "partnership_effectiveness")
    player_scorecards_fqn = get_gold_target_table_fqn(environment, "player_evaluation_scorecards")
    quality_fqn = get_gold_target_table_fqn(environment, "entity_data_quality_confidence")
    resolved_fqn = get_gold_target_table_fqn(environment, "resolved_match_teams")
    predictions_fqn = get_gold_target_table_fqn(environment, "match_outcome_predictions")

    countries = ", ".join(f"'{str(country).upper()}'" for country in eligibility_config["countries"])
    categories = ", ".join(f"'{str(category).upper()}'" for category in eligibility_config["categories"])
    require_active_team = "TRUE" if bool(eligibility_config.get("require_active_team", True)) else "FALSE"

    partnership_weight = float(scorecards_config["team_weights"]["partnership"])
    player_strength_weight = float(scorecards_config["team_weights"]["player_strength"])
    prediction_weight = float(scorecards_config["team_weights"]["prediction"])
    confidence_weight = float(scorecards_config["team_weights"]["confidence"])

    return f"""
WITH source_teams AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        UPPER(TRIM(CAST(team_category AS STRING))) AS team_category,
        UPPER(TRIM(CAST(country_code AS STRING))) AS country_code,
        UPPER(TRIM(CAST(team_status AS STRING))) AS team_status,
        CAST(active_flag AS BOOLEAN) AS active_flag_source,
        CAST(formation_date AS DATE) AS formation_date,
        CAST(dissolution_date AS DATE) AS dissolution_date,
        CASE
            WHEN CAST(formation_date AS DATE) IS NOT NULL
             AND CAST(formation_date AS DATE) > DATE('{analysis_date_literal}') THEN FALSE
            WHEN CAST(dissolution_date AS DATE) IS NOT NULL
             AND CAST(dissolution_date AS DATE) <= DATE('{analysis_date_literal}') THEN FALSE
            WHEN CAST(formation_date AS DATE) IS NULL
             AND CAST(dissolution_date AS DATE) IS NULL THEN COALESCE(CAST(active_flag AS BOOLEAN), FALSE)
            ELSE TRUE
        END AS active_flag
    FROM {teams_fqn}
    WHERE team_id IS NOT NULL
      AND UPPER(TRIM(CAST(country_code AS STRING))) IN ({countries})
      AND UPPER(TRIM(CAST(team_category AS STRING))) IN ({categories})
),
eligible_teams AS (
    SELECT *
    FROM source_teams
    WHERE NOT {require_active_team}
       OR active_flag
),
memberships_as_of AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        CAST(player_id AS STRING) AS player_id,
        COALESCE(CAST(membership_overlap_flag AS BOOLEAN), FALSE) AS membership_overlap_flag
    FROM {memberships_fqn}
    WHERE team_id IS NOT NULL
      AND player_id IS NOT NULL
      AND (
        CASE
            WHEN CAST(membership_start_date AS DATE) IS NOT NULL
             AND CAST(membership_start_date AS DATE) > DATE('{analysis_date_literal}') THEN FALSE
            WHEN CAST(membership_end_date AS DATE) IS NOT NULL
             AND CAST(membership_end_date AS DATE) < DATE('{analysis_date_literal}') THEN FALSE
            WHEN CAST(membership_start_date AS DATE) IS NULL
             AND CAST(membership_end_date AS DATE) IS NULL THEN COALESCE(CAST(current_membership_flag AS BOOLEAN), FALSE)
            ELSE TRUE
        END
      )
),
membership_rollup AS (
    SELECT
        team_id,
        sort_array(collect_set(player_id), true) AS player_ids,
        COUNT(DISTINCT player_id) AS current_member_count,
        MAX(CASE WHEN membership_overlap_flag THEN 1 ELSE 0 END) = 1 AS membership_overlap_warning_flag
    FROM memberships_as_of
    GROUP BY team_id
),
team_perf_career AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        COALESCE(CAST(candidate_attribution_allowed_flag AS BOOLEAN), FALSE) AS candidate_attribution_allowed_flag,
        CAST(evidence_reliability_score AS DOUBLE) AS evidence_reliability_score,
        CAST(feature_evidence_status AS STRING) AS feature_evidence_status
    FROM {team_perf_fqn}
    WHERE evidence_window = 'career'
),
partnership_by_team AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        CAST(partnership_key AS STRING) AS partnership_key,
        COALESCE(CAST(candidate_attribution_allowed_flag AS BOOLEAN), FALSE) AS candidate_attribution_allowed_flag,
        CAST(team_adjusted_win_rate AS DOUBLE) AS team_adjusted_win_rate,
        CAST(synergy_proxy AS DOUBLE) AS synergy_proxy,
        CAST(partnership_duration_days AS DOUBLE) AS partnership_duration_days,
        CAST(feature_evidence_status AS STRING) AS feature_evidence_status
    FROM {partnership_fqn}
    WHERE team_id IS NOT NULL
),
player_scorecards AS (
    SELECT
        CAST(player_id AS STRING) AS player_id,
        CAST(display_name AS STRING) AS display_name,
        CAST(confidence_adjusted_player_score AS DOUBLE) AS confidence_adjusted_player_score,
        CAST(combined_confidence_score AS DOUBLE) AS combined_confidence_score,
        CAST(evidence_band AS STRING) AS evidence_band
    FROM {player_scorecards_fqn}
    WHERE scoring_scenario = '{scoring_scenario}'
),
team_quality AS (
    SELECT
        CAST(entity_id AS STRING) AS team_id,
        CAST(data_quality_confidence_score AS DOUBLE) AS data_quality_confidence_score,
        CAST(material_limitation_text AS STRING) AS material_limitation_text
    FROM {quality_fqn}
    WHERE entity_type = 'TEAM'
),
team_resolution AS (
    SELECT
        CAST(resolved_team_id AS STRING) AS team_id,
        AVG(CAST(team_resolution_confidence AS DOUBLE)) AS team_resolution_confidence_raw
    FROM {resolved_fqn}
    WHERE resolved_team_id IS NOT NULL
      AND CAST(match_date AS DATE) IS NOT NULL
      AND CAST(match_date AS DATE) <= DATE('{analysis_date_literal}')
    GROUP BY CAST(resolved_team_id AS STRING)
),
prediction_inputs AS (
    SELECT
        CAST(team_a_team_id AS STRING) AS team_id,
        CAST(model_predicted_probability AS DOUBLE) * 100.0 AS probability_score
    FROM {predictions_fqn}
    WHERE CAST(match_date AS DATE) IS NULL OR CAST(match_date AS DATE) <= DATE('{analysis_date_literal}')
    UNION ALL
    SELECT
        CAST(team_b_team_id AS STRING) AS team_id,
        (1.0 - CAST(model_predicted_probability AS DOUBLE)) * 100.0 AS probability_score
    FROM {predictions_fqn}
    WHERE CAST(match_date AS DATE) IS NULL OR CAST(match_date AS DATE) <= DATE('{analysis_date_literal}')
),
prediction_strength AS (
    SELECT
        team_id,
        AVG(probability_score) AS prediction_strength_raw
    FROM prediction_inputs
    WHERE team_id IS NOT NULL
    GROUP BY team_id
),
base_rows AS (
    SELECT
        et.team_id,
        '{scoring_scenario}' AS scoring_scenario,
        DATE('{analysis_date_literal}') AS analysis_as_of_date,
        et.team_category,
        et.country_code,
        et.team_status,
        et.active_flag,
        et.formation_date,
        et.dissolution_date,
        get(mr.player_ids, 0) AS player_one_id,
        get(mr.player_ids, 1) AS player_two_id,
        ps1.display_name AS player_one_display_name,
        ps2.display_name AS player_two_display_name,
        COALESCE(mr.current_member_count, 0) AS current_member_count,
        COALESCE(mr.membership_overlap_warning_flag, FALSE) AS membership_overlap_warning_flag,
        pbt.partnership_key,
        ps1.confidence_adjusted_player_score AS player_one_score,
        ps2.confidence_adjusted_player_score AS player_two_score,
        CASE
            WHEN ps1.confidence_adjusted_player_score IS NOT NULL AND ps2.confidence_adjusted_player_score IS NOT NULL
                THEN (ps1.confidence_adjusted_player_score + ps2.confidence_adjusted_player_score) / 2.0
            WHEN ps1.confidence_adjusted_player_score IS NOT NULL THEN ps1.confidence_adjusted_player_score
            WHEN ps2.confidence_adjusted_player_score IS NOT NULL THEN ps2.confidence_adjusted_player_score
            ELSE NULL
        END AS average_player_score,
        CASE
            WHEN ps1.confidence_adjusted_player_score IS NOT NULL AND ps2.confidence_adjusted_player_score IS NOT NULL
                THEN LEAST(ps1.confidence_adjusted_player_score, ps2.confidence_adjusted_player_score)
            ELSE COALESCE(ps1.confidence_adjusted_player_score, ps2.confidence_adjusted_player_score)
        END AS minimum_player_score,
        CASE
            WHEN ps1.confidence_adjusted_player_score IS NOT NULL AND ps2.confidence_adjusted_player_score IS NOT NULL
                THEN GREATEST(0.0, 100.0 - ABS(ps1.confidence_adjusted_player_score - ps2.confidence_adjusted_player_score))
            ELSE NULL
        END AS player_score_balance,
        CASE
            WHEN pbt.team_adjusted_win_rate IS NULL AND pbt.synergy_proxy IS NULL AND pbt.partnership_duration_days IS NULL THEN NULL
            ELSE (
                COALESCE(GREATEST(0.0, LEAST(1.0, pbt.team_adjusted_win_rate)) * 100.0, 0.0) * 0.5
                + COALESCE(GREATEST(0.0, LEAST(100.0, 50.0 + (pbt.synergy_proxy * 100.0))), 0.0) * 0.3
                + COALESCE(GREATEST(0.0, LEAST(1.0, pbt.partnership_duration_days / 365.0)) * 100.0, 0.0) * 0.2
            )
        END AS partnership_strength_raw,
        pr.prediction_strength_raw,
        tpc.evidence_reliability_score AS team_feature_confidence_raw,
        CASE
            WHEN ps1.combined_confidence_score IS NOT NULL AND ps2.combined_confidence_score IS NOT NULL
                THEN (ps1.combined_confidence_score + ps2.combined_confidence_score) / 2.0
            WHEN ps1.combined_confidence_score IS NOT NULL THEN ps1.combined_confidence_score
            WHEN ps2.combined_confidence_score IS NOT NULL THEN ps2.combined_confidence_score
            ELSE NULL
        END AS player_confidence_raw,
        tq.data_quality_confidence_score AS data_quality_confidence_raw,
        tr.team_resolution_confidence_raw,
        tq.material_limitation_text,
        CASE
            WHEN tpc.team_id IS NULL OR pbt.team_id IS NULL THEN '{NONE_EVIDENCE}'
            WHEN tpc.feature_evidence_status IS NULL OR pbt.feature_evidence_status IS NULL THEN '{NONE_EVIDENCE}'
            WHEN tpc.feature_evidence_status = '{NONE_EVIDENCE}' OR pbt.feature_evidence_status = '{NONE_EVIDENCE}' THEN '{NONE_EVIDENCE}'
            WHEN tpc.feature_evidence_status = '{LIMITED_EVIDENCE}' OR pbt.feature_evidence_status = '{LIMITED_EVIDENCE}'
                OR COALESCE(ps1.evidence_band, 'LOW') IN ('LOW', 'MODERATE')
                OR COALESCE(ps2.evidence_band, 'LOW') IN ('LOW', 'MODERATE')
                THEN '{LIMITED_EVIDENCE}'
            ELSE '{SUFFICIENT_EVIDENCE}'
        END AS evidence_sufficiency_status,
        (
            COALESCE(tpc.candidate_attribution_allowed_flag, FALSE)
            AND COALESCE(pbt.candidate_attribution_allowed_flag, FALSE)
            AND COALESCE(mr.current_member_count, 0) = 2
            AND NOT COALESCE(mr.membership_overlap_warning_flag, FALSE)
        ) AS candidate_attribution_allowed_flag
    FROM eligible_teams AS et
    LEFT JOIN membership_rollup AS mr
      ON mr.team_id = et.team_id
    LEFT JOIN team_perf_career AS tpc
      ON tpc.team_id = et.team_id
    LEFT JOIN partnership_by_team AS pbt
      ON pbt.team_id = et.team_id
    LEFT JOIN player_scorecards AS ps1
      ON ps1.player_id = get(mr.player_ids, 0)
    LEFT JOIN player_scorecards AS ps2
      ON ps2.player_id = get(mr.player_ids, 1)
    LEFT JOIN team_quality AS tq
      ON tq.team_id = et.team_id
    LEFT JOIN team_resolution AS tr
      ON tr.team_id = et.team_id
    LEFT JOIN prediction_strength AS pr
      ON pr.team_id = et.team_id
),
scored_rows AS (
    SELECT
        base.*,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY country_code, team_category) = 1 THEN 100.0
            WHEN partnership_strength_raw IS NULL THEN NULL
            ELSE ROUND(PERCENT_RANK() OVER (
                PARTITION BY country_code, team_category
                ORDER BY partnership_strength_raw
            ) * 100.0, 4)
        END AS partnership_score,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY country_code, team_category) = 1 THEN 100.0
            WHEN average_player_score IS NULL THEN NULL
            ELSE ROUND(PERCENT_RANK() OVER (
                PARTITION BY country_code, team_category
                ORDER BY average_player_score
            ) * 100.0, 4)
        END AS player_strength_score,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY country_code, team_category) = 1 THEN 100.0
            WHEN prediction_strength_raw IS NULL THEN NULL
            ELSE ROUND(PERCENT_RANK() OVER (
                PARTITION BY country_code, team_category
                ORDER BY prediction_strength_raw
            ) * 100.0, 4)
        END AS prediction_score
    FROM base_rows AS base
),
eligibility_rows AS (
    SELECT
        scored.*,
        CASE
            WHEN team_feature_confidence_raw IS NOT NULL
             AND player_confidence_raw IS NOT NULL
             AND data_quality_confidence_raw IS NOT NULL
             AND team_resolution_confidence_raw IS NOT NULL
                THEN ROUND(
                    (team_feature_confidence_raw * 0.30)
                    + (player_confidence_raw * 0.25)
                    + (data_quality_confidence_raw * 0.25)
                    + (team_resolution_confidence_raw * 0.20),
                    4
                )
            WHEN team_feature_confidence_raw IS NOT NULL
              OR player_confidence_raw IS NOT NULL
              OR data_quality_confidence_raw IS NOT NULL
              OR team_resolution_confidence_raw IS NOT NULL
                THEN ROUND(
                    (
                        COALESCE(team_feature_confidence_raw, 0.0) * 0.30
                        + COALESCE(player_confidence_raw, 0.0) * 0.25
                        + COALESCE(data_quality_confidence_raw, 0.0) * 0.25
                        + COALESCE(team_resolution_confidence_raw, 0.0) * 0.20
                    )
                    / (
                        CASE WHEN team_feature_confidence_raw IS NOT NULL THEN 0.30 ELSE 0.0 END
                        + CASE WHEN player_confidence_raw IS NOT NULL THEN 0.25 ELSE 0.0 END
                        + CASE WHEN data_quality_confidence_raw IS NOT NULL THEN 0.25 ELSE 0.0 END
                        + CASE WHEN team_resolution_confidence_raw IS NOT NULL THEN 0.20 ELSE 0.0 END
                    ),
                    4
                )
            ELSE NULL
        END AS combined_team_confidence,
        CONCAT_WS(
            ',',
            CASE WHEN current_member_count <> 2 THEN 'INVALID_MEMBERSHIP_COUNT' END,
            CASE WHEN membership_overlap_warning_flag THEN 'AMBIGUOUS_TEAM_COMPOSITION' END,
            CASE WHEN player_one_id IS NULL OR player_two_id IS NULL THEN 'UNKNOWN_PLAYER' END,
            CASE WHEN player_one_id IS NOT NULL AND player_one_score IS NULL THEN 'UNKNOWN_PLAYER' END,
            CASE WHEN player_two_id IS NOT NULL AND player_two_score IS NULL THEN 'UNKNOWN_PLAYER' END,
            CASE WHEN evidence_sufficiency_status = '{NONE_EVIDENCE}' THEN 'NO_VALID_TEAM_ID' END
        ) AS eligibility_reason_codes
    FROM scored_rows AS scored
),
final_rows AS (
    SELECT
        team_id,
        scoring_scenario,
        analysis_as_of_date,
        team_category,
        country_code,
        team_status,
        active_flag,
        formation_date,
        dissolution_date,
        player_one_id,
        player_two_id,
        player_one_display_name,
        player_two_display_name,
        current_member_count,
        membership_overlap_warning_flag,
        CASE
            WHEN eligibility_reason_codes IS NULL OR eligibility_reason_codes = '' THEN TRUE
            ELSE FALSE
        END AS eligible_team_flag,
        CASE
            WHEN eligibility_reason_codes IS NOT NULL AND eligibility_reason_codes <> '' THEN '{INELIGIBLE_STATUS}'
            WHEN evidence_sufficiency_status = '{LIMITED_EVIDENCE}' THEN '{REVIEW_REQUIRED_STATUS}'
            ELSE '{ELIGIBLE_STATUS}'
        END AS eligibility_status,
        NULLIF(eligibility_reason_codes, '') AS eligibility_reason_codes,
        evidence_sufficiency_status,
        candidate_attribution_allowed_flag,
        partnership_key,
        player_one_score,
        player_two_score,
        average_player_score,
        minimum_player_score,
        player_score_balance,
        partnership_strength_raw,
        prediction_strength_raw,
        team_feature_confidence_raw,
        player_confidence_raw,
        data_quality_confidence_raw,
        team_resolution_confidence_raw,
        material_limitation_text,
        partnership_score,
        player_strength_score,
        prediction_score,
        combined_team_confidence AS confidence_component_score,
        combined_team_confidence,
        CASE
            WHEN partnership_score IS NOT NULL
              AND player_strength_score IS NOT NULL
              AND prediction_score IS NOT NULL
              AND combined_team_confidence IS NOT NULL
                THEN ROUND(
                    (
                        (partnership_score * {partnership_weight})
                        + (player_strength_score * {player_strength_weight})
                        + (prediction_score * {prediction_weight})
                        + (combined_team_confidence * {confidence_weight})
                    ) / ({partnership_weight + player_strength_weight + prediction_weight + confidence_weight}),
                    4
                )
            ELSE ROUND(
                (
                    COALESCE(partnership_score, 0.0) * {partnership_weight}
                    + COALESCE(player_strength_score, 0.0) * {player_strength_weight}
                    + COALESCE(prediction_score, 0.0) * {prediction_weight}
                    + COALESCE(combined_team_confidence, 0.0) * {confidence_weight}
                )
                / NULLIF(
                    (CASE WHEN partnership_score IS NOT NULL THEN {partnership_weight} ELSE 0.0 END)
                    + (CASE WHEN player_strength_score IS NOT NULL THEN {player_strength_weight} ELSE 0.0 END)
                    + (CASE WHEN prediction_score IS NOT NULL THEN {prediction_weight} ELSE 0.0 END)
                    + (CASE WHEN combined_team_confidence IS NOT NULL THEN {confidence_weight} ELSE 0.0 END),
                    0.0
                ),
                4
            )
        END AS raw_team_selection_score,
        CASE
            WHEN combined_team_confidence IS NULL THEN NULL
            ELSE ROUND(0.5 + (0.5 * combined_team_confidence / 100.0), 6)
        END AS confidence_factor,
        CASE
            WHEN combined_team_confidence IS NULL THEN NULL
            ELSE ROUND(
                (
                    CASE
                        WHEN partnership_score IS NOT NULL
                          AND player_strength_score IS NOT NULL
                          AND prediction_score IS NOT NULL
                          AND combined_team_confidence IS NOT NULL
                            THEN ROUND(
                                (
                                    (partnership_score * {partnership_weight})
                                    + (player_strength_score * {player_strength_weight})
                                    + (prediction_score * {prediction_weight})
                                    + (combined_team_confidence * {confidence_weight})
                                ) / ({partnership_weight + player_strength_weight + prediction_weight + confidence_weight}),
                                4
                            )
                        ELSE ROUND(
                            (
                                COALESCE(partnership_score, 0.0) * {partnership_weight}
                                + COALESCE(player_strength_score, 0.0) * {player_strength_weight}
                                + COALESCE(prediction_score, 0.0) * {prediction_weight}
                                + COALESCE(combined_team_confidence, 0.0) * {confidence_weight}
                            )
                            / NULLIF(
                                (CASE WHEN partnership_score IS NOT NULL THEN {partnership_weight} ELSE 0.0 END)
                                + (CASE WHEN player_strength_score IS NOT NULL THEN {player_strength_weight} ELSE 0.0 END)
                                + (CASE WHEN prediction_score IS NOT NULL THEN {prediction_weight} ELSE 0.0 END)
                                + (CASE WHEN combined_team_confidence IS NOT NULL THEN {confidence_weight} ELSE 0.0 END),
                                0.0
                            ),
                            4
                        )
                    END
                ) * (0.5 + (0.5 * combined_team_confidence / 100.0)),
                4
            )
        END AS confidence_adjusted_team_score,
        ROUND(
            LEAST(
                40.0,
                (CASE WHEN membership_overlap_warning_flag THEN 15.0 ELSE 0.0 END)
                + (CASE WHEN evidence_sufficiency_status = '{LIMITED_EVIDENCE}' THEN 10.0 ELSE 0.0 END)
                + (CASE WHEN evidence_sufficiency_status = '{NONE_EVIDENCE}' THEN 20.0 ELSE 0.0 END)
                + (CASE WHEN NOT candidate_attribution_allowed_flag THEN 15.0 ELSE 0.0 END)
                + (CASE WHEN COALESCE(combined_team_confidence, 0.0) < 60.0 THEN 10.0 ELSE 0.0 END)
            ),
            4
        ) AS risk_penalty_score,
        ROUND(
            COALESCE(
                (
                    CASE
                        WHEN combined_team_confidence IS NULL THEN NULL
                        ELSE ROUND(
                            (
                                CASE
                                    WHEN partnership_score IS NOT NULL
                                      AND player_strength_score IS NOT NULL
                                      AND prediction_score IS NOT NULL
                                      AND combined_team_confidence IS NOT NULL
                                        THEN ROUND(
                                            (
                                                (partnership_score * {partnership_weight})
                                                + (player_strength_score * {player_strength_weight})
                                                + (prediction_score * {prediction_weight})
                                                + (combined_team_confidence * {confidence_weight})
                                            ) / ({partnership_weight + player_strength_weight + prediction_weight + confidence_weight}),
                                            4
                                        )
                                    ELSE ROUND(
                                        (
                                            COALESCE(partnership_score, 0.0) * {partnership_weight}
                                            + COALESCE(player_strength_score, 0.0) * {player_strength_weight}
                                            + COALESCE(prediction_score, 0.0) * {prediction_weight}
                                            + COALESCE(combined_team_confidence, 0.0) * {confidence_weight}
                                        )
                                        / NULLIF(
                                            (CASE WHEN partnership_score IS NOT NULL THEN {partnership_weight} ELSE 0.0 END)
                                            + (CASE WHEN player_strength_score IS NOT NULL THEN {player_strength_weight} ELSE 0.0 END)
                                            + (CASE WHEN prediction_score IS NOT NULL THEN {prediction_weight} ELSE 0.0 END)
                                            + (CASE WHEN combined_team_confidence IS NOT NULL THEN {confidence_weight} ELSE 0.0 END),
                                            0.0
                                        ),
                                        4
                                    )
                                END
                            ) * (0.5 + (0.5 * combined_team_confidence / 100.0)),
                            4
                        )
                    END
                ),
                0.0
            ) - LEAST(
                40.0,
                (CASE WHEN membership_overlap_warning_flag THEN 15.0 ELSE 0.0 END)
                + (CASE WHEN evidence_sufficiency_status = '{LIMITED_EVIDENCE}' THEN 10.0 ELSE 0.0 END)
                + (CASE WHEN evidence_sufficiency_status = '{NONE_EVIDENCE}' THEN 20.0 ELSE 0.0 END)
                + (CASE WHEN NOT candidate_attribution_allowed_flag THEN 15.0 ELSE 0.0 END)
                + (CASE WHEN COALESCE(combined_team_confidence, 0.0) < 60.0 THEN 10.0 ELSE 0.0 END)
            ),
            4
        ) AS final_team_selection_score,
        CONCAT_WS(
            ',',
            CASE WHEN partnership_score IS NOT NULL AND partnership_score >= 70.0 THEN 'partnership' END,
            CASE WHEN player_strength_score IS NOT NULL AND player_strength_score >= 70.0 THEN 'player_strength' END,
            CASE WHEN prediction_score IS NOT NULL AND prediction_score >= 70.0 THEN 'prediction' END,
            CASE WHEN combined_team_confidence IS NOT NULL AND combined_team_confidence >= 70.0 THEN 'confidence' END
        ) AS top_strengths,
        CONCAT_WS(
            ',',
            CASE WHEN membership_overlap_warning_flag THEN 'membership_overlap' END,
            CASE WHEN evidence_sufficiency_status = '{LIMITED_EVIDENCE}' THEN 'limited_evidence' END,
            CASE WHEN evidence_sufficiency_status = '{NONE_EVIDENCE}' THEN 'no_evidence' END,
            CASE WHEN NOT candidate_attribution_allowed_flag THEN 'non_attributable_history' END,
            CASE WHEN COALESCE(combined_team_confidence, 0.0) < 60.0 THEN 'low_confidence' END,
            CASE WHEN material_limitation_text IS NOT NULL AND material_limitation_text <> '' THEN 'quality_limitations' END
        ) AS top_risks,
        CONCAT(
            'Final=',
            COALESCE(CAST(
                ROUND(
                    COALESCE(
                        (
                            CASE
                                WHEN combined_team_confidence IS NULL THEN NULL
                                ELSE ROUND(
                                    (
                                        CASE
                                            WHEN partnership_score IS NOT NULL
                                              AND player_strength_score IS NOT NULL
                                              AND prediction_score IS NOT NULL
                                              AND combined_team_confidence IS NOT NULL
                                                THEN ROUND(
                                                    (
                                                        (partnership_score * {partnership_weight})
                                                        + (player_strength_score * {player_strength_weight})
                                                        + (prediction_score * {prediction_weight})
                                                        + (combined_team_confidence * {confidence_weight})
                                                    ) / ({partnership_weight + player_strength_weight + prediction_weight + confidence_weight}),
                                                    4
                                                )
                                            ELSE ROUND(
                                                (
                                                    COALESCE(partnership_score, 0.0) * {partnership_weight}
                                                    + COALESCE(player_strength_score, 0.0) * {player_strength_weight}
                                                    + COALESCE(prediction_score, 0.0) * {prediction_weight}
                                                    + COALESCE(combined_team_confidence, 0.0) * {confidence_weight}
                                                )
                                                / NULLIF(
                                                    (CASE WHEN partnership_score IS NOT NULL THEN {partnership_weight} ELSE 0.0 END)
                                                    + (CASE WHEN player_strength_score IS NOT NULL THEN {player_strength_weight} ELSE 0.0 END)
                                                    + (CASE WHEN prediction_score IS NOT NULL THEN {prediction_weight} ELSE 0.0 END)
                                                    + (CASE WHEN combined_team_confidence IS NOT NULL THEN {confidence_weight} ELSE 0.0 END),
                                                    0.0
                                                ),
                                                4
                                            )
                                        END
                                    ) * (0.5 + (0.5 * combined_team_confidence / 100.0)),
                                    4
                                )
                            END
                        ),
                        0.0
                    ) - LEAST(
                        40.0,
                        (CASE WHEN membership_overlap_warning_flag THEN 15.0 ELSE 0.0 END)
                        + (CASE WHEN evidence_sufficiency_status = '{LIMITED_EVIDENCE}' THEN 10.0 ELSE 0.0 END)
                        + (CASE WHEN evidence_sufficiency_status = '{NONE_EVIDENCE}' THEN 20.0 ELSE 0.0 END)
                        + (CASE WHEN NOT candidate_attribution_allowed_flag THEN 15.0 ELSE 0.0 END)
                        + (CASE WHEN COALESCE(combined_team_confidence, 0.0) < 60.0 THEN 10.0 ELSE 0.0 END)
                    ),
                    4
                ) AS STRING
            ), 'NA'),
            '; Confidence=',
            COALESCE(CAST(ROUND(COALESCE(combined_team_confidence, 0.0), 4) AS STRING), '0.0'),
            '; Eligibility=',
            CASE
                WHEN eligibility_reason_codes IS NOT NULL AND eligibility_reason_codes <> '' THEN '{INELIGIBLE_STATUS}'
                WHEN evidence_sufficiency_status = '{LIMITED_EVIDENCE}' THEN '{REVIEW_REQUIRED_STATUS}'
                ELSE '{ELIGIBLE_STATUS}'
            END
        ) AS ranking_rationale
    FROM eligibility_rows
)
SELECT * FROM final_rows
""".strip()


def build_olympic_team_candidates_sql(
    environment: ReleaseEnvironment,
    *,
    scoring_scenario: str,
    eligibility_config: dict[str, Any],
) -> str:
    """Return the Spark SQL used to build olympic_team_candidates."""
    scorecards_fqn = get_gold_target_table_fqn(environment, "team_selection_scorecards")
    primary_count = int(eligibility_config.get("primary_teams_per_country_category", 1))
    alternate_count = int(eligibility_config.get("alternate_teams_per_country_category", 2))
    watchlist_count = int(eligibility_config.get("watchlist_teams_per_country_category", 3))

    return f"""
WITH ranked_candidates AS (
    SELECT
        country_code,
        team_category AS category_code,
        team_id,
        scoring_scenario,
        analysis_as_of_date,
        ROW_NUMBER() OVER (
            PARTITION BY country_code, team_category, scoring_scenario
            ORDER BY final_team_selection_score DESC,
                     combined_team_confidence DESC,
                     team_id ASC
        ) AS candidate_rank,
        final_team_selection_score,
        confidence_adjusted_team_score,
        raw_team_selection_score,
        combined_team_confidence,
        evidence_sufficiency_status,
        candidate_attribution_allowed_flag,
        player_one_id,
        player_two_id,
        player_one_display_name,
        player_two_display_name,
        top_strengths,
        top_risks,
        ranking_rationale AS candidate_rationale
    FROM {scorecards_fqn}
    WHERE scoring_scenario = '{scoring_scenario}'
      AND eligible_team_flag
      AND final_team_selection_score IS NOT NULL
)
SELECT
    country_code,
    category_code,
    team_id,
    scoring_scenario,
    analysis_as_of_date,
    candidate_rank,
    CASE
        WHEN candidate_rank <= {primary_count} THEN 'PRIMARY'
        WHEN candidate_rank <= {primary_count + alternate_count} THEN 'ALTERNATE'
        WHEN candidate_rank <= {primary_count + alternate_count + watchlist_count} THEN 'WATCHLIST'
        ELSE NULL
    END AS recommendation_tier,
    final_team_selection_score,
    confidence_adjusted_team_score,
    raw_team_selection_score,
    combined_team_confidence,
    evidence_sufficiency_status,
    candidate_attribution_allowed_flag,
    player_one_id,
    player_two_id,
    player_one_display_name,
    player_two_display_name,
    top_strengths,
    top_risks,
    candidate_rationale
FROM ranked_candidates
WHERE candidate_rank <= {primary_count + alternate_count + watchlist_count}
""".strip()


def build_olympic_team_candidates(
    *,
    team_selection_scorecard_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    scoring_scenario: str,
    eligibility_config: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Build ranked Olympic team candidates from published team scorecards."""
    primary_count = int(eligibility_config.get("primary_teams_per_country_category", 1))
    alternate_count = int(eligibility_config.get("alternate_teams_per_country_category", 2))
    watchlist_count = int(eligibility_config.get("watchlist_teams_per_country_category", 3))

    eligible_rows = [
        row
        for row in team_selection_scorecard_rows
        if row.get("scoring_scenario") == scoring_scenario
        and _coerce_bool(row.get("eligible_team_flag"))
        and _coerce_float(row.get("final_team_selection_score")) is not None
    ]

    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in eligible_rows:
        key = (
            _normalize_required_string(row.get("country_code")),
            _normalize_required_string(row.get("team_category")),
        )
        grouped_rows.setdefault(key, []).append(row)

    candidate_rows: list[dict[str, Any]] = []
    for (country_code, category_code), rows in sorted(grouped_rows.items()):
        ordered_rows = sorted(
            rows,
            key=lambda row: (
                -float(_coerce_float(row.get("final_team_selection_score")) or 0.0),
                -float(_coerce_float(row.get("combined_team_confidence")) or 0.0),
                str(row.get("team_id") or ""),
            ),
        )
        for rank, row in enumerate(ordered_rows, start=1):
            tier = _candidate_tier(
                rank,
                primary_count=primary_count,
                alternate_count=alternate_count,
                watchlist_count=watchlist_count,
            )
            if tier is None:
                continue
            candidate_rows.append(
                {
                    "country_code": country_code,
                    "category_code": category_code,
                    "team_id": row.get("team_id"),
                    "scoring_scenario": scoring_scenario,
                    "analysis_as_of_date": row.get("analysis_as_of_date"),
                    "candidate_rank": rank,
                    "recommendation_tier": tier,
                    "final_team_selection_score": row.get("final_team_selection_score"),
                    "confidence_adjusted_team_score": row.get("confidence_adjusted_team_score"),
                    "raw_team_selection_score": row.get("raw_team_selection_score"),
                    "combined_team_confidence": row.get("combined_team_confidence"),
                    "evidence_sufficiency_status": row.get("evidence_sufficiency_status"),
                    "candidate_attribution_allowed_flag": row.get(
                        "candidate_attribution_allowed_flag"
                    ),
                    "player_one_id": row.get("player_one_id"),
                    "player_two_id": row.get("player_two_id"),
                    "player_one_display_name": row.get("player_one_display_name"),
                    "player_two_display_name": row.get("player_two_display_name"),
                    "top_strengths": row.get("top_strengths"),
                    "top_risks": row.get("top_risks"),
                    "candidate_rationale": row.get("ranking_rationale"),
                }
            )

    return tuple(candidate_rows)


def publish_team_selection_scorecards_from_sql(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
) -> TeamSelectionScorecardsPublicationSummary:
    """Build and publish team_selection_scorecards using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "team_selection_scorecards")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "team_selection_scorecards")
    stage_row_count, output_row_count = publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_team_selection_scorecards_sql(
            environment,
            analysis_as_of_date=analysis_as_of_date,
            scoring_scenario=scoring_scenario,
            scorecards_config=scorecards_config,
            eligibility_config=eligibility_config,
        ),
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("team_id", "scoring_scenario"),
            label="team_selection_scorecards",
        ),
    )
    return TeamSelectionScorecardsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=stage_row_count,
        output_row_count=output_row_count,
    )


def publish_olympic_team_candidates_from_sql(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    scoring_scenario: str,
    eligibility_config: dict[str, Any],
) -> OlympicTeamCandidatesPublicationSummary:
    """Build and publish olympic_team_candidates using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "olympic_team_candidates")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "olympic_team_candidates")
    stage_row_count, output_row_count = publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_olympic_team_candidates_sql(
            environment,
            scoring_scenario=scoring_scenario,
            eligibility_config=eligibility_config,
        ),
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("country_code", "category_code", "team_id", "scoring_scenario"),
            label="olympic_team_candidates",
        ),
    )
    return OlympicTeamCandidatesPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=stage_row_count,
        output_row_count=output_row_count,
    )


def publish_team_selection_scorecards(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> TeamSelectionScorecardsPublicationSummary:
    """Publish team_selection_scorecards from Python-built records."""
    target_table_fqn = get_gold_target_table_fqn(environment, "team_selection_scorecards")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "team_selection_scorecards")
    publish_stage_records_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        records=rows,
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("team_id", "scoring_scenario"),
            label="team_selection_scorecards",
        ),
    )
    output_row_count = int(spark.table(target_table_fqn).count())
    return TeamSelectionScorecardsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=len(rows),
        output_row_count=output_row_count,
    )


def publish_olympic_team_candidates(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> OlympicTeamCandidatesPublicationSummary:
    """Publish olympic_team_candidates from Python-built records."""
    target_table_fqn = get_gold_target_table_fqn(environment, "olympic_team_candidates")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "olympic_team_candidates")
    if not rows:
        publish_sql_table(
            spark,
            stage_table_fqn,
            _build_empty_olympic_team_candidates_sql(),
        )
        _validate_key_constraints(
            spark,
            stage_table_fqn,
            key_columns=("country_code", "category_code", "team_id", "scoring_scenario"),
            label="olympic_team_candidates",
        )
        publish_sql_table(
            spark,
            target_table_fqn,
            f"SELECT * FROM {stage_table_fqn}",
        )
        output_row_count = int(spark.table(target_table_fqn).count())
        return OlympicTeamCandidatesPublicationSummary(
            target_table_fqn=target_table_fqn,
            stage_table_fqn=stage_table_fqn,
            input_row_count=0,
            output_row_count=output_row_count,
        )

    publish_stage_records_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        records=rows,
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("country_code", "category_code", "team_id", "scoring_scenario"),
            label="olympic_team_candidates",
        ),
    )
    output_row_count = int(spark.table(target_table_fqn).count())
    return OlympicTeamCandidatesPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=len(rows),
        output_row_count=output_row_count,
    )


def _build_empty_olympic_team_candidates_sql() -> str:
    return """
SELECT
    CAST(NULL AS STRING) AS country_code,
    CAST(NULL AS STRING) AS category_code,
    CAST(NULL AS STRING) AS team_id,
    CAST(NULL AS STRING) AS scoring_scenario,
    CAST(NULL AS DATE) AS analysis_as_of_date,
    CAST(NULL AS INT) AS candidate_rank,
    CAST(NULL AS STRING) AS recommendation_tier,
    CAST(NULL AS DOUBLE) AS final_team_selection_score,
    CAST(NULL AS DOUBLE) AS confidence_adjusted_team_score,
    CAST(NULL AS DOUBLE) AS raw_team_selection_score,
    CAST(NULL AS DOUBLE) AS combined_team_confidence,
    CAST(NULL AS STRING) AS evidence_sufficiency_status,
    CAST(NULL AS BOOLEAN) AS candidate_attribution_allowed_flag,
    CAST(NULL AS STRING) AS player_one_id,
    CAST(NULL AS STRING) AS player_two_id,
    CAST(NULL AS STRING) AS player_one_display_name,
    CAST(NULL AS STRING) AS player_two_display_name,
    CAST(NULL AS STRING) AS top_strengths,
    CAST(NULL AS STRING) AS top_risks,
    CAST(NULL AS STRING) AS candidate_rationale
WHERE 1 = 0
""".strip()


def _current_members_by_team(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
) -> dict[str, tuple[str, ...]]:
    current_members: dict[str, set[str]] = {}
    for row in team_memberships_rows:
        team_id = _normalize_optional_string(row.get("team_id"))
        player_id = _normalize_optional_string(row.get("player_id"))
        if team_id is None or player_id is None:
            continue
        if not _membership_is_active_as_of_date(
            row,
            analysis_as_of_date=analysis_as_of_date,
        ):
            continue
        current_members.setdefault(team_id, set()).add(player_id)
    return {
        team_id: tuple(sorted(player_ids))
        for team_id, player_ids in current_members.items()
    }


def _membership_overlap_by_team(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
) -> dict[str, bool]:
    overlap: dict[str, bool] = {}
    for row in team_memberships_rows:
        team_id = _normalize_optional_string(row.get("team_id"))
        if team_id is None:
            continue
        if not _membership_is_active_as_of_date(
            row,
            analysis_as_of_date=analysis_as_of_date,
        ):
            continue
        overlap[team_id] = overlap.get(team_id, False) or _coerce_bool(
            row.get("membership_overlap_flag")
        )
    return overlap


def _team_resolution_confidence_by_id(
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
) -> dict[str, float]:
    totals: dict[str, list[float]] = {}
    for row in rows:
        team_id = _normalize_optional_string(row.get("resolved_team_id"))
        match_date = _coerce_date(row.get("match_date"))
        confidence = _coerce_float(row.get("team_resolution_confidence"))
        if team_id is None or match_date is None or confidence is None:
            continue
        if match_date > analysis_as_of_date:
            continue
        totals.setdefault(team_id, []).append(confidence)
    return {
        team_id: round(sum(values) / len(values), 4)
        for team_id, values in totals.items()
        if values
    }


def _prediction_strength_by_team_id(
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
) -> dict[str, float]:
    probabilities_by_team: dict[str, list[float]] = {}
    for row in rows:
        match_date = _coerce_date(row.get("match_date"))
        if match_date is not None and match_date > analysis_as_of_date:
            continue
        probability = _coerce_float(row.get("model_predicted_probability"))
        team_a = _normalize_optional_string(row.get("team_a_team_id"))
        team_b = _normalize_optional_string(row.get("team_b_team_id"))
        if probability is not None and team_a is not None:
            probabilities_by_team.setdefault(team_a, []).append(probability * 100.0)
        if probability is not None and team_b is not None:
            probabilities_by_team.setdefault(team_b, []).append((1.0 - probability) * 100.0)
    return {
        team_id: round(sum(values) / len(values), 4)
        for team_id, values in probabilities_by_team.items()
        if values
    }


def _derive_evidence_sufficiency_status(
    team_perf_row: dict[str, Any],
    partnership_row: dict[str, Any],
    player_one_row: dict[str, Any],
    player_two_row: dict[str, Any],
) -> str:
    statuses = {
        _normalize_optional_string(team_perf_row.get("feature_evidence_status")),
        _normalize_optional_string(partnership_row.get("feature_evidence_status")),
        _normalize_optional_string(player_one_row.get("evidence_band")),
        _normalize_optional_string(player_two_row.get("evidence_band")),
    }
    if not team_perf_row or not partnership_row:
        return NONE_EVIDENCE
    if NONE_EVIDENCE in statuses or None in statuses:
        return NONE_EVIDENCE
    if LIMITED_EVIDENCE in statuses or "LOW" in statuses or "MODERATE" in statuses:
        return LIMITED_EVIDENCE
    return SUFFICIENT_EVIDENCE


def _derive_eligibility_reason_codes(
    *,
    team_row: dict[str, Any],
    team_id: str,
    current_member_count: int,
    player_one_id: str | None,
    player_two_id: str | None,
    player_one_row: dict[str, Any],
    player_two_row: dict[str, Any],
    candidate_attribution_allowed_flag: bool,
    evidence_sufficiency_status: str,
    require_active_team: bool,
    analysis_as_of_date: date,
    overlap_warning_flag: bool,
    active_as_of_date: bool,
) -> list[str]:
    reason_codes: list[str] = []
    del team_row, team_id, candidate_attribution_allowed_flag, require_active_team
    del analysis_as_of_date, active_as_of_date
    if current_member_count != 2:
        reason_codes.append("INVALID_MEMBERSHIP_COUNT")
    if overlap_warning_flag:
        reason_codes.append("AMBIGUOUS_TEAM_COMPOSITION")
    if player_one_id is None or player_two_id is None:
        reason_codes.append("UNKNOWN_PLAYER")
    if player_one_id is not None and not player_one_row:
        reason_codes.append("UNKNOWN_PLAYER")
    if player_two_id is not None and not player_two_row:
        reason_codes.append("UNKNOWN_PLAYER")
    if evidence_sufficiency_status == NONE_EVIDENCE:
        reason_codes.append("NO_VALID_TEAM_ID")
    return list(dict.fromkeys(reason_codes))


def _derive_eligibility_status(
    *,
    reason_codes: list[str],
    evidence_sufficiency_status: str,
) -> str:
    if reason_codes:
        if evidence_sufficiency_status == LIMITED_EVIDENCE and reason_codes == ["CRITICAL_QUALITY_FAILURE"]:
            return REVIEW_REQUIRED_STATUS
        return INELIGIBLE_STATUS
    if evidence_sufficiency_status == LIMITED_EVIDENCE:
        return REVIEW_REQUIRED_STATUS
    return ELIGIBLE_STATUS


def _risk_penalty_score(row: dict[str, Any], combined_team_confidence: float | None) -> float:
    penalty = 0.0
    if row.get("membership_overlap_warning_flag"):
        penalty += 15.0
    if row.get("evidence_sufficiency_status") == LIMITED_EVIDENCE:
        penalty += 10.0
    if row.get("evidence_sufficiency_status") == NONE_EVIDENCE:
        penalty += 20.0
    if not _coerce_bool(row.get("candidate_attribution_allowed_flag")):
        penalty += 15.0
    if (combined_team_confidence or 0.0) < 60.0:
        penalty += 10.0
    return round(min(penalty, 40.0), 4)


def _top_strengths(component_scores: dict[str, float | None]) -> str | None:
    ordered = sorted(
        [
            (name, value)
            for name, value in component_scores.items()
            if value is not None
        ],
        key=lambda item: (-float(item[1]), item[0]),
    )
    text = ",".join(name for name, _value in ordered[:3])
    return text or None


def _top_risks(row: dict[str, Any], combined_team_confidence: float | None) -> str | None:
    risks: list[str] = []
    if row.get("membership_overlap_warning_flag"):
        risks.append("membership_overlap")
    if row.get("evidence_sufficiency_status") == LIMITED_EVIDENCE:
        risks.append("limited_evidence")
    if row.get("evidence_sufficiency_status") == NONE_EVIDENCE:
        risks.append("no_evidence")
    if not _coerce_bool(row.get("candidate_attribution_allowed_flag")):
        risks.append("non_attributable_history")
    if (combined_team_confidence or 0.0) < 60.0:
        risks.append("low_confidence")
    if row.get("material_limitation_text"):
        risks.append("quality_limitations")
    return ",".join(risks[:3]) or None


def _candidate_tier(
    rank: int,
    *,
    primary_count: int,
    alternate_count: int,
    watchlist_count: int,
) -> str | None:
    if rank <= primary_count:
        return "PRIMARY"
    if rank <= primary_count + alternate_count:
        return "ALTERNATE"
    if rank <= primary_count + alternate_count + watchlist_count:
        return "WATCHLIST"
    return None


def _apply_group_percentiles(
    rows: list[dict[str, Any]],
    *,
    group_fields: tuple[str, ...],
    raw_field: str,
    output_field: str,
) -> None:
    grouped_values: dict[tuple[str, ...], list[float]] = {}
    for row in rows:
        group_value = tuple(str(row.get(field) or "") for field in group_fields)
        raw_value = _coerce_float(row.get(raw_field))
        if any(not value for value in group_value) or raw_value is None:
            continue
        grouped_values.setdefault(group_value, []).append(raw_value)

    percentile_maps = {
        group_value: _percentile_map(values)
        for group_value, values in grouped_values.items()
    }
    for row in rows:
        group_value = tuple(str(row.get(field) or "") for field in group_fields)
        raw_value = _coerce_float(row.get(raw_field))
        if any(not value for value in group_value) or raw_value is None:
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


def _minimum_non_null(*values: float | None) -> float | None:
    available = [float(value) for value in values if value is not None]
    if not available:
        return None
    return round(min(available), 4)


def _score_balance(first_score: float | None, second_score: float | None) -> float | None:
    if first_score is None or second_score is None:
        return None
    return round(max(0.0, 100.0 - abs(first_score - second_score)), 4)


def _scale_unit_interval(value: Any) -> float | None:
    float_value = _coerce_float(value)
    if float_value is None:
        return None
    return round(max(0.0, min(1.0, float_value)) * 100.0, 4)


def _scale_signed_signal(value: Any) -> float | None:
    float_value = _coerce_float(value)
    if float_value is None:
        return None
    return round(max(0.0, min(100.0, 50.0 + (float_value * 100.0))), 4)


def _scale_days(value: Any, *, ceiling_days: float = 365.0) -> float | None:
    float_value = _coerce_float(value)
    if float_value is None:
        return None
    if ceiling_days <= 0.0:
        return None
    return round(max(0.0, min(1.0, float_value / ceiling_days)) * 100.0, 4)


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


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    text = _normalize_optional_string(value)
    if text is None:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _membership_is_active_as_of_date(
    membership_row: dict[str, Any],
    *,
    analysis_as_of_date: date,
) -> bool:
    start_date = _coerce_date(membership_row.get("membership_start_date"))
    end_date = _coerce_date(membership_row.get("membership_end_date"))
    if start_date is not None and start_date > analysis_as_of_date:
        return False
    if end_date is not None and end_date < analysis_as_of_date:
        return False
    if start_date is None and end_date is None:
        return _coerce_bool(membership_row.get("current_membership_flag"))
    return True


def _team_is_active_as_of_date(
    team_row: dict[str, Any],
    *,
    analysis_as_of_date: date,
) -> bool:
    formation_date = _coerce_date(team_row.get("formation_date"))
    dissolution_date = _coerce_date(team_row.get("dissolution_date"))
    if formation_date is not None and formation_date > analysis_as_of_date:
        return False
    if dissolution_date is not None and dissolution_date <= analysis_as_of_date:
        return False
    if formation_date is None and dissolution_date is None:
        return _coerce_bool(team_row.get("active_flag"))
    return True
