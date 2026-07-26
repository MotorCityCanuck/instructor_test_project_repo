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
from napa_pipeline.silver_to_gold.publish import publish_stage_records_to_gold_table


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
    scorecard_rows = build_team_selection_scorecards(
        teams_rows=_collect_table_rows(spark, get_silver_source_table_fqn(environment, "teams")),
        team_memberships_rows=_collect_table_rows(
            spark,
            get_silver_source_table_fqn(environment, "team_memberships"),
        ),
        team_performance_features_rows=_collect_table_rows(
            spark,
            get_gold_target_table_fqn(environment, "team_performance_features"),
        ),
        partnership_effectiveness_rows=_collect_table_rows(
            spark,
            get_gold_target_table_fqn(environment, "partnership_effectiveness"),
        ),
        player_scorecard_rows=_collect_table_rows(
            spark,
            get_gold_target_table_fqn(environment, "player_evaluation_scorecards"),
        ),
        entity_data_quality_confidence_rows=_collect_table_rows(
            spark,
            get_gold_target_table_fqn(environment, "entity_data_quality_confidence"),
        ),
        resolved_match_teams_rows=_collect_table_rows(
            spark,
            get_gold_target_table_fqn(environment, "resolved_match_teams"),
        ),
        match_outcome_predictions_rows=_collect_table_rows(
            spark,
            get_gold_target_table_fqn(environment, "match_outcome_predictions"),
        ),
        analysis_as_of_date=analysis_as_of_date,
        scoring_scenario=scoring_scenario,
        scorecards_config=scorecards_config,
        eligibility_config=eligibility_config,
    )
    scorecards_summary = publish_team_selection_scorecards(
        spark,
        environment,
        rows=scorecard_rows,
    )
    candidate_rows = build_olympic_team_candidates(
        team_selection_scorecard_rows=scorecard_rows,
        scoring_scenario=scoring_scenario,
        eligibility_config=eligibility_config,
    )
    candidates_summary = publish_olympic_team_candidates(
        spark,
        environment,
        rows=candidate_rows,
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

    current_members_by_team = _current_members_by_team(team_memberships_rows)
    overlap_by_team = _membership_overlap_by_team(team_memberships_rows)
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

        active_flag = _coerce_bool(team_row.get("active_flag"))
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
                "eligible_team_flag": eligibility_status == ELIGIBLE_STATUS,
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
        and row.get("eligibility_status") == ELIGIBLE_STATUS
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


def _current_members_by_team(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[str, tuple[str, ...]]:
    current_members: dict[str, set[str]] = {}
    for row in team_memberships_rows:
        team_id = _normalize_optional_string(row.get("team_id"))
        player_id = _normalize_optional_string(row.get("player_id"))
        if team_id is None or player_id is None:
            continue
        if not _coerce_bool(row.get("current_membership_flag")):
            continue
        current_members.setdefault(team_id, set()).add(player_id)
    return {
        team_id: tuple(sorted(player_ids))
        for team_id, player_ids in current_members.items()
    }


def _membership_overlap_by_team(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[str, bool]:
    overlap: dict[str, bool] = {}
    for row in team_memberships_rows:
        team_id = _normalize_optional_string(row.get("team_id"))
        if team_id is None:
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
) -> list[str]:
    reason_codes: list[str] = []
    active_flag = _coerce_bool(team_row.get("active_flag"))
    dissolution_date = _coerce_date(team_row.get("dissolution_date"))
    if require_active_team and not active_flag:
        reason_codes.append("TEAM_NOT_ACTIVE")
    if dissolution_date is not None and dissolution_date <= analysis_as_of_date:
        reason_codes.append("TEAM_DISSOLVED")
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
    if not candidate_attribution_allowed_flag:
        reason_codes.append("CRITICAL_QUALITY_FAILURE")
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
