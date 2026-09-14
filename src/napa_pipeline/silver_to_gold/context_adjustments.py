"""Gold contextual factors for Olympic team-selection scorecards."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import sqrt
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_stage_table_fqn,
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.publish import publish_stage_to_gold_table


@dataclass(frozen=True)
class TeamContextAdjustmentFeaturesPublicationSummary:
    """Published-table summary for team_context_adjustment_features."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


def clamp(value: float, floor: float, ceiling: float) -> float:
    """Return a value bounded inclusively by the supplied limits."""
    return max(floor, min(ceiling, value))


def player_regional_strength_factor(
    *,
    effective_observations: float | None,
    regional_mean_residual: float | None,
    country_mean_residual: float | None,
    regional_config: dict[str, Any],
) -> float:
    """Calculate a reliability-shrunk, country-centered regional factor."""
    if (
        effective_observations is None
        or regional_mean_residual is None
        or country_mean_residual is None
        or effective_observations < float(regional_config["min_effective_observations"])
    ):
        return 1.0
    reliability = effective_observations / (
        effective_observations + float(regional_config["shrinkage_k"])
    )
    centered_residual = regional_mean_residual - country_mean_residual
    adjustment = float(regional_config["residual_multiplier"]) * centered_residual * reliability
    return clamp(
        1.0 + adjustment,
        float(regional_config["factor_floor"]),
        float(regional_config["factor_ceiling"]),
    )


def player_age_factor(age: float | None, age_config: dict[str, Any]) -> float:
    """Return the bounded current-performance age factor with a neutral null fallback."""
    if age is None:
        return 1.0
    excess = max(float(age) - float(age_config["neutral_through_age"]), 0.0)
    penalty = min(
        float(age_config["max_penalty"]),
        float(age_config["quadratic_coefficient"]) * excess * excess,
    )
    return 1.0 - penalty


def player_fatigue_factor(fatigue_load: float | None, fatigue_config: dict[str, Any]) -> float:
    """Return the bounded recent-workload factor with a neutral null fallback."""
    load = max(float(fatigue_load or 0.0), 0.0)
    penalty = min(
        float(fatigue_config["max_penalty"]),
        float(fatigue_config["penalty_rate"])
        * max(load - float(fatigue_config["threshold"]), 0.0),
    )
    return 1.0 - penalty


def geometric_team_factor(player_one_factor: float, player_two_factor: float) -> float:
    """Combine two non-negative player factors without averaging partner context."""
    return sqrt(max(player_one_factor, 0.0) * max(player_two_factor, 0.0))


def context_adjustment_factor(
    *,
    team_regional_strength_factor: float,
    team_age_factor: float,
    team_fatigue_factor: float,
    composite_config: dict[str, Any],
) -> float:
    """Compose bounded regional, age, and fatigue factors."""
    return clamp(
        team_regional_strength_factor * team_age_factor * team_fatigue_factor,
        float(composite_config["factor_floor"]),
        float(composite_config["factor_ceiling"]),
    )


def build_team_context_adjustment_features_sql(
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    context_config: dict[str, Any],
) -> str:
    """Return Spark SQL for one transparent contextual-feature row per team."""
    players_fqn = get_silver_source_table_fqn(environment, "players")
    teams_fqn = get_silver_source_table_fqn(environment, "teams")
    memberships_fqn = get_silver_source_table_fqn(environment, "team_memberships")
    match_sides_fqn = get_gold_target_table_fqn(environment, "competition_match_sides")
    player_matches_fqn = get_gold_target_table_fqn(environment, "competition_player_matches")
    analysis_date = analysis_as_of_date.isoformat()
    regional = context_config["regional"]
    age = context_config["age"]
    fatigue = context_config["fatigue"]
    composite = context_config["composite"]

    return f"""
WITH memberships_as_of AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        CAST(player_id AS STRING) AS player_id
    FROM {memberships_fqn}
    WHERE team_id IS NOT NULL
      AND player_id IS NOT NULL
      AND CASE
          WHEN CAST(membership_start_date AS DATE) IS NOT NULL
           AND CAST(membership_start_date AS DATE) > DATE('{analysis_date}') THEN FALSE
          WHEN CAST(membership_end_date AS DATE) IS NOT NULL
           AND CAST(membership_end_date AS DATE) < DATE('{analysis_date}') THEN FALSE
          WHEN CAST(membership_start_date AS DATE) IS NULL
           AND CAST(membership_end_date AS DATE) IS NULL
            THEN COALESCE(CAST(current_membership_flag AS BOOLEAN), FALSE)
          ELSE TRUE
      END
), team_members AS (
    SELECT
        team_id,
        sort_array(collect_set(player_id), true) AS player_ids
    FROM memberships_as_of
    GROUP BY team_id
), player_context AS (
    SELECT
        CAST(player_id AS STRING) AS player_id,
        UPPER(TRIM(CAST(country_code AS STRING))) AS country_code,
        CAST(home_region_id AS STRING) AS home_region_id,
        CAST(age AS DOUBLE) AS age
    FROM {players_fqn}
    WHERE player_id IS NOT NULL
), regional_sides AS (
    SELECT
        CAST(match_id AS STRING) AS match_id,
        CAST(match_date AS DATE) AS match_date,
        CAST(player_one_id AS STRING) AS player_one_id,
        CAST(player_two_id AS STRING) AS player_two_id,
        CASE WHEN CAST(won_flag AS BOOLEAN) THEN 1.0 ELSE 0.0 END
          - (1.0 / (1.0 + POW(10.0,
              (CAST(opponent_pre_match_team_rating AS DOUBLE) - CAST(pre_match_team_rating AS DOUBLE)) / 400.0
            ))) AS rating_residual
    FROM {match_sides_fqn}
    WHERE CAST(completed_flag AS BOOLEAN)
      AND CAST(match_date AS DATE) <= DATE('{analysis_date}')
      AND CAST(match_date AS DATE) >= DATE_SUB(DATE('{analysis_date}'), {int(regional['lookback_days'])})
      AND pre_match_team_rating IS NOT NULL
      AND opponent_pre_match_team_rating IS NOT NULL
      AND NOT COALESCE(CAST(side_cardinality_warning_flag AS BOOLEAN), FALSE)
), regional_player_observations AS (
    SELECT pc.country_code, pc.home_region_id, rs.rating_residual, 0.5 AS observation_weight
    FROM regional_sides AS rs
    INNER JOIN player_context AS pc ON pc.player_id = rs.player_one_id
    WHERE pc.home_region_id IS NOT NULL
    UNION ALL
    SELECT pc.country_code, pc.home_region_id, rs.rating_residual, 0.5 AS observation_weight
    FROM regional_sides AS rs
    INNER JOIN player_context AS pc ON pc.player_id = rs.player_two_id
    WHERE pc.home_region_id IS NOT NULL
), country_residuals AS (
    SELECT
        country_code,
        SUM(rating_residual * observation_weight) / NULLIF(SUM(observation_weight), 0.0) AS country_mean_residual
    FROM regional_player_observations
    GROUP BY country_code
), regional_residuals AS (
    SELECT
        observation.country_code,
        observation.home_region_id,
        SUM(observation.observation_weight) AS regional_effective_observations,
        SUM(observation.rating_residual * observation.observation_weight)
          / NULLIF(SUM(observation.observation_weight), 0.0) AS regional_mean_residual,
        country.country_mean_residual
    FROM regional_player_observations AS observation
    INNER JOIN country_residuals AS country ON country.country_code = observation.country_code
    GROUP BY observation.country_code, observation.home_region_id, country.country_mean_residual
), deduplicated_player_matches AS (
    SELECT match_id, match_date, player_id, points_for, points_against
    FROM (
        SELECT
            CAST(match_id AS STRING) AS match_id,
            CAST(match_date AS DATE) AS match_date,
            CAST(player_id AS STRING) AS player_id,
            CAST(points_for AS DOUBLE) AS points_for,
            CAST(points_against AS DOUBLE) AS points_against,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(match_id AS STRING), CAST(player_id AS STRING)
                ORDER BY COALESCE(CAST(points_for AS DOUBLE), 0.0) + COALESCE(CAST(points_against AS DOUBLE), 0.0) DESC,
                         CAST(team_id AS STRING) ASC
            ) AS duplicate_rank
        FROM {player_matches_fqn}
        WHERE match_id IS NOT NULL
          AND player_id IS NOT NULL
          AND CAST(match_date AS DATE) < DATE('{analysis_date}')
          AND CAST(match_date AS DATE) >= DATE_SUB(DATE('{analysis_date}'), {int(fatigue['lookback_days'])})
    )
    WHERE duplicate_rank = 1
), fatigue_by_player AS (
    SELECT
        player_id,
        COUNT(*) AS matches_last_10_days,
        SUM(
            (1.0 + (COALESCE(points_for, 0.0) + COALESCE(points_against, 0.0)) / {float(fatigue['points_scale'])})
            * EXP(-DATEDIFF(DATE('{analysis_date}'), match_date) / {float(fatigue['decay_days'])})
        ) AS weighted_fatigue_load
    FROM deduplicated_player_matches
    GROUP BY player_id
), base_teams AS (
    SELECT CAST(team_id AS STRING) AS team_id
    FROM {teams_fqn}
    WHERE team_id IS NOT NULL
), player_factors AS (
    SELECT
        team.team_id,
        get(member.player_ids, 0) AS player_one_id,
        get(member.player_ids, 1) AS player_two_id,
        player_one.home_region_id AS player_one_home_region_id,
        player_two.home_region_id AS player_two_home_region_id,
        regional_one.regional_effective_observations AS player_one_regional_effective_observations,
        regional_two.regional_effective_observations AS player_two_regional_effective_observations,
        regional_one.regional_mean_residual AS player_one_regional_mean_residual,
        regional_two.regional_mean_residual AS player_two_regional_mean_residual,
        regional_one.country_mean_residual AS player_one_country_mean_residual,
        regional_two.country_mean_residual AS player_two_country_mean_residual,
        CASE WHEN regional_one.regional_effective_observations IS NULL THEN NULL
             ELSE regional_one.regional_effective_observations / (regional_one.regional_effective_observations + {float(regional['shrinkage_k'])}) END AS player_one_regional_reliability,
        CASE WHEN regional_two.regional_effective_observations IS NULL THEN NULL
             ELSE regional_two.regional_effective_observations / (regional_two.regional_effective_observations + {float(regional['shrinkage_k'])}) END AS player_two_regional_reliability,
        player_one.age AS player_one_age,
        player_two.age AS player_two_age,
        COALESCE(fatigue_one.matches_last_10_days, 0) AS player_one_matches_last_10_days,
        COALESCE(fatigue_two.matches_last_10_days, 0) AS player_two_matches_last_10_days,
        COALESCE(fatigue_one.weighted_fatigue_load, 0.0) AS player_one_fatigue_load,
        COALESCE(fatigue_two.weighted_fatigue_load, 0.0) AS player_two_fatigue_load,
        CASE WHEN regional_one.regional_effective_observations < {float(regional['min_effective_observations'])}
                  OR regional_one.regional_effective_observations IS NULL THEN 1.0
             ELSE 1.0 + GREATEST({float(regional['factor_floor']) - 1.0}, LEAST({float(regional['factor_ceiling']) - 1.0},
                {float(regional['residual_multiplier'])} * (regional_one.regional_mean_residual - regional_one.country_mean_residual)
                * (regional_one.regional_effective_observations / (regional_one.regional_effective_observations + {float(regional['shrinkage_k'])}))
             )) END AS player_one_regional_strength_factor,
        CASE WHEN regional_two.regional_effective_observations < {float(regional['min_effective_observations'])}
                  OR regional_two.regional_effective_observations IS NULL THEN 1.0
             ELSE 1.0 + GREATEST({float(regional['factor_floor']) - 1.0}, LEAST({float(regional['factor_ceiling']) - 1.0},
                {float(regional['residual_multiplier'])} * (regional_two.regional_mean_residual - regional_two.country_mean_residual)
                * (regional_two.regional_effective_observations / (regional_two.regional_effective_observations + {float(regional['shrinkage_k'])}))
             )) END AS player_two_regional_strength_factor,
        CASE WHEN player_one.age IS NULL THEN 1.0 ELSE 1.0 - LEAST({float(age['max_penalty'])}, {float(age['quadratic_coefficient'])} * POW(GREATEST(player_one.age - {float(age['neutral_through_age'])}, 0.0), 2)) END AS player_one_age_factor,
        CASE WHEN player_two.age IS NULL THEN 1.0 ELSE 1.0 - LEAST({float(age['max_penalty'])}, {float(age['quadratic_coefficient'])} * POW(GREATEST(player_two.age - {float(age['neutral_through_age'])}, 0.0), 2)) END AS player_two_age_factor,
        1.0 - LEAST({float(fatigue['max_penalty'])}, {float(fatigue['penalty_rate'])} * GREATEST(COALESCE(fatigue_one.weighted_fatigue_load, 0.0) - {float(fatigue['threshold'])}, 0.0)) AS player_one_fatigue_factor,
        1.0 - LEAST({float(fatigue['max_penalty'])}, {float(fatigue['penalty_rate'])} * GREATEST(COALESCE(fatigue_two.weighted_fatigue_load, 0.0) - {float(fatigue['threshold'])}, 0.0)) AS player_two_fatigue_factor
    FROM base_teams AS team
    LEFT JOIN team_members AS member ON member.team_id = team.team_id
    LEFT JOIN player_context AS player_one ON player_one.player_id = get(member.player_ids, 0)
    LEFT JOIN player_context AS player_two ON player_two.player_id = get(member.player_ids, 1)
    LEFT JOIN regional_residuals AS regional_one ON regional_one.country_code = player_one.country_code AND regional_one.home_region_id = player_one.home_region_id
    LEFT JOIN regional_residuals AS regional_two ON regional_two.country_code = player_two.country_code AND regional_two.home_region_id = player_two.home_region_id
    LEFT JOIN fatigue_by_player AS fatigue_one ON fatigue_one.player_id = player_one.player_id
    LEFT JOIN fatigue_by_player AS fatigue_two ON fatigue_two.player_id = player_two.player_id
), team_factors AS (
    SELECT
        *,
        SQRT(GREATEST(player_one_regional_strength_factor, 0.0) * GREATEST(player_two_regional_strength_factor, 0.0)) AS team_regional_strength_factor,
        SQRT(GREATEST(player_one_age_factor, 0.0) * GREATEST(player_two_age_factor, 0.0)) AS team_age_factor,
        SQRT(GREATEST(player_one_fatigue_factor, 0.0) * GREATEST(player_two_fatigue_factor, 0.0)) AS team_fatigue_factor
    FROM player_factors
)
SELECT
    team_id,
    DATE('{analysis_date}') AS analysis_as_of_date,
    player_one_id, player_two_id,
    player_one_home_region_id, player_two_home_region_id,
    player_one_regional_effective_observations, player_two_regional_effective_observations,
    player_one_regional_mean_residual, player_two_regional_mean_residual,
    player_one_country_mean_residual, player_two_country_mean_residual,
    player_one_regional_reliability, player_two_regional_reliability,
    player_one_regional_strength_factor, player_two_regional_strength_factor,
    team_regional_strength_factor,
    player_one_age, player_two_age, player_one_age_factor, player_two_age_factor, team_age_factor,
    player_one_matches_last_10_days, player_two_matches_last_10_days,
    player_one_fatigue_load, player_two_fatigue_load,
    player_one_fatigue_factor, player_two_fatigue_factor, team_fatigue_factor,
    team_regional_strength_factor * team_age_factor * team_fatigue_factor AS unbounded_context_adjustment_factor,
    GREATEST({float(composite['factor_floor'])}, LEAST({float(composite['factor_ceiling'])},
        team_regional_strength_factor * team_age_factor * team_fatigue_factor
    )) AS context_adjustment_factor,
    CASE
        WHEN player_one_id IS NULL OR player_two_id IS NULL
          OR player_one_age IS NULL OR player_two_age IS NULL
          OR player_one_regional_effective_observations < {float(regional['min_effective_observations'])}
          OR player_two_regional_effective_observations < {float(regional['min_effective_observations'])}
          OR player_one_regional_effective_observations IS NULL
          OR player_two_regional_effective_observations IS NULL THEN 'PARTIAL'
        ELSE 'COMPLETE'
    END AS context_evidence_status
FROM team_factors
""".strip()


def publish_team_context_adjustment_features(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    context_config: dict[str, Any],
) -> TeamContextAdjustmentFeaturesPublicationSummary:
    """Stage and publish the context feature table with one row per Silver team."""
    stage_table_fqn = get_gold_stage_table_fqn(environment, "team_context_adjustment_features")
    target_table_fqn = get_gold_target_table_fqn(environment, "team_context_adjustment_features")
    stage_sql = build_team_context_adjustment_features_sql(
        environment,
        analysis_as_of_date=analysis_as_of_date,
        context_config=context_config,
    )
    input_row_count, output_row_count = publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=stage_sql,
    )
    return TeamContextAdjustmentFeaturesPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=input_row_count,
        output_row_count=output_row_count,
    )
