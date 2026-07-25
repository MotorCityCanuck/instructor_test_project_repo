"""Phase 7 team and partnership feature builders for the Silver-to-Gold pipeline."""

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
from napa_pipeline.silver_to_gold.publish import publish_stage_to_gold_table


NO_EVIDENCE = "NONE"
LIMITED_EVIDENCE = "LIMITED"
SUFFICIENT_EVIDENCE = "SUFFICIENT"


TEAM_FEATURE_REGISTRY: tuple[dict[str, Any], ...] = (
    {
        "feature_name": "shrinkage_adjusted_win_rate",
        "description": "Observed team win rate stabilized toward a neutral prior.",
        "source": "competition_match_sides,resolved_match_teams",
        "grain": "team_id,evidence_window",
        "window": "all",
        "calculation": "(wins + prior_matches * 0.5) / (match_count + prior_matches)",
        "direction": "higher_is_better",
        "minimum_evidence": 1,
        "null_behavior": "null when match_count = 0",
        "version": "1.0.0",
    },
    {
        "feature_name": "performance_above_expectation",
        "description": "Observed team win rate minus average pre-match expected win probability.",
        "source": "competition_match_sides",
        "grain": "team_id,evidence_window",
        "window": "all",
        "calculation": "win_pct - avg_expected_win_probability",
        "direction": "higher_is_better",
        "minimum_evidence": 1,
        "null_behavior": "null when match_count = 0",
        "version": "1.0.0",
    },
    {
        "feature_name": "recent_form_win_pct",
        "description": "Observed team win percentage over the trailing recent window.",
        "source": "competition_match_sides,resolved_match_teams",
        "grain": "team_id,evidence_window",
        "window": "trailing_90",
        "calculation": "recent wins / recent matches",
        "direction": "higher_is_better",
        "minimum_evidence": 1,
        "null_behavior": "null when no recent matches exist",
        "version": "1.0.0",
    },
    {
        "feature_name": "consistency_score",
        "description": "Composite stability score combining win rate and negative-tail point-share quality.",
        "source": "competition_match_sides",
        "grain": "team_id,evidence_window",
        "window": "all",
        "calculation": "bounded weighted composite of win_pct, point_share_stddev, and worst quartile point share",
        "direction": "higher_is_better",
        "minimum_evidence": 5,
        "null_behavior": "null when below minimum consistency evidence",
        "version": "1.0.0",
    },
    {
        "feature_name": "partnership_duration_days",
        "description": "Observed duration between the first and latest shared match for a partnership or team.",
        "source": "competition_match_sides,resolved_match_teams",
        "grain": "team_id,evidence_window or partnership_key",
        "window": "all",
        "calculation": "datediff(max_match_date, min_match_date) + 1",
        "direction": "higher_is_better",
        "minimum_evidence": 1,
        "null_behavior": "null when no shared match history exists",
        "version": "1.0.0",
    },
    {
        "feature_name": "synergy_proxy",
        "description": "Observed partnership win rate relative to average individual player performance.",
        "source": "player_performance_features,player_current_ratings,resolved_match_teams,competition_match_sides",
        "grain": "partnership_key",
        "window": "career and trailing_90 inputs",
        "calculation": "shared_win_pct - average_player_win_pct",
        "direction": "higher_is_better",
        "minimum_evidence": 1,
        "null_behavior": "null when player or shared partnership evidence is unavailable",
        "version": "1.0.0",
    },
)


@dataclass(frozen=True)
class TeamPerformanceFeaturesPublicationSummary:
    """Published-table summary for team_performance_features."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class PartnershipEffectivenessPublicationSummary:
    """Published-table summary for partnership_effectiveness."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


def get_team_feature_registry() -> tuple[dict[str, Any], ...]:
    """Return the Phase 7 team feature registry."""
    return TEAM_FEATURE_REGISTRY


def calculate_shrinkage_adjusted_win_rate(
    *,
    win_count: int,
    match_count: int,
    prior_matches: int,
    prior_mean: float = 0.5,
) -> float | None:
    """Return a neutral-prior stabilized win rate."""
    if match_count <= 0:
        return None
    denominator = match_count + max(prior_matches, 0)
    if denominator <= 0:
        return None
    return (float(win_count) + (max(prior_matches, 0) * prior_mean)) / float(denominator)


def calculate_evidence_reliability_score(
    *,
    match_count: int,
    sufficient_match_count: int,
    attributable_flag: bool,
) -> float:
    """Return a bounded evidence reliability score on a 0-100 scale."""
    if match_count <= 0 or sufficient_match_count <= 0:
        return 0.0
    coverage_component = min(float(match_count) / float(sufficient_match_count), 1.0)
    attribution_component = 1.0 if attributable_flag else 0.6
    return round(100.0 * coverage_component * attribution_component, 4)


def build_team_performance_features_sql(
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    features_config: dict[str, Any],
    evidence_windows_config: dict[str, Any],
) -> str:
    """Return the Spark SQL used to build team_performance_features."""
    resolved_fqn = get_gold_target_table_fqn(environment, "resolved_match_teams")
    match_sides_fqn = get_gold_target_table_fqn(environment, "competition_match_sides")
    teams_fqn = get_silver_source_table_fqn(environment, "teams")
    team_memberships_fqn = get_silver_source_table_fqn(environment, "team_memberships")
    analysis_date_literal = analysis_as_of_date.isoformat()
    recency_half_life_days = float(features_config.get("recency_half_life_days", 60))
    minimum_matches_for_consistency = int(features_config.get("minimum_matches_for_consistency", 5))
    minimum_matches_for_team_scorecard = int(
        evidence_windows_config.get("minimum_matches_for_team_scorecard", 3)
    )
    trailing_365 = int(evidence_windows_config.get("primary_window_days", 365))
    trailing_180 = int(evidence_windows_config.get("trend_window_days", 180))
    trailing_90 = int(evidence_windows_config.get("recent_window_days", 90))

    return f"""
WITH windows AS (
    SELECT 'career' AS evidence_window, CAST(NULL AS INT) AS window_days
    UNION ALL SELECT 'trailing_365', {trailing_365}
    UNION ALL SELECT 'trailing_180', {trailing_180}
    UNION ALL SELECT 'trailing_90', {trailing_90}
),
base_teams AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        UPPER(TRIM(CAST(team_category AS STRING))) AS team_category,
        UPPER(TRIM(CAST(country_code AS STRING))) AS country_code,
        UPPER(TRIM(CAST(team_status AS STRING))) AS team_status,
        CAST(active_flag AS BOOLEAN) AS active_flag,
        CAST(formation_date AS DATE) AS formation_date,
        CAST(dissolution_date AS DATE) AS dissolution_date
    FROM {teams_fqn}
    WHERE team_id IS NOT NULL
),
team_membership_quality AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        MAX(CASE WHEN COALESCE(CAST(membership_overlap_flag AS BOOLEAN), FALSE) THEN 1 ELSE 0 END)
            AS membership_overlap_warning_int
    FROM {team_memberships_fqn}
    WHERE team_id IS NOT NULL
    GROUP BY CAST(team_id AS STRING)
),
eligible_sides AS (
    SELECT
        CAST(rmt.resolved_team_id AS STRING) AS team_id,
        COALESCE(CAST(rmt.candidate_attribution_allowed_flag AS BOOLEAN), FALSE)
            AS candidate_attribution_allowed_flag,
        CAST(cms.match_id AS STRING) AS match_id,
        CAST(cms.match_date AS DATE) AS match_date,
        CAST(cms.won_flag AS BOOLEAN) AS won_flag,
        CAST(cms.lost_flag AS BOOLEAN) AS lost_flag,
        CAST(cms.games_won AS DOUBLE) AS games_won,
        CAST(cms.games_lost AS DOUBLE) AS games_lost,
        CAST(cms.point_share AS DOUBLE) AS point_share,
        CAST(cms.point_differential AS DOUBLE) AS point_differential,
        CAST(cms.pre_match_team_rating AS DOUBLE) AS pre_match_team_rating,
        CAST(cms.opponent_pre_match_team_rating AS DOUBLE) AS opponent_pre_match_team_rating,
        CAST(cms.close_game_count AS DOUBLE) AS close_game_count,
        CAST(cms.deciding_game_flag AS BOOLEAN) AS deciding_game_flag,
        CAST(cms.membership_history_warning_flag AS BOOLEAN) AS membership_history_warning_flag
    FROM {resolved_fqn} AS rmt
    INNER JOIN {match_sides_fqn} AS cms
      ON CAST(rmt.match_id AS STRING) = CAST(cms.match_id AS STRING)
     AND CAST(rmt.match_team_id AS STRING) = CAST(cms.match_team_id AS STRING)
    WHERE rmt.resolved_team_id IS NOT NULL
      AND CAST(cms.match_date AS DATE) IS NOT NULL
      AND CAST(cms.match_date AS DATE) <= DATE('{analysis_date_literal}')
),
windowed_sides AS (
    SELECT
        w.evidence_window,
        es.*
    FROM windows AS w
    INNER JOIN eligible_sides AS es
      ON w.window_days IS NULL
      OR DATEDIFF(DATE('{analysis_date_literal}'), es.match_date) <= w.window_days
),
expected_values AS (
    SELECT
        evidence_window,
        team_id,
        match_id,
        match_date,
        won_flag,
        lost_flag,
        games_won,
        games_lost,
        point_share,
        point_differential,
        close_game_count,
        deciding_game_flag,
        candidate_attribution_allowed_flag,
        membership_history_warning_flag,
        opponent_pre_match_team_rating,
        POWER(
            0.5,
            CAST(DATEDIFF(DATE('{analysis_date_literal}'), match_date) AS DOUBLE) / {recency_half_life_days}
        ) AS recency_weight,
        CASE
            WHEN pre_match_team_rating IS NULL OR opponent_pre_match_team_rating IS NULL THEN NULL
            ELSE 1.0 / (
                1.0 + POWER(10.0, ((opponent_pre_match_team_rating - pre_match_team_rating) / 400.0))
            )
        END AS expected_win_probability
    FROM windowed_sides
),
team_recent_form AS (
    SELECT
        team_id,
        AVG(CASE WHEN won_flag THEN 1.0 ELSE 0.0 END) AS recent_form_win_pct
    FROM eligible_sides
    WHERE DATEDIFF(DATE('{analysis_date_literal}'), match_date) <= {trailing_90}
    GROUP BY team_id
),
point_share_quartiles AS (
    SELECT
        evidence_window,
        team_id,
        point_share,
        NTILE(4) OVER (
            PARTITION BY evidence_window, team_id
            ORDER BY point_share ASC
        ) AS quartile_rank
    FROM expected_values
    WHERE point_share IS NOT NULL
),
worst_quartile AS (
    SELECT
        evidence_window,
        team_id,
        AVG(point_share) AS worst_quartile_point_share
    FROM point_share_quartiles
    WHERE quartile_rank = 1
    GROUP BY evidence_window, team_id
),
window_aggregates AS (
    SELECT
        evidence_window,
        team_id,
        MAX(CASE WHEN candidate_attribution_allowed_flag THEN 1 ELSE 0 END)
            AS candidate_attribution_allowed_int,
        COUNT(*) AS match_count,
        SUM(CASE WHEN won_flag THEN 1 ELSE 0 END) AS win_count,
        SUM(CASE WHEN lost_flag THEN 1 ELSE 0 END) AS loss_count,
        SUM(COALESCE(games_won, 0.0)) AS total_games_won,
        SUM(COALESCE(games_lost, 0.0)) AS total_games_lost,
        AVG(point_share) AS avg_point_share,
        AVG(point_differential) AS avg_point_differential,
        AVG(expected_win_probability) AS avg_expected_win_probability,
        AVG(opponent_pre_match_team_rating) AS strength_of_schedule,
        SUM(recency_weight) AS weighted_match_total,
        SUM(CASE WHEN won_flag THEN recency_weight ELSE 0.0 END) AS weighted_win_total,
        SUM(CASE WHEN close_game_count > 0 AND won_flag THEN 1 ELSE 0 END) AS close_match_win_count,
        SUM(CASE WHEN close_game_count > 0 THEN 1 ELSE 0 END) AS close_match_count,
        SUM(CASE WHEN deciding_game_flag AND won_flag THEN 1 ELSE 0 END) AS deciding_game_win_count,
        SUM(CASE WHEN deciding_game_flag THEN 1 ELSE 0 END) AS deciding_game_count,
        STDDEV_POP(point_share) AS point_share_stddev,
        MIN(match_date) AS first_match_date,
        MAX(match_date) AS last_match_date,
        MAX(CASE WHEN membership_history_warning_flag THEN 1 ELSE 0 END) AS membership_history_warning_int
    FROM expected_values
    GROUP BY evidence_window, team_id
)
SELECT
    bt.team_id,
    w.evidence_window,
    DATE('{analysis_date_literal}') AS analysis_as_of_date,
    bt.team_category,
    bt.country_code,
    bt.team_status,
    bt.active_flag,
    bt.formation_date,
    bt.dissolution_date,
    CASE
        WHEN COALESCE(wa.match_count, 0) = 0 THEN COALESCE(bt.active_flag, FALSE)
        ELSE COALESCE(wa.candidate_attribution_allowed_int, 0) = 1
    END AS candidate_attribution_allowed_flag,
    COALESCE(wa.match_count, 0) AS match_count,
    COALESCE(wa.win_count, 0) AS win_count,
    COALESCE(wa.loss_count, 0) AS loss_count,
    CASE
        WHEN COALESCE(wa.match_count, 0) = 0 THEN NULL
        ELSE wa.win_count / wa.match_count
    END AS win_pct,
    CASE
        WHEN COALESCE(wa.match_count, 0) = 0 THEN NULL
        ELSE (wa.win_count + ({minimum_matches_for_team_scorecard} * 0.5))
            / (wa.match_count + {minimum_matches_for_team_scorecard})
    END AS shrinkage_adjusted_win_rate,
    CASE
        WHEN COALESCE(wa.total_games_won, 0.0) + COALESCE(wa.total_games_lost, 0.0) = 0.0 THEN NULL
        ELSE wa.total_games_won / (wa.total_games_won + wa.total_games_lost)
    END AS game_win_pct,
    wa.avg_point_share,
    wa.avg_point_differential,
    wa.avg_expected_win_probability,
    CASE
        WHEN COALESCE(wa.match_count, 0) = 0 THEN NULL
        ELSE (wa.win_count / wa.match_count) - COALESCE(wa.avg_expected_win_probability, 0.0)
    END AS performance_above_expectation,
    wa.strength_of_schedule,
    trf.recent_form_win_pct,
    CASE
        WHEN COALESCE(wa.close_match_count, 0) = 0 THEN NULL
        ELSE wa.close_match_win_count / wa.close_match_count
    END AS close_match_win_pct,
    CASE
        WHEN COALESCE(wa.deciding_game_count, 0) = 0 THEN NULL
        ELSE wa.deciding_game_win_count / wa.deciding_game_count
    END AS deciding_game_win_pct,
    wa.point_share_stddev,
    wq.worst_quartile_point_share,
    CASE
        WHEN COALESCE(wa.match_count, 0) < {minimum_matches_for_consistency} THEN NULL
        ELSE ROUND(
            100.0 * (
                0.50 * (wa.win_count / wa.match_count)
                + 0.25 * GREATEST(0.0, LEAST(1.0, 1.0 - (COALESCE(wa.point_share_stddev, 0.25) / 0.25)))
                + 0.25 * GREATEST(0.0, LEAST(1.0, COALESCE(wq.worst_quartile_point_share, 0.0)))
            ),
            4
        )
    END AS consistency_score,
    CASE
        WHEN wa.first_match_date IS NULL OR wa.last_match_date IS NULL THEN NULL
        ELSE DATEDIFF(wa.last_match_date, wa.first_match_date) + 1
    END AS partnership_duration_days,
    (
        COALESCE(tmq.membership_overlap_warning_int, 0) = 1
        OR COALESCE(wa.membership_history_warning_int, 0) = 1
    ) AS membership_overlap_warning_flag,
    ROUND(
        100.0
        * LEAST(
            1.0,
            COALESCE(wa.match_count, 0) / CAST({minimum_matches_for_team_scorecard} AS DOUBLE)
        )
        * CASE
            WHEN COALESCE(wa.match_count, 0) = 0 THEN 0.0
            WHEN COALESCE(wa.candidate_attribution_allowed_int, 0) = 1 THEN 1.0
            ELSE 0.6
        END,
        4
    ) AS evidence_reliability_score,
    CASE
        WHEN COALESCE(wa.match_count, 0) = 0 THEN '{NO_EVIDENCE}'
        WHEN COALESCE(wa.match_count, 0) < {minimum_matches_for_team_scorecard} THEN '{LIMITED_EVIDENCE}'
        ELSE '{SUFFICIENT_EVIDENCE}'
    END AS feature_evidence_status
FROM base_teams AS bt
CROSS JOIN windows AS w
LEFT JOIN window_aggregates AS wa
  ON wa.team_id = bt.team_id
 AND wa.evidence_window = w.evidence_window
LEFT JOIN team_recent_form AS trf
  ON trf.team_id = bt.team_id
LEFT JOIN worst_quartile AS wq
  ON wq.team_id = bt.team_id
 AND wq.evidence_window = w.evidence_window
LEFT JOIN team_membership_quality AS tmq
  ON tmq.team_id = bt.team_id
""".strip()


def build_partnership_effectiveness_sql(
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    evidence_windows_config: dict[str, Any],
) -> str:
    """Return the Spark SQL used to build partnership_effectiveness."""
    resolved_fqn = get_gold_target_table_fqn(environment, "resolved_match_teams")
    match_sides_fqn = get_gold_target_table_fqn(environment, "competition_match_sides")
    player_ratings_fqn = get_gold_target_table_fqn(environment, "player_current_ratings")
    player_features_fqn = get_gold_target_table_fqn(environment, "player_performance_features")
    teams_fqn = get_silver_source_table_fqn(environment, "teams")
    analysis_date_literal = analysis_as_of_date.isoformat()
    recent_window_days = int(evidence_windows_config.get("recent_window_days", 90))
    minimum_matches_for_team_scorecard = int(
        evidence_windows_config.get("minimum_matches_for_team_scorecard", 3)
    )

    return f"""
WITH eligible_partnerships AS (
    SELECT
        COALESCE(CAST(rmt.resolved_team_id AS STRING), CAST(rmt.canonical_player_pair_key AS STRING))
            AS partnership_key,
        CAST(rmt.resolved_team_id AS STRING) AS team_id,
        CAST(rmt.canonical_player_pair_key AS STRING) AS canonical_player_pair_key,
        CAST(rmt.player_one_id AS STRING) AS player_one_id,
        CAST(rmt.player_two_id AS STRING) AS player_two_id,
        CAST(cms.match_id AS STRING) AS match_id,
        CAST(cms.match_date AS DATE) AS match_date,
        CAST(cms.won_flag AS BOOLEAN) AS won_flag,
        CAST(cms.lost_flag AS BOOLEAN) AS lost_flag,
        CAST(cms.pre_match_team_rating AS DOUBLE) AS pre_match_team_rating,
        CAST(cms.opponent_pre_match_team_rating AS DOUBLE) AS pre_match_opponent_team_rating,
        COALESCE(CAST(rmt.candidate_attribution_allowed_flag AS BOOLEAN), FALSE)
            AS candidate_attribution_allowed_flag
    FROM {resolved_fqn} AS rmt
    INNER JOIN {match_sides_fqn} AS cms
      ON CAST(rmt.match_id AS STRING) = CAST(cms.match_id AS STRING)
     AND CAST(rmt.match_team_id AS STRING) = CAST(cms.match_team_id AS STRING)
    WHERE rmt.canonical_player_pair_key IS NOT NULL
      AND CAST(cms.match_date AS DATE) IS NOT NULL
      AND CAST(cms.match_date AS DATE) <= DATE('{analysis_date_literal}')
),
partnership_rollup AS (
    SELECT
        partnership_key,
        MAX(team_id) AS team_id,
        MAX(canonical_player_pair_key) AS canonical_player_pair_key,
        MAX(player_one_id) AS player_one_id,
        MAX(player_two_id) AS player_two_id,
        MAX(CASE WHEN candidate_attribution_allowed_flag THEN 1 ELSE 0 END)
            AS candidate_attribution_allowed_int,
        COUNT(*) AS shared_match_count,
        SUM(CASE WHEN won_flag THEN 1 ELSE 0 END) AS shared_win_count,
        SUM(CASE WHEN lost_flag THEN 1 ELSE 0 END) AS shared_loss_count,
        AVG(
            CASE
                WHEN pre_match_team_rating IS NULL OR pre_match_opponent_team_rating IS NULL THEN NULL
                ELSE 1.0 / (
                    1.0 + POWER(10.0, ((pre_match_opponent_team_rating - pre_match_team_rating) / 400.0))
                )
            END
        ) AS avg_expected_win_probability,
        SUM(
            CASE
                WHEN DATEDIFF(DATE('{analysis_date_literal}'), match_date) <= {recent_window_days}
                    THEN 1 ELSE 0
            END
        ) AS recent_shared_match_count,
        SUM(
            CASE
                WHEN DATEDIFF(DATE('{analysis_date_literal}'), match_date) <= {recent_window_days}
                     AND won_flag THEN 1 ELSE 0
            END
        ) AS recent_shared_win_count,
        MIN(match_date) AS first_shared_match_date,
        MAX(match_date) AS last_shared_match_date
    FROM eligible_partnerships
    GROUP BY partnership_key
),
current_ratings AS (
    SELECT
        CAST(player_id AS STRING) AS player_id,
        CAST(analytical_rating_value AS DOUBLE) AS analytical_rating_value
    FROM {player_ratings_fqn}
),
player_features_career AS (
    SELECT
        CAST(player_id AS STRING) AS player_id,
        CAST(win_pct AS DOUBLE) AS career_win_pct
    FROM {player_features_fqn}
    WHERE evidence_window = 'career'
),
player_features_recent AS (
    SELECT
        CAST(player_id AS STRING) AS player_id,
        CAST(win_pct AS DOUBLE) AS recent_win_pct
    FROM {player_features_fqn}
    WHERE evidence_window = 'trailing_90'
),
teams_base AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        UPPER(TRIM(CAST(team_category AS STRING))) AS team_category,
        UPPER(TRIM(CAST(country_code AS STRING))) AS country_code
    FROM {teams_fqn}
    WHERE team_id IS NOT NULL
)
SELECT
    pr.partnership_key,
    pr.team_id,
    DATE('{analysis_date_literal}') AS analysis_as_of_date,
    pr.canonical_player_pair_key,
    pr.player_one_id,
    pr.player_two_id,
    tb.team_category,
    tb.country_code,
    COALESCE(pr.candidate_attribution_allowed_int, 0) = 1 AS candidate_attribution_allowed_flag,
    pr.team_id IS NULL AS unresolved_partnership_flag,
    pr.shared_match_count,
    pr.shared_win_count,
    pr.shared_loss_count,
    CASE
        WHEN pr.shared_match_count = 0 THEN NULL
        ELSE pr.shared_win_count / CAST(pr.shared_match_count AS DOUBLE)
    END AS shared_win_pct,
    pr.recent_shared_match_count,
    CASE
        WHEN pr.recent_shared_match_count = 0 THEN NULL
        ELSE pr.recent_shared_win_count / CAST(pr.recent_shared_match_count AS DOUBLE)
    END AS recent_shared_win_pct,
    CASE
        WHEN pr.shared_match_count = 0 THEN NULL
        ELSE (pr.shared_win_count + ({minimum_matches_for_team_scorecard} * 0.5))
            / (pr.shared_match_count + {minimum_matches_for_team_scorecard})
    END AS team_adjusted_win_rate,
    CASE
        WHEN pr.shared_match_count = 0 THEN NULL
        ELSE (pr.shared_win_count / CAST(pr.shared_match_count AS DOUBLE))
            - COALESCE(pr.avg_expected_win_probability, 0.0)
    END AS team_performance_above_expectation,
    (
        COALESCE(cr1.analytical_rating_value, 0.0) + COALESCE(cr2.analytical_rating_value, 0.0)
    ) / CASE
        WHEN cr1.analytical_rating_value IS NOT NULL AND cr2.analytical_rating_value IS NOT NULL THEN 2.0
        WHEN cr1.analytical_rating_value IS NOT NULL OR cr2.analytical_rating_value IS NOT NULL THEN 1.0
        ELSE NULL
    END AS combined_player_current_rating,
    (
        COALESCE(pfc1.career_win_pct, 0.0) + COALESCE(pfc2.career_win_pct, 0.0)
    ) / CASE
        WHEN pfc1.career_win_pct IS NOT NULL AND pfc2.career_win_pct IS NOT NULL THEN 2.0
        WHEN pfc1.career_win_pct IS NOT NULL OR pfc2.career_win_pct IS NOT NULL THEN 1.0
        ELSE NULL
    END AS average_player_win_pct,
    (
        COALESCE(pfr1.recent_win_pct, 0.0) + COALESCE(pfr2.recent_win_pct, 0.0)
    ) / CASE
        WHEN pfr1.recent_win_pct IS NOT NULL AND pfr2.recent_win_pct IS NOT NULL THEN 2.0
        WHEN pfr1.recent_win_pct IS NOT NULL OR pfr2.recent_win_pct IS NOT NULL THEN 1.0
        ELSE NULL
    END AS average_player_recent_win_pct,
    CASE
        WHEN pr.shared_match_count = 0 THEN NULL
        WHEN (
            CASE
                WHEN pfc1.career_win_pct IS NOT NULL AND pfc2.career_win_pct IS NOT NULL THEN 2.0
                WHEN pfc1.career_win_pct IS NOT NULL OR pfc2.career_win_pct IS NOT NULL THEN 1.0
                ELSE NULL
            END
        ) IS NULL THEN NULL
        ELSE (pr.shared_win_count / CAST(pr.shared_match_count AS DOUBLE))
            - (
                (COALESCE(pfc1.career_win_pct, 0.0) + COALESCE(pfc2.career_win_pct, 0.0))
                / (
                    CASE
                        WHEN pfc1.career_win_pct IS NOT NULL AND pfc2.career_win_pct IS NOT NULL THEN 2.0
                        WHEN pfc1.career_win_pct IS NOT NULL OR pfc2.career_win_pct IS NOT NULL THEN 1.0
                        ELSE 1.0
                    END
                )
            )
    END AS synergy_proxy,
    CASE
        WHEN pr.first_shared_match_date IS NULL OR pr.last_shared_match_date IS NULL THEN NULL
        ELSE DATEDIFF(pr.last_shared_match_date, pr.first_shared_match_date) + 1
    END AS partnership_duration_days,
    CASE
        WHEN pr.last_shared_match_date IS NULL THEN NULL
        ELSE DATEDIFF(DATE('{analysis_date_literal}'), pr.last_shared_match_date)
    END AS days_since_last_shared_match,
    ROUND(
        100.0
        * LEAST(1.0, pr.shared_match_count / CAST({minimum_matches_for_team_scorecard} AS DOUBLE))
        * CASE
            WHEN COALESCE(pr.candidate_attribution_allowed_int, 0) = 1 THEN 1.0
            ELSE 0.6
        END,
        4
    ) AS evidence_reliability_score,
    CASE
        WHEN pr.shared_match_count = 0 THEN '{NO_EVIDENCE}'
        WHEN pr.shared_match_count < {minimum_matches_for_team_scorecard} THEN '{LIMITED_EVIDENCE}'
        ELSE '{SUFFICIENT_EVIDENCE}'
    END AS feature_evidence_status
FROM partnership_rollup AS pr
LEFT JOIN teams_base AS tb
  ON tb.team_id = pr.team_id
LEFT JOIN current_ratings AS cr1
  ON cr1.player_id = pr.player_one_id
LEFT JOIN current_ratings AS cr2
  ON cr2.player_id = pr.player_two_id
LEFT JOIN player_features_career AS pfc1
  ON pfc1.player_id = pr.player_one_id
LEFT JOIN player_features_career AS pfc2
  ON pfc2.player_id = pr.player_two_id
LEFT JOIN player_features_recent AS pfr1
  ON pfr1.player_id = pr.player_one_id
LEFT JOIN player_features_recent AS pfr2
  ON pfr2.player_id = pr.player_two_id
""".strip()


def publish_team_performance_features(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    features_config: dict[str, Any],
    evidence_windows_config: dict[str, Any],
) -> TeamPerformanceFeaturesPublicationSummary:
    """Build and publish team_performance_features using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "team_performance_features")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "team_performance_features")
    publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_team_performance_features_sql(
            environment,
            analysis_as_of_date=analysis_as_of_date,
            features_config=features_config,
            evidence_windows_config=evidence_windows_config,
        ),
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("team_id", "evidence_window"),
            label="team_performance_features",
        ),
    )
    input_row_count = int(spark.table(get_silver_source_table_fqn(environment, "teams")).count()) * 4
    output_row_count = int(spark.table(target_table_fqn).count())
    return TeamPerformanceFeaturesPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=input_row_count,
        output_row_count=output_row_count,
    )


def publish_partnership_effectiveness(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    evidence_windows_config: dict[str, Any],
) -> PartnershipEffectivenessPublicationSummary:
    """Build and publish partnership_effectiveness using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "partnership_effectiveness")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "partnership_effectiveness")
    publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_partnership_effectiveness_sql(
            environment,
            analysis_as_of_date=analysis_as_of_date,
            evidence_windows_config=evidence_windows_config,
        ),
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("partnership_key",),
            label="partnership_effectiveness",
        ),
    )
    summary_row = spark.sql(
        f"""
SELECT COUNT(*) AS partnership_count
FROM (
    SELECT DISTINCT
        COALESCE(CAST(resolved_team_id AS STRING), CAST(canonical_player_pair_key AS STRING)) AS partnership_key
    FROM {get_gold_target_table_fqn(environment, "resolved_match_teams")}
    WHERE canonical_player_pair_key IS NOT NULL
      AND match_date IS NOT NULL
      AND CAST(match_date AS DATE) <= DATE('{analysis_as_of_date.isoformat()}')
)
""".strip()
    ).collect()[0]
    mapping = summary_row.asDict(recursive=True) if hasattr(summary_row, "asDict") else dict(summary_row)
    input_row_count = int(mapping["partnership_count"] or 0)
    output_row_count = int(spark.table(target_table_fqn).count())
    return PartnershipEffectivenessPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=input_row_count,
        output_row_count=output_row_count,
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
    if int(mapping["null_key_count"] or 0) != 0 or int(mapping["duplicate_group_count"] or 0) != 0:
        raise ValueError(
            f"{label} validation failed for {table_fqn}: "
            f"null_key_count={int(mapping['null_key_count'] or 0)}, "
            f"duplicate_group_count={int(mapping['duplicate_group_count'] or 0)}."
        )
