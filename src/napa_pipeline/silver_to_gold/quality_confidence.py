"""Phase 8 entity data-quality confidence builders for the Silver-to-Gold pipeline."""

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


HIGH_CONFIDENCE = "HIGH"
MODERATE_CONFIDENCE = "MODERATE"
LOW_CONFIDENCE = "LOW"
CRITICAL_CONFIDENCE = "CRITICAL"


@dataclass(frozen=True)
class EntityDataQualityConfidencePublicationSummary:
    """Published-table summary for entity_data_quality_confidence."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


def calculate_weighted_confidence_score(
    components: dict[str, float | None],
    component_weights: dict[str, float | int],
) -> float:
    """Return a 0-100 weighted confidence score from component scores."""
    total = 0.0
    for component_name, raw_weight in component_weights.items():
        component_value = float(components.get(component_name) or 0.0)
        total += component_value * float(raw_weight)
    return round(total, 4)


def quality_confidence_band_for_score(
    score: float,
    band_thresholds: dict[str, float | int],
    *,
    critical_issue_count: int = 0,
) -> str:
    """Return the configured confidence band for a score."""
    if critical_issue_count > 0:
        return CRITICAL_CONFIDENCE
    if score >= float(band_thresholds["high"]):
        return HIGH_CONFIDENCE
    if score >= float(band_thresholds["moderate"]):
        return MODERATE_CONFIDENCE
    if score >= float(band_thresholds["low"]):
        return LOW_CONFIDENCE
    return CRITICAL_CONFIDENCE


def build_entity_data_quality_confidence_sql(
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    quality_rules_config: dict[str, Any],
) -> str:
    """Return the Spark SQL used to build entity_data_quality_confidence."""
    players_fqn = get_silver_source_table_fqn(environment, "players")
    player_perf_fqn = get_gold_target_table_fqn(environment, "player_performance_features")
    player_dev_fqn = get_gold_target_table_fqn(environment, "player_development_features")
    player_ratings_fqn = get_gold_target_table_fqn(environment, "player_current_ratings")
    player_matches_fqn = get_gold_target_table_fqn(environment, "competition_player_matches")
    resolved_fqn = get_gold_target_table_fqn(environment, "resolved_match_teams")
    teams_fqn = get_silver_source_table_fqn(environment, "teams")
    team_memberships_fqn = get_silver_source_table_fqn(environment, "team_memberships")
    team_perf_fqn = get_gold_target_table_fqn(environment, "team_performance_features")
    team_sides_fqn = get_gold_target_table_fqn(environment, "competition_match_sides")
    analysis_date_literal = analysis_as_of_date.isoformat()
    minimum_match_count = int(quality_rules_config["minimum_match_count_for_confidence"])
    minimum_recent_match_count = int(quality_rules_config["minimum_recent_match_count"])
    weights = quality_rules_config["confidence_component_weights"]
    bands = quality_rules_config["confidence_band_thresholds"]

    player_weighted_score_sql = _weighted_score_sql("player_components", weights)
    team_weighted_score_sql = _weighted_score_sql("team_components", weights)

    return f"""
WITH player_perf_career AS (
    SELECT
        CAST(player_id AS STRING) AS player_id,
        CAST(match_count AS DOUBLE) AS career_match_count
    FROM {player_perf_fqn}
    WHERE evidence_window = 'career'
),
player_perf_recent AS (
    SELECT
        CAST(player_id AS STRING) AS player_id,
        CAST(match_count AS DOUBLE) AS recent_match_count
    FROM {player_perf_fqn}
    WHERE evidence_window = 'trailing_90'
),
player_development AS (
    SELECT
        CAST(player_id AS STRING) AS player_id,
        CAST(current_registration_flag AS BOOLEAN) AS current_registration_flag,
        CAST(latest_assessment_confidence AS DOUBLE) AS latest_assessment_confidence
    FROM {player_dev_fqn}
),
player_ratings AS (
    SELECT
        CAST(player_id AS STRING) AS player_id,
        CAST(analytical_rating_value AS DOUBLE) AS analytical_rating_value,
        CAST(rated_match_count AS DOUBLE) AS rated_match_count
    FROM {player_ratings_fqn}
),
player_match_quality AS (
    SELECT
        CAST(player_id AS STRING) AS player_id,
        COUNT(*) AS total_player_matches,
        AVG(
            CASE
                WHEN won_flag IS NOT NULL
                 AND lost_flag IS NOT NULL
                 AND partner_player_id IS NOT NULL
                    THEN 1.0
                ELSE 0.0
            END
        ) AS player_match_structure_ratio,
        AVG(
            CASE
                WHEN games_won IS NOT NULL
                 AND games_lost IS NOT NULL
                 AND point_share IS NOT NULL
                 AND point_differential IS NOT NULL
                    THEN 1.0
                ELSE 0.0
            END
        ) AS player_game_score_ratio,
        MAX(CASE WHEN COALESCE(CAST(membership_history_warning_flag AS BOOLEAN), FALSE) THEN 1 ELSE 0 END)
            AS player_membership_warning_int
    FROM {player_matches_fqn}
    GROUP BY CAST(player_id AS STRING)
),
player_resolution_quality AS (
    SELECT
        CAST(cpm.player_id AS STRING) AS player_id,
        AVG(CASE WHEN rmt.resolved_team_id IS NOT NULL THEN 1.0 ELSE 0.0 END) AS player_resolution_ratio
    FROM {player_matches_fqn} AS cpm
    LEFT JOIN {resolved_fqn} AS rmt
      ON CAST(cpm.match_id AS STRING) = CAST(rmt.match_id AS STRING)
     AND CAST(cpm.match_team_id AS STRING) = CAST(rmt.match_team_id AS STRING)
    GROUP BY CAST(cpm.player_id AS STRING)
),
player_components AS (
    SELECT
        'PLAYER' AS entity_type,
        CAST(p.player_id AS STRING) AS entity_id,
        DATE('{analysis_date_literal}') AS analysis_as_of_date,
        CAST(p.display_name AS STRING) AS display_label,
        UPPER(TRIM(CAST(p.country_code AS STRING))) AS country_code,
        CAST(p.active_flag AS BOOLEAN) AS active_flag,
        CAST(NULL AS BOOLEAN) AS candidate_attribution_allowed_flag,
        ROUND(
            100.0 * (
                CASE WHEN p.display_name IS NOT NULL AND TRIM(CAST(p.display_name AS STRING)) <> '' THEN 0.5 ELSE 0.0 END
                + CASE WHEN p.country_code IS NOT NULL AND TRIM(CAST(p.country_code AS STRING)) <> '' THEN 0.5 ELSE 0.0 END
            ),
            4
        ) AS identity_integrity,
        CASE
            WHEN pd.current_registration_flag IS TRUE THEN 100.0
            WHEN pd.current_registration_flag IS FALSE THEN 50.0
            ELSE 0.0
        END AS relationship_integrity,
        ROUND(100.0 * COALESCE(pmq.player_match_structure_ratio, 0.0), 4) AS match_structure_integrity,
        ROUND(100.0 * COALESCE(pmq.player_game_score_ratio, 0.0), 4) AS game_score_integrity,
        CASE
            WHEN pr.analytical_rating_value IS NULL THEN 0.0
            WHEN COALESCE(pr.rated_match_count, 0.0) > 0 THEN 100.0
            ELSE 40.0
        END AS rating_coverage,
        ROUND(
            100.0 * LEAST(COALESCE(ppc.career_match_count, 0.0) / CAST({minimum_match_count} AS DOUBLE), 1.0),
            4
        ) AS match_volume_coverage,
        ROUND(
            100.0 * LEAST(COALESCE(ppr.recent_match_count, 0.0) / CAST({minimum_recent_match_count} AS DOUBLE), 1.0),
            4
        ) AS recency_coverage,
        ROUND(100.0 * COALESCE(prq.player_resolution_ratio, 0.0), 4) AS team_resolution_coverage,
        ROUND(
            100.0 * (
                CASE WHEN p.display_name IS NOT NULL AND TRIM(CAST(p.display_name AS STRING)) <> '' THEN 0.25 ELSE 0.0 END
                + CASE WHEN p.country_code IS NOT NULL AND TRIM(CAST(p.country_code AS STRING)) <> '' THEN 0.25 ELSE 0.0 END
                + CASE WHEN pd.current_registration_flag IS NOT NULL THEN 0.25 ELSE 0.0 END
                + CASE WHEN pd.latest_assessment_confidence IS NOT NULL THEN 0.25 ELSE 0.0 END
            ),
            4
        ) AS source_data_quality_score,
        CASE
            WHEN (
                (CASE WHEN p.display_name IS NOT NULL AND TRIM(CAST(p.display_name AS STRING)) <> '' THEN 0.5 ELSE 0.0 END
                 + CASE WHEN p.country_code IS NOT NULL AND TRIM(CAST(p.country_code AS STRING)) <> '' THEN 0.5 ELSE 0.0 END) * 100.0
            ) < 100.0 THEN 1 ELSE 0
        END
        + CASE WHEN pd.current_registration_flag IS NULL THEN 1 ELSE 0 END
        + CASE
            WHEN COALESCE(ppc.career_match_count, 0.0) > 0.0
             AND COALESCE(prq.player_resolution_ratio, 0.0) < 0.5 THEN 1
            ELSE 0
        END AS critical_quality_issue_count,
        CASE
            WHEN COALESCE(ppc.career_match_count, 0.0) > 0.0
             AND COALESCE(ppc.career_match_count, 0.0) < {minimum_match_count} THEN 1
            ELSE 0
        END
        + CASE WHEN COALESCE(ppr.recent_match_count, 0.0) < {minimum_recent_match_count} THEN 1 ELSE 0 END
        + CASE WHEN pd.latest_assessment_confidence IS NULL THEN 1 ELSE 0 END
        + CASE WHEN COALESCE(pmq.player_membership_warning_int, 0) = 1 THEN 1 ELSE 0 END AS warning_quality_issue_count,
        CONCAT_WS(
            '; ',
            CASE
                WHEN p.display_name IS NULL OR TRIM(CAST(p.display_name AS STRING)) = ''
                  OR p.country_code IS NULL OR TRIM(CAST(p.country_code AS STRING)) = ''
                    THEN 'incomplete player identity'
            END,
            CASE WHEN pd.current_registration_flag IS NULL THEN 'missing registration evidence' END,
            CASE
                WHEN COALESCE(ppc.career_match_count, 0.0) > 0.0
                 AND COALESCE(ppc.career_match_count, 0.0) < {minimum_match_count}
                    THEN 'limited match volume'
            END,
            CASE WHEN COALESCE(ppr.recent_match_count, 0.0) < {minimum_recent_match_count} THEN 'stale recent activity' END,
            CASE
                WHEN COALESCE(ppc.career_match_count, 0.0) > 0.0
                 AND COALESCE(prq.player_resolution_ratio, 0.0) < 1.0
                    THEN 'partial historical team resolution'
            END,
            CASE WHEN pd.latest_assessment_confidence IS NULL THEN 'missing assessment confidence' END,
            CASE WHEN COALESCE(pmq.player_membership_warning_int, 0) = 1 THEN 'membership history warning present' END
        ) AS material_limitation_text
    FROM {players_fqn} AS p
    LEFT JOIN player_perf_career AS ppc
      ON CAST(p.player_id AS STRING) = ppc.player_id
    LEFT JOIN player_perf_recent AS ppr
      ON CAST(p.player_id AS STRING) = ppr.player_id
    LEFT JOIN player_development AS pd
      ON CAST(p.player_id AS STRING) = pd.player_id
    LEFT JOIN player_ratings AS pr
      ON CAST(p.player_id AS STRING) = pr.player_id
    LEFT JOIN player_match_quality AS pmq
      ON CAST(p.player_id AS STRING) = pmq.player_id
    LEFT JOIN player_resolution_quality AS prq
      ON CAST(p.player_id AS STRING) = prq.player_id
    WHERE p.player_id IS NOT NULL
),
team_perf_career AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        CAST(candidate_attribution_allowed_flag AS BOOLEAN) AS candidate_attribution_allowed_flag,
        CAST(match_count AS DOUBLE) AS career_match_count
    FROM {team_perf_fqn}
    WHERE evidence_window = 'career'
),
team_perf_recent AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        CAST(match_count AS DOUBLE) AS recent_match_count
    FROM {team_perf_fqn}
    WHERE evidence_window = 'trailing_90'
),
team_current_membership AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        COUNT(DISTINCT CASE WHEN COALESCE(CAST(current_membership_flag AS BOOLEAN), FALSE) THEN CAST(player_id AS STRING) END)
            AS current_member_count,
        MAX(CASE WHEN COALESCE(CAST(membership_overlap_flag AS BOOLEAN), FALSE) THEN 1 ELSE 0 END)
            AS membership_overlap_warning_int
    FROM {team_memberships_fqn}
    WHERE team_id IS NOT NULL
    GROUP BY CAST(team_id AS STRING)
),
team_side_quality AS (
    SELECT
        CAST(cms.team_id AS STRING) AS team_id,
        AVG(
            CASE
                WHEN rmt.player_one_id IS NOT NULL
                 AND rmt.player_two_id IS NOT NULL
                    THEN 1.0
                ELSE 0.0
            END
        ) AS team_match_structure_ratio,
        AVG(
            CASE
                WHEN cms.games_won IS NOT NULL
                 AND cms.games_lost IS NOT NULL
                 AND cms.point_share IS NOT NULL
                 AND cms.point_differential IS NOT NULL
                    THEN 1.0
                ELSE 0.0
            END
        ) AS team_game_score_ratio,
        AVG(
            CASE
                WHEN cms.pre_match_team_rating IS NOT NULL
                 AND cms.opponent_pre_match_team_rating IS NOT NULL
                    THEN 1.0
                ELSE 0.0
            END
        ) AS team_rating_coverage_ratio,
        MAX(CASE WHEN COALESCE(CAST(cms.membership_history_warning_flag AS BOOLEAN), FALSE) THEN 1 ELSE 0 END)
            AS team_membership_history_warning_int
    FROM {team_sides_fqn} AS cms
    LEFT JOIN {resolved_fqn} AS rmt
      ON CAST(cms.match_id AS STRING) = CAST(rmt.match_id AS STRING)
     AND CAST(cms.match_team_id AS STRING) = CAST(rmt.match_team_id AS STRING)
    WHERE cms.team_id IS NOT NULL
    GROUP BY CAST(cms.team_id AS STRING)
),
team_resolution_quality AS (
    SELECT
        CAST(cms.team_id AS STRING) AS team_id,
        AVG(
            CASE
                WHEN CAST(rmt.resolved_team_id AS STRING) = CAST(cms.team_id AS STRING) THEN 1.0
                ELSE 0.0
            END
        ) AS team_resolution_ratio
    FROM {team_sides_fqn} AS cms
    LEFT JOIN {resolved_fqn} AS rmt
      ON CAST(cms.match_id AS STRING) = CAST(rmt.match_id AS STRING)
     AND CAST(cms.match_team_id AS STRING) = CAST(rmt.match_team_id AS STRING)
    WHERE cms.team_id IS NOT NULL
    GROUP BY CAST(cms.team_id AS STRING)
),
team_components AS (
    SELECT
        'TEAM' AS entity_type,
        CAST(t.team_id AS STRING) AS entity_id,
        DATE('{analysis_date_literal}') AS analysis_as_of_date,
        CONCAT('TEAM ', CAST(t.team_id AS STRING)) AS display_label,
        UPPER(TRIM(CAST(t.country_code AS STRING))) AS country_code,
        CAST(t.active_flag AS BOOLEAN) AS active_flag,
        CAST(tpc.candidate_attribution_allowed_flag AS BOOLEAN) AS candidate_attribution_allowed_flag,
        ROUND(
            100.0 * (
                CASE WHEN t.team_category IS NOT NULL AND TRIM(CAST(t.team_category AS STRING)) <> '' THEN 0.3333333333 ELSE 0.0 END
                + CASE WHEN t.country_code IS NOT NULL AND TRIM(CAST(t.country_code AS STRING)) <> '' THEN 0.3333333333 ELSE 0.0 END
                + CASE WHEN t.team_status IS NOT NULL AND TRIM(CAST(t.team_status AS STRING)) <> '' THEN 0.3333333334 ELSE 0.0 END
            ),
            4
        ) AS identity_integrity,
        CASE
            WHEN COALESCE(tcm.current_member_count, 0) = 2 AND COALESCE(tcm.membership_overlap_warning_int, 0) = 0 THEN 100.0
            WHEN COALESCE(tcm.current_member_count, 0) = 2 THEN 60.0
            WHEN COALESCE(tcm.current_member_count, 0) > 0 THEN 40.0
            ELSE 0.0
        END AS relationship_integrity,
        ROUND(100.0 * COALESCE(tsq.team_match_structure_ratio, 0.0), 4) AS match_structure_integrity,
        ROUND(100.0 * COALESCE(tsq.team_game_score_ratio, 0.0), 4) AS game_score_integrity,
        ROUND(100.0 * COALESCE(tsq.team_rating_coverage_ratio, 0.0), 4) AS rating_coverage,
        ROUND(
            100.0 * LEAST(COALESCE(tpc.career_match_count, 0.0) / CAST({minimum_match_count} AS DOUBLE), 1.0),
            4
        ) AS match_volume_coverage,
        ROUND(
            100.0 * LEAST(COALESCE(tpr.recent_match_count, 0.0) / CAST({minimum_recent_match_count} AS DOUBLE), 1.0),
            4
        ) AS recency_coverage,
        ROUND(100.0 * COALESCE(trq.team_resolution_ratio, 0.0), 4) AS team_resolution_coverage,
        ROUND(
            100.0 * (
                CASE WHEN t.team_category IS NOT NULL AND TRIM(CAST(t.team_category AS STRING)) <> '' THEN 0.25 ELSE 0.0 END
                + CASE WHEN t.country_code IS NOT NULL AND TRIM(CAST(t.country_code AS STRING)) <> '' THEN 0.25 ELSE 0.0 END
                + CASE WHEN t.team_status IS NOT NULL AND TRIM(CAST(t.team_status AS STRING)) <> '' THEN 0.25 ELSE 0.0 END
                + CASE WHEN t.active_flag IS NOT NULL THEN 0.25 ELSE 0.0 END
            ),
            4
        ) AS source_data_quality_score,
        CASE
            WHEN (
                (CASE WHEN t.team_category IS NOT NULL AND TRIM(CAST(t.team_category AS STRING)) <> '' THEN 0.3333333333 ELSE 0.0 END
                 + CASE WHEN t.country_code IS NOT NULL AND TRIM(CAST(t.country_code AS STRING)) <> '' THEN 0.3333333333 ELSE 0.0 END
                 + CASE WHEN t.team_status IS NOT NULL AND TRIM(CAST(t.team_status AS STRING)) <> '' THEN 0.3333333334 ELSE 0.0 END) * 100.0
            ) < 100.0 THEN 1 ELSE 0
        END
        + CASE WHEN COALESCE(tcm.current_member_count, 0) = 0 THEN 1 ELSE 0 END
        + CASE
            WHEN COALESCE(tpc.career_match_count, 0.0) > 0.0
             AND COALESCE(trq.team_resolution_ratio, 0.0) < 0.5 THEN 1
            ELSE 0
        END AS critical_quality_issue_count,
        CASE
            WHEN COALESCE(tpc.career_match_count, 0.0) > 0.0
             AND COALESCE(tpc.career_match_count, 0.0) < {minimum_match_count} THEN 1
            ELSE 0
        END
        + CASE WHEN COALESCE(tpr.recent_match_count, 0.0) < {minimum_recent_match_count} THEN 1 ELSE 0 END
        + CASE WHEN COALESCE(tcm.membership_overlap_warning_int, 0) = 1 THEN 1 ELSE 0 END
        + CASE WHEN COALESCE(tsq.team_membership_history_warning_int, 0) = 1 THEN 1 ELSE 0 END AS warning_quality_issue_count,
        CONCAT_WS(
            '; ',
            CASE
                WHEN t.team_category IS NULL OR TRIM(CAST(t.team_category AS STRING)) = ''
                  OR t.country_code IS NULL OR TRIM(CAST(t.country_code AS STRING)) = ''
                  OR t.team_status IS NULL OR TRIM(CAST(t.team_status AS STRING)) = ''
                    THEN 'incomplete team identity'
            END,
            CASE WHEN COALESCE(tcm.current_member_count, 0) = 0 THEN 'missing current roster evidence' END,
            CASE
                WHEN COALESCE(tcm.current_member_count, 0) > 0
                 AND COALESCE(tcm.current_member_count, 0) <> 2
                    THEN 'non-standard current roster size'
            END,
            CASE
                WHEN COALESCE(tpc.career_match_count, 0.0) > 0.0
                 AND COALESCE(tpc.career_match_count, 0.0) < {minimum_match_count}
                    THEN 'limited match volume'
            END,
            CASE WHEN COALESCE(tpr.recent_match_count, 0.0) < {minimum_recent_match_count} THEN 'stale recent activity' END,
            CASE
                WHEN COALESCE(tpc.career_match_count, 0.0) > 0.0
                 AND COALESCE(trq.team_resolution_ratio, 0.0) < 1.0
                    THEN 'partial historical team resolution'
            END,
            CASE WHEN COALESCE(tcm.membership_overlap_warning_int, 0) = 1 THEN 'membership overlap warning present' END,
            CASE WHEN COALESCE(tsq.team_membership_history_warning_int, 0) = 1 THEN 'match-side membership warning present' END,
            CASE
                WHEN COALESCE(tpc.candidate_attribution_allowed_flag, FALSE) = FALSE
                    THEN 'not currently candidate attributable'
            END
        ) AS material_limitation_text
    FROM {teams_fqn} AS t
    LEFT JOIN team_perf_career AS tpc
      ON CAST(t.team_id AS STRING) = tpc.team_id
    LEFT JOIN team_perf_recent AS tpr
      ON CAST(t.team_id AS STRING) = tpr.team_id
    LEFT JOIN team_current_membership AS tcm
      ON CAST(t.team_id AS STRING) = tcm.team_id
    LEFT JOIN team_side_quality AS tsq
      ON CAST(t.team_id AS STRING) = tsq.team_id
    LEFT JOIN team_resolution_quality AS trq
      ON CAST(t.team_id AS STRING) = trq.team_id
    WHERE t.team_id IS NOT NULL
),
player_final AS (
    SELECT
        entity_type,
        entity_id,
        analysis_as_of_date,
        display_label,
        country_code,
        active_flag,
        candidate_attribution_allowed_flag,
        identity_integrity,
        relationship_integrity,
        match_structure_integrity,
        game_score_integrity,
        rating_coverage,
        match_volume_coverage,
        recency_coverage,
        team_resolution_coverage,
        source_data_quality_score,
        ROUND({player_weighted_score_sql}, 4) AS data_quality_confidence_score,
        CASE
            WHEN critical_quality_issue_count > 0 THEN '{CRITICAL_CONFIDENCE}'
            WHEN ROUND({player_weighted_score_sql}, 4) >= {float(bands["high"])} THEN '{HIGH_CONFIDENCE}'
            WHEN ROUND({player_weighted_score_sql}, 4) >= {float(bands["moderate"])} THEN '{MODERATE_CONFIDENCE}'
            WHEN ROUND({player_weighted_score_sql}, 4) >= {float(bands["low"])} THEN '{LOW_CONFIDENCE}'
            ELSE '{CRITICAL_CONFIDENCE}'
        END AS quality_confidence_band,
        critical_quality_issue_count,
        warning_quality_issue_count,
        material_limitation_text
    FROM player_components
),
team_final AS (
    SELECT
        entity_type,
        entity_id,
        analysis_as_of_date,
        display_label,
        country_code,
        active_flag,
        candidate_attribution_allowed_flag,
        identity_integrity,
        relationship_integrity,
        match_structure_integrity,
        game_score_integrity,
        rating_coverage,
        match_volume_coverage,
        recency_coverage,
        team_resolution_coverage,
        source_data_quality_score,
        ROUND({team_weighted_score_sql}, 4) AS data_quality_confidence_score,
        CASE
            WHEN critical_quality_issue_count > 0 THEN '{CRITICAL_CONFIDENCE}'
            WHEN ROUND({team_weighted_score_sql}, 4) >= {float(bands["high"])} THEN '{HIGH_CONFIDENCE}'
            WHEN ROUND({team_weighted_score_sql}, 4) >= {float(bands["moderate"])} THEN '{MODERATE_CONFIDENCE}'
            WHEN ROUND({team_weighted_score_sql}, 4) >= {float(bands["low"])} THEN '{LOW_CONFIDENCE}'
            ELSE '{CRITICAL_CONFIDENCE}'
        END AS quality_confidence_band,
        critical_quality_issue_count,
        warning_quality_issue_count,
        material_limitation_text
    FROM team_components
)
SELECT * FROM player_final
UNION ALL
SELECT * FROM team_final
""".strip()


def publish_entity_data_quality_confidence(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    quality_rules_config: dict[str, Any],
) -> EntityDataQualityConfidencePublicationSummary:
    """Build and publish entity_data_quality_confidence using Spark-native SQL."""
    players_fqn = get_silver_source_table_fqn(environment, "players")
    teams_fqn = get_silver_source_table_fqn(environment, "teams")
    target_table_fqn = get_gold_target_table_fqn(environment, "entity_data_quality_confidence")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "entity_data_quality_confidence")
    publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_entity_data_quality_confidence_sql(
            environment,
            analysis_as_of_date=analysis_as_of_date,
            quality_rules_config=quality_rules_config,
        ),
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("entity_type", "entity_id"),
            label="entity_data_quality_confidence",
        ),
    )
    input_row_count = int(spark.table(players_fqn).count()) + int(spark.table(teams_fqn).count())
    output_row_count = int(spark.table(target_table_fqn).count())
    return EntityDataQualityConfidencePublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=input_row_count,
        output_row_count=output_row_count,
    )


def _weighted_score_sql(
    alias_name: str,
    component_weights: dict[str, Any],
) -> str:
    return " + ".join(
        f"COALESCE({alias_name}.{component_name}, 0.0) * {float(raw_weight)}"
        for component_name, raw_weight in component_weights.items()
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
