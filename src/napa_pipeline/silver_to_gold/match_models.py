"""Phase 9 match-outcome modeling builders for the Silver-to-Gold pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_stage_table_fqn,
    get_gold_target_table_fqn,
)
from napa_pipeline.silver_to_gold.publish import (
    publish_stage_records_to_gold_table,
    publish_stage_to_gold_table,
)


BASELINE_ALGORITHM = "ANALYTICAL_RATING_PROBABILITY"
DEFAULT_RATING_SCALE = 400.0
PREDICTION_EXPLANATION = (
    "Baseline analytical rating probability from pre-match team ratings."
)


@dataclass(frozen=True)
class MatchOutcomeTrainingSetPublicationSummary:
    """Published-table summary for match_outcome_training_set."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class MatchOutcomePredictionsPublicationSummary:
    """Published-table summary for match_outcome_predictions."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class MatchModelMetricsPublicationSummary:
    """Published-table summary for match_model_metrics."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int
    metric_records: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class Phase9PublicationSummary:
    """Published-table summary for all Phase 9 target tables."""

    training_set: MatchOutcomeTrainingSetPublicationSummary
    predictions: MatchOutcomePredictionsPublicationSummary
    metrics: MatchModelMetricsPublicationSummary


def build_match_outcome_training_set_sql(
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    models_config: dict[str, Any],
    rating_scale: float = DEFAULT_RATING_SCALE,
) -> str:
    """Return the Spark SQL used to build match_outcome_training_set."""
    sides_fqn = get_gold_target_table_fqn(environment, "competition_match_sides")
    team_features_fqn = get_gold_target_table_fqn(environment, "team_performance_features")
    train_fraction = float(models_config["train_fraction"])
    analysis_date_literal = analysis_as_of_date.isoformat()

    return f"""
WITH valid_match_sides AS (
    SELECT
        CAST(match_id AS STRING) AS match_id,
        CAST(match_team_id AS STRING) AS match_team_id,
        CAST(match_date AS DATE) AS match_date,
        CAST(batch_id AS STRING) AS batch_id,
        CAST(batch_sequence AS INT) AS batch_sequence,
        CAST(batch_date AS DATE) AS batch_date,
        CAST(region_id AS STRING) AS region_id,
        UPPER(TRIM(CAST(match_country_code AS STRING))) AS match_country_code,
        UPPER(TRIM(CAST(match_type AS STRING))) AS match_type,
        UPPER(TRIM(CAST(competition_category AS STRING))) AS competition_category,
        CAST(team_number AS INT) AS team_number,
        CAST(team_id AS STRING) AS team_id,
        CAST(winning_team_number AS INT) AS winning_team_number,
        CAST(completed_flag AS BOOLEAN) AS completed_flag,
        CAST(pre_match_team_rating AS DOUBLE) AS pre_match_team_rating
    FROM {sides_fqn}
    WHERE CAST(match_date AS DATE) IS NOT NULL
      AND CAST(match_date AS DATE) <= DATE('{analysis_date_literal}')
      AND CAST(winning_team_number AS INT) IN (1, 2)
      AND COALESCE(CAST(completed_flag AS BOOLEAN), FALSE) = TRUE
),
team_features_recent AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        CAST(recent_form_win_pct AS DOUBLE) AS recent_form_win_pct
    FROM {team_features_fqn}
    WHERE evidence_window = 'trailing_90'
),
team_features_career AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        CAST(shrinkage_adjusted_win_rate AS DOUBLE) AS shrinkage_adjusted_win_rate,
        CAST(strength_of_schedule AS DOUBLE) AS strength_of_schedule,
        CAST(partnership_duration_days AS DOUBLE) AS partnership_duration_days,
        CAST(consistency_score AS DOUBLE) AS consistency_score,
        CAST(evidence_reliability_score AS DOUBLE) AS evidence_reliability_score,
        CAST(feature_evidence_status AS STRING) AS feature_evidence_status
    FROM {team_features_fqn}
    WHERE evidence_window = 'career'
),
canonical_matches AS (
    SELECT
        side_one.match_id,
        side_one.match_date,
        side_one.batch_id,
        side_one.batch_sequence,
        side_one.batch_date,
        side_one.region_id,
        side_one.match_country_code,
        side_one.match_type,
        side_one.competition_category,
        side_one.match_team_id AS team_a_match_team_id,
        side_two.match_team_id AS team_b_match_team_id,
        side_one.team_id AS team_a_team_id,
        side_two.team_id AS team_b_team_id,
        side_one.pre_match_team_rating AS team_a_pre_match_team_rating,
        side_two.pre_match_team_rating AS team_b_pre_match_team_rating,
        side_one.winning_team_number AS actual_winner_team_number,
        side_one.winning_team_number = 1 AS team_a_win_flag,
        side_one.winning_team_number = 2 AS team_b_win_flag,
        (
            1.0 / (
                1.0 + POWER(
                    10.0,
                    (
                        COALESCE(side_two.pre_match_team_rating, 1500.0)
                        - COALESCE(side_one.pre_match_team_rating, 1500.0)
                    ) / {float(rating_scale)}
                )
            )
        ) AS rating_expected_probability,
        side_one.pre_match_team_rating - side_two.pre_match_team_rating AS team_rating_difference,
        recent_one.recent_form_win_pct - recent_two.recent_form_win_pct AS recent_form_difference,
        career_one.shrinkage_adjusted_win_rate - career_two.shrinkage_adjusted_win_rate
            AS adjusted_win_rate_difference,
        career_one.strength_of_schedule - career_two.strength_of_schedule
            AS strength_of_schedule_difference,
        career_one.partnership_duration_days - career_two.partnership_duration_days
            AS partnership_continuity_difference,
        career_one.consistency_score - career_two.consistency_score
            AS consistency_score_difference,
        career_one.evidence_reliability_score - career_two.evidence_reliability_score
            AS rating_reliability_difference,
        career_one.feature_evidence_status AS team_a_feature_evidence_status,
        career_two.feature_evidence_status AS team_b_feature_evidence_status
    FROM valid_match_sides AS side_one
    INNER JOIN valid_match_sides AS side_two
      ON side_one.match_id = side_two.match_id
     AND side_one.team_number = 1
     AND side_two.team_number = 2
    LEFT JOIN team_features_recent AS recent_one
      ON recent_one.team_id = side_one.team_id
    LEFT JOIN team_features_recent AS recent_two
      ON recent_two.team_id = side_two.team_id
    LEFT JOIN team_features_career AS career_one
      ON career_one.team_id = side_one.team_id
    LEFT JOIN team_features_career AS career_two
      ON career_two.team_id = side_two.team_id
),
ordered_matches AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            ORDER BY match_date, COALESCE(batch_sequence, 2147483647), match_id
        ) AS ordered_match_number,
        COUNT(*) OVER () AS total_match_count
    FROM canonical_matches
)
SELECT
    match_id,
    match_date,
    batch_id,
    batch_sequence,
    batch_date,
    region_id,
    match_country_code,
    match_type,
    competition_category,
    team_a_match_team_id,
    team_b_match_team_id,
    1 AS team_a_team_number,
    2 AS team_b_team_number,
    team_a_team_id,
    team_b_team_id,
    team_a_pre_match_team_rating,
    team_b_pre_match_team_rating,
    rating_expected_probability,
    team_rating_difference,
    recent_form_difference,
    adjusted_win_rate_difference,
    strength_of_schedule_difference,
    partnership_continuity_difference,
    consistency_score_difference,
    rating_reliability_difference,
    actual_winner_team_number,
    team_a_win_flag,
    team_b_win_flag,
    team_a_feature_evidence_status,
    team_b_feature_evidence_status,
    CASE
        WHEN ordered_match_number <= GREATEST(1, CAST(total_match_count * {train_fraction} AS BIGINT))
            THEN 'train'
        ELSE 'validation'
    END AS split_name
FROM ordered_matches
""".strip()


def build_match_outcome_predictions_sql(
    environment: ReleaseEnvironment,
    *,
    model_run_id: str,
    model_name: str,
    model_version: str,
    algorithm: str = BASELINE_ALGORITHM,
) -> str:
    """Return the Spark SQL used to build match_outcome_predictions."""
    training_fqn = get_gold_target_table_fqn(environment, "match_outcome_training_set")
    escaped_explanation = PREDICTION_EXPLANATION.replace("'", "''")

    return f"""
SELECT
    CAST(match_id AS STRING) AS match_id,
    CAST(match_date AS DATE) AS match_date,
    CAST(batch_id AS STRING) AS batch_id,
    CAST(batch_sequence AS INT) AS batch_sequence,
    CAST(batch_date AS DATE) AS batch_date,
    CAST(region_id AS STRING) AS region_id,
    UPPER(TRIM(CAST(match_country_code AS STRING))) AS match_country_code,
    UPPER(TRIM(CAST(match_type AS STRING))) AS match_type,
    UPPER(TRIM(CAST(competition_category AS STRING))) AS competition_category,
    CAST(split_name AS STRING) AS split_name,
    '{model_run_id}' AS model_run_id,
    '{model_name}' AS model_name,
    '{model_version}' AS model_version,
    '{algorithm}' AS algorithm,
    CAST(team_a_match_team_id AS STRING) AS team_a_match_team_id,
    CAST(team_b_match_team_id AS STRING) AS team_b_match_team_id,
    CAST(team_a_team_id AS STRING) AS team_a_team_id,
    CAST(team_b_team_id AS STRING) AS team_b_team_id,
    CAST(team_a_team_number AS INT) AS team_a_team_number,
    CAST(team_b_team_number AS INT) AS team_b_team_number,
    CAST(team_a_pre_match_team_rating AS DOUBLE) AS team_a_pre_match_team_rating,
    CAST(team_b_pre_match_team_rating AS DOUBLE) AS team_b_pre_match_team_rating,
    CAST(rating_expected_probability AS DOUBLE) AS rating_expected_probability,
    CAST(rating_expected_probability AS DOUBLE) AS model_predicted_probability,
    CAST(team_rating_difference AS DOUBLE) AS team_rating_difference,
    CAST(recent_form_difference AS DOUBLE) AS recent_form_difference,
    CAST(adjusted_win_rate_difference AS DOUBLE) AS adjusted_win_rate_difference,
    CAST(strength_of_schedule_difference AS DOUBLE) AS strength_of_schedule_difference,
    CAST(partnership_continuity_difference AS DOUBLE) AS partnership_continuity_difference,
    CAST(consistency_score_difference AS DOUBLE) AS consistency_score_difference,
    CAST(rating_reliability_difference AS DOUBLE) AS rating_reliability_difference,
    CAST(actual_winner_team_number AS INT) AS actual_winner_team_number,
    CAST(team_a_win_flag AS BOOLEAN) AS team_a_win_flag,
    CASE
        WHEN COALESCE(rating_expected_probability, 0.5) >= 0.5 THEN team_a_team_number
        ELSE team_b_team_number
    END AS predicted_winner_team_number,
    CASE
        WHEN COALESCE(rating_expected_probability, 0.5) >= 0.5 THEN team_a_team_id
        ELSE team_b_team_id
    END AS predicted_winner_team_id,
    CASE
        WHEN actual_winner_team_number = 1 THEN team_a_team_id
        ELSE team_b_team_id
    END AS actual_winner_team_id,
    CASE
        WHEN (
            CASE
                WHEN COALESCE(rating_expected_probability, 0.5) >= 0.5 THEN team_a_team_number
                ELSE team_b_team_number
            END
        ) = actual_winner_team_number THEN TRUE
        ELSE FALSE
    END AS prediction_correct_flag,
    '{escaped_explanation}' AS prediction_explanation
FROM {training_fqn}
""".strip()


def publish_phase9_modeling_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    models_config: dict[str, Any],
    model_run_id: str,
    model_name: str,
    model_version: str,
    feature_definition_version: str,
) -> Phase9PublicationSummary:
    """Publish the Gold Phase 9 training, prediction, and metrics tables."""
    training_summary = publish_match_outcome_training_set(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        models_config=models_config,
    )
    predictions_summary = publish_match_outcome_predictions(
        spark,
        environment,
        model_run_id=model_run_id,
        model_name=model_name,
        model_version=model_version,
    )
    metrics_summary = publish_match_model_metrics(
        spark,
        environment,
        model_run_id=model_run_id,
        model_name=model_name,
        model_version=model_version,
        feature_definition_version=feature_definition_version,
    )
    return Phase9PublicationSummary(
        training_set=training_summary,
        predictions=predictions_summary,
        metrics=metrics_summary,
    )


def publish_match_outcome_training_set(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    models_config: dict[str, Any],
) -> MatchOutcomeTrainingSetPublicationSummary:
    """Build and publish match_outcome_training_set using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "match_outcome_training_set")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "match_outcome_training_set")
    input_row_count = _count_distinct_matches_from_competition_sides(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
    )
    publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_match_outcome_training_set_sql(
            environment,
            analysis_as_of_date=analysis_as_of_date,
            models_config=models_config,
        ),
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("match_id",),
            label="match_outcome_training_set",
        ),
    )
    output_row_count = int(spark.table(target_table_fqn).count())
    return MatchOutcomeTrainingSetPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=input_row_count,
        output_row_count=output_row_count,
    )


def publish_match_outcome_predictions(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    model_run_id: str,
    model_name: str,
    model_version: str,
) -> MatchOutcomePredictionsPublicationSummary:
    """Build and publish match_outcome_predictions using the baseline model."""
    training_fqn = get_gold_target_table_fqn(environment, "match_outcome_training_set")
    target_table_fqn = get_gold_target_table_fqn(environment, "match_outcome_predictions")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "match_outcome_predictions")
    input_row_count = int(spark.table(training_fqn).count())
    publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_match_outcome_predictions_sql(
            environment,
            model_run_id=model_run_id,
            model_name=model_name,
            model_version=model_version,
        ),
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("match_id",),
            label="match_outcome_predictions",
        ),
    )
    output_row_count = int(spark.table(target_table_fqn).count())
    return MatchOutcomePredictionsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=input_row_count,
        output_row_count=output_row_count,
    )


def publish_match_model_metrics(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    model_run_id: str,
    model_name: str,
    model_version: str,
    feature_definition_version: str,
) -> MatchModelMetricsPublicationSummary:
    """Build and publish match_model_metrics from published predictions."""
    target_table_fqn = get_gold_target_table_fqn(environment, "match_model_metrics")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "match_model_metrics")
    metric_records = build_match_model_metric_records(
        spark,
        environment,
        model_run_id=model_run_id,
        model_name=model_name,
        model_version=model_version,
        feature_definition_version=feature_definition_version,
    )
    publish_stage_records_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        records=metric_records,
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("model_run_id", "split_name", "metric_name"),
            label="match_model_metrics",
        ),
    )
    output_row_count = int(spark.table(target_table_fqn).count())
    return MatchModelMetricsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=len(metric_records),
        output_row_count=output_row_count,
        metric_records=tuple(metric_records),
    )


def build_match_model_metric_records(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    model_run_id: str,
    model_name: str,
    model_version: str,
    feature_definition_version: str,
) -> list[dict[str, Any]]:
    """Return metric records computed from published match_outcome_predictions."""
    predictions_fqn = get_gold_target_table_fqn(environment, "match_outcome_predictions")
    prediction_rows = [
        row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        for row in spark.sql(
            f"""
SELECT
    split_name,
    model_predicted_probability,
    team_a_win_flag
FROM {predictions_fqn}
""".strip()
        ).collect()
    ]

    metrics_records: list[dict[str, Any]] = []
    for split_name in ("train", "validation"):
        split_rows = [row for row in prediction_rows if str(row.get("split_name")) == split_name]
        metrics_records.extend(
            _build_split_metric_records(
                split_rows,
                model_run_id=model_run_id,
                model_name=model_name,
                model_version=model_version,
                feature_definition_version=feature_definition_version,
                split_name=split_name,
            )
        )
    return metrics_records


def _build_split_metric_records(
    split_rows: list[dict[str, Any]],
    *,
    model_run_id: str,
    model_name: str,
    model_version: str,
    feature_definition_version: str,
    split_name: str,
) -> list[dict[str, Any]]:
    probabilities = [_clip_probability(_coerce_float(row.get("model_predicted_probability"))) for row in split_rows]
    outcomes = [1 if _coerce_bool(row.get("team_a_win_flag")) else 0 for row in split_rows]
    evaluated_row_count = len(split_rows)
    if evaluated_row_count == 0:
        base_metrics = {
            "accuracy": None,
            "precision": None,
            "recall": None,
            "f1": None,
            "roc_auc": None,
            "log_loss": None,
            "brier_score": None,
        }
    else:
        predicted_flags = [1 if probability >= 0.5 else 0 for probability in probabilities]
        true_positive = sum(1 for predicted, actual in zip(predicted_flags, outcomes) if predicted == 1 and actual == 1)
        false_positive = sum(1 for predicted, actual in zip(predicted_flags, outcomes) if predicted == 1 and actual == 0)
        false_negative = sum(1 for predicted, actual in zip(predicted_flags, outcomes) if predicted == 0 and actual == 1)
        true_negative = sum(1 for predicted, actual in zip(predicted_flags, outcomes) if predicted == 0 and actual == 0)
        precision = true_positive / (true_positive + false_positive) if (true_positive + false_positive) else 0.0
        recall = true_positive / (true_positive + false_negative) if (true_positive + false_negative) else 0.0
        f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        base_metrics = {
            "accuracy": (true_positive + true_negative) / evaluated_row_count,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "roc_auc": calculate_roc_auc(probabilities, outcomes),
            "log_loss": calculate_log_loss(probabilities, outcomes),
            "brier_score": calculate_brier_score(probabilities, outcomes),
        }

    records = [
        {
            "model_run_id": model_run_id,
            "model_name": model_name,
            "model_version": model_version,
            "algorithm": BASELINE_ALGORITHM,
            "feature_definition_version": feature_definition_version,
            "split_name": split_name,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "evaluated_row_count": evaluated_row_count,
            "evaluation_window": split_name,
        }
        for metric_name, metric_value in base_metrics.items()
    ]

    records.extend(
        {
            "model_run_id": model_run_id,
            "model_name": model_name,
            "model_version": model_version,
            "algorithm": BASELINE_ALGORITHM,
            "feature_definition_version": feature_definition_version,
            "split_name": split_name,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "evaluated_row_count": evaluated_count,
            "evaluation_window": split_name,
        }
        for metric_name, metric_value, evaluated_count in calculate_calibration_band_metrics(
            probabilities,
            outcomes,
        )
    )
    return records


def calculate_brier_score(probabilities: list[float], outcomes: list[int]) -> float | None:
    """Return the Brier score for a binary prediction set."""
    if not probabilities:
        return None
    return sum((probability - actual) ** 2 for probability, actual in zip(probabilities, outcomes)) / len(probabilities)


def calculate_log_loss(probabilities: list[float], outcomes: list[int]) -> float | None:
    """Return the clipped binary log loss for a prediction set."""
    if not probabilities:
        return None
    return -sum(
        actual * math.log(probability) + (1 - actual) * math.log(1.0 - probability)
        for probability, actual in zip(probabilities, outcomes)
    ) / len(probabilities)


def calculate_roc_auc(probabilities: list[float], outcomes: list[int]) -> float | None:
    """Return ROC AUC using an average-rank Mann-Whitney calculation."""
    positive_count = sum(outcomes)
    negative_count = len(outcomes) - positive_count
    if positive_count == 0 or negative_count == 0:
        return None

    ordered = sorted(zip(probabilities, outcomes), key=lambda item: item[0])
    rank_sum_for_positive = 0.0
    index = 0
    while index < len(ordered):
        tie_end = index
        while tie_end < len(ordered) and ordered[tie_end][0] == ordered[index][0]:
            tie_end += 1
        average_rank = ((index + 1) + tie_end) / 2.0
        positive_in_tie = sum(actual for _probability, actual in ordered[index:tie_end])
        rank_sum_for_positive += average_rank * positive_in_tie
        index = tie_end

    return (
        rank_sum_for_positive - (positive_count * (positive_count + 1) / 2.0)
    ) / (positive_count * negative_count)


def calculate_calibration_band_metrics(
    probabilities: list[float],
    outcomes: list[int],
) -> list[tuple[str, float | None, int]]:
    """Return per-band calibration gap metrics."""
    bands = (
        ("00_20", 0.0, 0.2),
        ("20_40", 0.2, 0.4),
        ("40_60", 0.4, 0.6),
        ("60_80", 0.6, 0.8),
        ("80_100", 0.8, 1.0000001),
    )
    results: list[tuple[str, float | None, int]] = []
    for band_name, lower_bound, upper_bound in bands:
        indices = [
            index
            for index, probability in enumerate(probabilities)
            if lower_bound <= probability < upper_bound
        ]
        if not indices:
            results.append((f"calibration_gap_band_{band_name}", None, 0))
            continue
        average_probability = sum(probabilities[index] for index in indices) / len(indices)
        average_outcome = sum(outcomes[index] for index in indices) / len(indices)
        results.append(
            (
                f"calibration_gap_band_{band_name}",
                average_probability - average_outcome,
                len(indices),
            )
        )
    return results


def _count_distinct_matches_from_competition_sides(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
) -> int:
    sides_fqn = get_gold_target_table_fqn(environment, "competition_match_sides")
    query = f"""
SELECT COUNT(*) AS match_count
FROM (
    SELECT DISTINCT CAST(match_id AS STRING) AS match_id
    FROM {sides_fqn}
    WHERE CAST(match_date AS DATE) IS NOT NULL
      AND CAST(match_date AS DATE) <= DATE('{analysis_as_of_date.isoformat()}')
      AND CAST(winning_team_number AS INT) IN (1, 2)
      AND COALESCE(CAST(completed_flag AS BOOLEAN), FALSE) = TRUE
)
""".strip()
    row = spark.sql(query).collect()[0]
    mapping = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    return int(mapping["match_count"] or 0)


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


def _coerce_bool(value: Any) -> bool:
    return bool(value)


def _coerce_float(value: Any) -> float:
    return float(value or 0.0)


def _clip_probability(value: float) -> float:
    return min(max(float(value), 1e-15), 1.0 - 1e-15)
