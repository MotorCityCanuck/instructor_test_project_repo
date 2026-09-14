"""Post-Gold Olympic team-set selection from governed team scorecards."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ModuleNotFoundError:  # pragma: no cover - local test fallback
    class _FallbackType:
        pass

    class DateType(_FallbackType):
        pass

    class DoubleType(_FallbackType):
        pass

    class IntegerType(_FallbackType):
        pass

    class StringType(_FallbackType):
        pass

    class TimestampType(_FallbackType):
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
)
from napa_pipeline.silver_to_gold.publish import publish_stage_to_gold_table


REQUIRED_GROUPS = (
    ("USA", "MENS"),
    ("USA", "WOMENS"),
    ("USA", "MIXED"),
    ("CAN", "MENS"),
    ("CAN", "WOMENS"),
    ("CAN", "MIXED"),
)
REQUIRED_SOURCE_COLUMNS = (
    "team_id",
    "scoring_scenario",
    "analysis_as_of_date",
    "team_category",
    "country_code",
    "eligible_team_flag",
    "final_team_selection_score",
    "combined_team_confidence",
    "regional_adjustment",
    "age_adjustment",
    "fatigue_adjustment",
)


class OlympicTeamSelectionError(ValueError):
    """Raised when a governed Gold selection cannot be completed."""


@dataclass(frozen=True)
class OlympicTeamSelectionsPublicationSummary:
    """Published-table summary for olympic_team_selections."""

    source_table_fqn: str
    stage_table_fqn: str
    target_table_fqn: str
    input_row_count: int
    output_row_count: int


OLYMPIC_TEAM_SELECTIONS_SCHEMA = StructType(
    [
        StructField("dataset_source", StringType(), False),
        StructField("selection_run_id", StringType(), False),
        StructField("selection_timestamp", TimestampType(), False),
        StructField("selection_set_number", IntegerType(), False),
        StructField("selection_rank", IntegerType(), False),
        StructField("country_code", StringType(), False),
        StructField("division", StringType(), False),
        StructField("team_number", StringType(), False),
        StructField("source_gold_rank", IntegerType(), True),
        StructField("selection_score", DoubleType(), False),
        StructField("regional_adjustment", DoubleType(), True),
        StructField("age_adjustment", DoubleType(), True),
        StructField("fatigue_adjustment", DoubleType(), True),
        StructField("confidence_metric", DoubleType(), True),
        StructField("supporting_match_count", IntegerType(), True),
        StructField("selection_reason", StringType(), False),
        StructField("source_team_id", StringType(), False),
        StructField("scoring_scenario", StringType(), False),
        StructField("analysis_as_of_date", DateType(), True),
    ]
)


def parse_team_set_count(value: str | int) -> int:
    """Parse and validate the required positive team-set count."""
    if isinstance(value, bool):
        raise OlympicTeamSelectionError("team_set_count must be a positive integer.")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.strip().isdigit():
        parsed = int(value.strip())
    else:
        raise OlympicTeamSelectionError("team_set_count must be a positive integer.")
    if parsed < 1:
        raise OlympicTeamSelectionError("team_set_count must be a positive integer.")
    return parsed


def validate_gold_source_contract(spark: Any, environment: ReleaseEnvironment) -> str:
    """Validate the governed scorecard contract required by this module."""
    source_table_fqn = get_gold_target_table_fqn(environment, "team_selection_scorecards")
    try:
        actual_columns = {field.name for field in spark.table(source_table_fqn).schema.fields}
    except Exception as exc:
        raise OlympicTeamSelectionError(
            f"Selection failed: required Gold table '{source_table_fqn}' is unavailable."
        ) from exc

    missing_columns = sorted(set(REQUIRED_SOURCE_COLUMNS) - actual_columns)
    if missing_columns:
        raise OlympicTeamSelectionError(
            f"Selection failed: Gold table '{source_table_fqn}' is missing required columns: "
            f"{', '.join(missing_columns)}. The Olympic team-selection specification requires "
            "governed regional, age, and fatigue adjustments; add them to the upstream Gold "
            "scorecard contract before running this module."
        )
    return source_table_fqn


def validate_preselection_counts(
    group_counts: dict[tuple[str, str], int],
    *,
    dataset_source: str,
    team_set_count: int,
) -> None:
    """Require enough eligible scorecard rows in every mandated group."""
    unexpected_groups = sorted(set(group_counts) - set(REQUIRED_GROUPS))
    if unexpected_groups:
        raise OlympicTeamSelectionError(
            f"Selection failed: unexpected country/division groups in Gold input: {unexpected_groups}."
        )
    for country_code, division in REQUIRED_GROUPS:
        available = group_counts.get((country_code, division), 0)
        if available < team_set_count:
            raise OlympicTeamSelectionError(
                f"Selection failed: {dataset_source} / {country_code} / {division} requested "
                f"{team_set_count} teams but only {available} eligible ranked teams are available."
            )


def build_olympic_team_selections_sql(
    environment: ReleaseEnvironment,
    *,
    dataset_source: str,
    team_set_count: int,
    selection_run_id: str,
    scoring_scenario: str,
) -> str:
    """Return deterministic SQL selecting top governed scorecards by required group."""
    scorecards_fqn = get_gold_target_table_fqn(environment, "team_selection_scorecards")
    groups_sql = ", ".join(
        f"('{country_code}', '{division}')" for country_code, division in REQUIRED_GROUPS
    )
    return f"""
WITH required_groups AS (
    SELECT * FROM VALUES {groups_sql} AS required_groups(country_code, division)
), eligible_scorecards AS (
    SELECT
        scorecard.team_id,
        scorecard.scoring_scenario,
        scorecard.analysis_as_of_date,
        scorecard.country_code,
        scorecard.team_category AS division,
        scorecard.final_team_selection_score AS selection_score,
        scorecard.regional_adjustment,
        scorecard.age_adjustment,
        scorecard.fatigue_adjustment,
        scorecard.combined_team_confidence AS confidence_metric
    FROM {scorecards_fqn} AS scorecard
    INNER JOIN required_groups AS required_group
        ON scorecard.country_code = required_group.country_code
       AND scorecard.team_category = required_group.division
    WHERE scorecard.scoring_scenario = '{scoring_scenario}'
      AND scorecard.eligible_team_flag
      AND scorecard.team_id IS NOT NULL
      AND scorecard.final_team_selection_score IS NOT NULL
), ranked_scorecards AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY country_code, division
            ORDER BY selection_score DESC,
                     confidence_metric DESC NULLS LAST,
                     team_id ASC
        ) AS selection_rank
    FROM eligible_scorecards
)
SELECT
    '{dataset_source}' AS dataset_source,
    '{selection_run_id}' AS selection_run_id,
    current_timestamp() AS selection_timestamp,
    selection_rank AS selection_set_number,
    selection_rank,
    country_code,
    division,
    team_id AS team_number,
    CAST(NULL AS INT) AS source_gold_rank,
    selection_score,
    regional_adjustment,
    age_adjustment,
    fatigue_adjustment,
    confidence_metric,
    CAST(NULL AS INT) AS supporting_match_count,
    CONCAT(
        'Ranked #', CAST(selection_rank AS STRING), ' among ', country_code, ' ', division,
        ' teams using the governed Gold final selection score with regional, age, and fatigue adjustments.'
    ) AS selection_reason,
    team_id AS source_team_id,
    scoring_scenario,
    analysis_as_of_date
FROM ranked_scorecards
WHERE selection_rank <= {team_set_count}
""".strip()


def get_preselection_group_counts(
    spark: Any,
    source_table_fqn: str,
    *,
    scoring_scenario: str,
) -> dict[tuple[str, str], int]:
    """Return eligible counts for the six required groups without deriving any teams."""
    required_groups_sql = ", ".join(
        f"('{country_code}', '{division}')" for country_code, division in REQUIRED_GROUPS
    )
    rows = spark.sql(
        f"""
SELECT
    country_code,
    team_category AS division,
    COUNT(*) AS eligible_count
FROM {source_table_fqn}
WHERE scoring_scenario = '{scoring_scenario}'
  AND eligible_team_flag
  AND team_id IS NOT NULL
  AND final_team_selection_score IS NOT NULL
  AND (country_code, team_category) IN ({required_groups_sql})
GROUP BY country_code, team_category
""".strip()
    ).collect()
    return {
        (str(row["country_code"]), str(row["division"])): int(row["eligible_count"])
        for row in rows
    }


def validate_selection_rows(
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    team_set_count: int,
) -> None:
    """Validate output shape, ranks, and source-team traceability before promotion."""
    expected_total = team_set_count * len(REQUIRED_GROUPS)
    if len(rows) != expected_total:
        raise OlympicTeamSelectionError(
            f"Selection failed: expected {expected_total} output rows but produced {len(rows)}."
        )

    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        country_code = str(row.get("country_code") or "")
        division = str(row.get("division") or "")
        team_number = row.get("team_number")
        source_team_id = row.get("source_team_id")
        if not team_number or team_number != source_team_id:
            raise OlympicTeamSelectionError(
                "Selection failed: every selected team must have a non-null canonical team number "
                "that traces to its Gold scorecard."
            )
        grouped_rows.setdefault((country_code, division), []).append(row)

    if set(grouped_rows) != set(REQUIRED_GROUPS):
        raise OlympicTeamSelectionError("Selection failed: output does not contain exactly the six required groups.")
    for group, group_rows in grouped_rows.items():
        ranks = sorted(int(row["selection_rank"]) for row in group_rows)
        team_numbers = [str(row["team_number"]) for row in group_rows]
        if len(group_rows) != team_set_count or ranks != list(range(1, team_set_count + 1)):
            raise OlympicTeamSelectionError(
                f"Selection failed: {group[0]} / {group[1]} does not contain ranks 1 through {team_set_count}."
            )
        if len(set(team_numbers)) != len(team_numbers):
            raise OlympicTeamSelectionError(
                f"Selection failed: {group[0]} / {group[1]} contains duplicate team numbers."
            )


def publish_olympic_team_selections(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    dataset_source: str,
    team_set_count: int,
    selection_run_id: str,
    scoring_scenario: str,
) -> OlympicTeamSelectionsPublicationSummary:
    """Stage, validate, and publish one reproducible selection output for a release."""
    source_table_fqn = validate_gold_source_contract(spark, environment)
    group_counts = get_preselection_group_counts(
        spark, source_table_fqn, scoring_scenario=scoring_scenario
    )
    validate_preselection_counts(
        group_counts, dataset_source=dataset_source, team_set_count=team_set_count
    )
    stage_table_fqn = get_gold_stage_table_fqn(environment, "olympic_team_selections")
    target_table_fqn = get_gold_target_table_fqn(environment, "olympic_team_selections")
    selection_sql = build_olympic_team_selections_sql(
        environment,
        dataset_source=dataset_source,
        team_set_count=team_set_count,
        selection_run_id=selection_run_id,
        scoring_scenario=scoring_scenario,
    )

    def validate_stage(stage_spark: Any, stage_fqn: str) -> None:
        stage_rows = [row.asDict(recursive=True) for row in stage_spark.table(stage_fqn).collect()]
        validate_selection_rows(stage_rows, team_set_count=team_set_count)

    input_row_count, output_row_count = publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=selection_sql,
        validation_fn=validate_stage,
    )
    return OlympicTeamSelectionsPublicationSummary(
        source_table_fqn=source_table_fqn,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        input_row_count=input_row_count,
        output_row_count=output_row_count,
    )
