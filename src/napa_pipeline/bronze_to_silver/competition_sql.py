"""Spark SQL plans for Bronze-to-Silver competition-table execution."""

from __future__ import annotations

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.operations import PipelineContext
from napa_pipeline.bronze_to_silver.reference_sql import SqlReferenceBuildPlan, sql_literal


def supports_competition_sql_transform(transform_name: str) -> bool:
    """Return whether a competition transform has a Spark SQL execution plan."""
    return transform_name in {
        "build_matches",
        "build_match_teams",
        "build_match_team_players",
        "build_match_games",
    }


def build_competition_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    target_table: str,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    """Return the SQL execution plan for one supported competition table."""
    if target_table == "matches":
        return _build_matches_sql_plan(context, source_table_fqn, silver_schema_fqn)
    if target_table == "match_teams":
        return _build_match_teams_sql_plan(config, context, source_table_fqn, silver_schema_fqn)
    if target_table == "match_team_players":
        return _build_match_team_players_sql_plan(config, context, source_table_fqn, silver_schema_fqn)
    if target_table == "match_games":
        return _build_match_games_sql_plan(config, context, source_table_fqn, silver_schema_fqn)
    raise ValueError(f"No SQL competition plan is defined for target table '{target_table}'.")


def _build_matches_sql_plan(
    context: PipelineContext,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    monthly_batches_fqn = f"{silver_schema_fqn}.monthly_batches"
    regions_fqn = f"{silver_schema_fqn}.regions"
    metadata_sql = _metadata_sql(
        context,
        source_table="matches",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(match_id, '<NULL>'), "
            "coalesce(batch_id, '<NULL>'), coalesce(region_id, '<NULL>'), "
            "coalesce(cast(match_date as string), '<NULL>'), "
            "coalesce(cast(winning_team_number as string), '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH normalized_source AS (
    SELECT
        NULLIF(TRIM(CAST(COALESCE(match_id, id) AS STRING)), '') AS match_id,
        NULLIF(TRIM(CAST(batch_id AS STRING)), '') AS batch_id,
        NULLIF(TRIM(CAST(region_id AS STRING)), '') AS region_id,
        TRIM(CAST(COALESCE(match_date, date) AS STRING)) AS match_date_raw,
        NULLIF(UPPER(TRIM(CAST(match_type AS STRING))), '') AS match_type,
        NULLIF(UPPER(TRIM(CAST(COALESCE(competition_category, category) AS STRING))), '') AS competition_category,
        NULLIF(UPPER(TRIM(CAST(COALESCE(match_status, status) AS STRING))), '') AS match_status,
        TRIM(CAST(COALESCE(winning_team_number, winner_team_number) AS STRING)) AS winning_team_number_raw
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT *
    FROM normalized_source
),
typed_source AS (
    SELECT
        source.*,
        TO_DATE(source.match_date_raw) AS match_date,
        CAST(source.winning_team_number_raw AS INT) AS winning_team_number
    FROM deduped_source source
),
validated_source AS (
    SELECT
        source.*,
        batch.batch_sk,
        region.region_sk,
        CASE
            WHEN source.match_status IN ('COMPLETED', 'FINAL') OR source.winning_team_number IS NOT NULL THEN true
            ELSE false
        END AS completed_flag
    FROM typed_source source
    LEFT JOIN {monthly_batches_fqn} batch
        ON source.batch_id = batch.batch_id
    LEFT JOIN {regions_fqn} region
        ON source.region_id = region.region_id
),
invalid_rows AS (
    SELECT
        'matches' AS source_table,
        'matches' AS target_table,
        COALESCE(match_id, '<NULL>') AS source_business_key,
        CASE
            WHEN match_id IS NULL THEN 'MISSING_PRIMARY_KEY'
            WHEN batch_id IS NOT NULL AND batch_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN region_id IS NOT NULL AND region_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN match_date_raw IS NOT NULL AND match_date_raw <> '' AND match_date IS NULL THEN 'INVALID_DATE'
            WHEN winning_team_number_raw IS NOT NULL AND winning_team_number_raw <> ''
                 AND (winning_team_number IS NULL OR winning_team_number NOT IN (1, 2)) THEN 'VALUE_OUT_OF_RANGE'
            WHEN completed_flag = true AND winning_team_number IS NULL THEN 'VALUE_OUT_OF_RANGE'
            WHEN match_status IN ('CANCELLED', 'FORFEITED') AND completed_flag = true THEN 'VALUE_OUT_OF_RANGE'
        END AS reject_reason,
        CASE
            WHEN match_id IS NULL THEN 'MATCH_001'
            WHEN batch_id IS NOT NULL AND batch_sk IS NULL THEN 'MATCH_002'
            WHEN region_id IS NOT NULL AND region_sk IS NULL THEN 'MATCH_003'
            WHEN match_date_raw IS NOT NULL AND match_date_raw <> '' AND match_date IS NULL THEN 'MATCH_004'
            WHEN winning_team_number_raw IS NOT NULL AND winning_team_number_raw <> ''
                 AND (winning_team_number IS NULL OR winning_team_number NOT IN (1, 2)) THEN 'MATCH_005'
            WHEN completed_flag = true AND winning_team_number IS NULL THEN 'MATCH_006'
            WHEN match_status IN ('CANCELLED', 'FORFEITED') AND completed_flag = true THEN 'MATCH_007'
        END AS rule_id,
        CASE
            WHEN match_id IS NULL THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN match_id IS NULL THEN 'match_id could not be resolved.'
            WHEN batch_id IS NOT NULL AND batch_sk IS NULL THEN concat('batch_id ''', batch_id, ''' was not found in accepted monthly_batches.')
            WHEN region_id IS NOT NULL AND region_sk IS NULL THEN concat('region_id ''', region_id, ''' was not found in accepted regions.')
            WHEN match_date_raw IS NOT NULL AND match_date_raw <> '' AND match_date IS NULL THEN concat('Invalid match_date value ''', match_date_raw, '''.')
            WHEN winning_team_number_raw IS NOT NULL AND winning_team_number_raw <> ''
                 AND (winning_team_number IS NULL OR winning_team_number NOT IN (1, 2)) THEN 'winning_team_number must be 1 or 2.'
            WHEN completed_flag = true AND winning_team_number IS NULL THEN 'Completed matches require a winning_team_number.'
            WHEN match_status IN ('CANCELLED', 'FORFEITED') AND completed_flag = true THEN 'Cancelled or forfeited matches cannot be completed.'
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'match_id', match_id,
                'batch_id', batch_id,
                'region_id', region_id,
                'match_date_raw', match_date_raw,
                'match_type', match_type,
                'competition_category', competition_category,
                'match_status', match_status,
                'winning_team_number_raw', winning_team_number_raw
            )
        ) AS source_record_json
    FROM validated_source
    WHERE match_id IS NULL
       OR (batch_id IS NOT NULL AND batch_sk IS NULL)
       OR (region_id IS NOT NULL AND region_sk IS NULL)
       OR (match_date_raw IS NOT NULL AND match_date_raw <> '' AND match_date IS NULL)
       OR (
            winning_team_number_raw IS NOT NULL AND winning_team_number_raw <> ''
            AND (winning_team_number IS NULL OR winning_team_number NOT IN (1, 2))
       )
       OR (completed_flag = true AND winning_team_number IS NULL)
       OR (match_status IN ('CANCELLED', 'FORFEITED') AND completed_flag = true)
),
valid_rows AS (
    SELECT
        match_id,
        batch_id,
        batch_sk,
        region_id,
        region_sk,
        match_date,
        match_type,
        competition_category,
        match_status,
        winning_team_number,
        completed_flag
    FROM validated_source
    WHERE match_id IS NOT NULL
      AND NOT (
          (batch_id IS NOT NULL AND batch_sk IS NULL)
          OR (region_id IS NOT NULL AND region_sk IS NULL)
          OR (match_date_raw IS NOT NULL AND match_date_raw <> '' AND match_date IS NULL)
          OR (
                winning_team_number_raw IS NOT NULL AND winning_team_number_raw <> ''
                AND (winning_team_number IS NULL OR winning_team_number NOT IN (1, 2))
          )
          OR (completed_flag = true AND winning_team_number IS NULL)
          OR (match_status IN ('CANCELLED', 'FORFEITED') AND completed_flag = true)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_id
            ORDER BY
                (
                    CASE WHEN batch_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN region_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN match_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN match_type IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN competition_category IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN match_status IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN winning_team_number IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(match_id, '<NULL>'),
                        coalesce(batch_id, '<NULL>'),
                        coalesce(region_id, '<NULL>'),
                        coalesce(cast(match_date as string), '<NULL>'),
                        coalesce(match_type, '<NULL>'),
                        coalesce(competition_category, '<NULL>'),
                        coalesce(match_status, '<NULL>'),
                        coalesce(cast(winning_team_number as string), '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
)
""".strip()

    accepted_sql = f"""
{base_ctes}
SELECT
    match_id,
    sha2(coalesce(match_id, '<NULL>'), 256) AS match_sk,
    batch_id,
    batch_sk,
    region_id,
    region_sk,
    match_date,
    match_type,
    competition_category,
    match_status,
    winning_team_number,
    completed_flag,
    YEAR(match_date) AS match_year,
    MONTH(match_date) AS match_month,
    {metadata_sql}
FROM ranked_rows
WHERE duplicate_rank = 1
""".strip()

    rejected_sql = f"""
{base_ctes}
SELECT
    source_table,
    target_table,
    source_business_key,
    reject_reason,
    reject_reason AS reject_reason_code,
    reject_reason_detail,
    rule_id,
    rule_severity,
    pipeline_run_id,
    _pipeline_run_id,
    _source_dataset,
    load_ts,
    _load_ts,
    source_record_json,
    sha2(source_record_json, 256) AS _record_hash
FROM invalid_rows
UNION ALL
SELECT
    'matches' AS source_table,
    'matches' AS target_table,
    match_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'MATCH_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'match_id', match_id,
            'batch_id', batch_id,
            'region_id', region_id,
            'match_date', match_date,
            'match_type', match_type,
            'competition_category', competition_category,
            'match_status', match_status,
            'winning_team_number', winning_team_number
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'match_id', match_id,
                'batch_id', batch_id,
                'region_id', region_id,
                'match_date', match_date,
                'match_type', match_type,
                'competition_category', competition_category,
                'match_status', match_status,
                'winning_team_number', winning_team_number
            )
        ),
        256
    ) AS _record_hash
FROM ranked_rows
WHERE duplicate_rank > 1
""".strip()

    return SqlReferenceBuildPlan(
        accepted_sql=accepted_sql,
        rejected_sql=rejected_sql,
        bronze_row_count_sql=f"SELECT COUNT(*) AS value FROM {source_table_fqn}",
        exact_duplicate_count_sql=_exact_duplicate_count_sql(
            source_table_fqn,
            [
                "NULLIF(TRIM(CAST(COALESCE(match_id, id) AS STRING)), '') AS match_id",
                "NULLIF(TRIM(CAST(batch_id AS STRING)), '') AS batch_id",
                "NULLIF(TRIM(CAST(region_id AS STRING)), '') AS region_id",
                "TRIM(CAST(COALESCE(match_date, date) AS STRING)) AS match_date_raw",
                "NULLIF(UPPER(TRIM(CAST(match_type AS STRING))), '') AS match_type",
                "NULLIF(UPPER(TRIM(CAST(COALESCE(competition_category, category) AS STRING))), '') AS competition_category",
                "NULLIF(UPPER(TRIM(CAST(COALESCE(match_status, status) AS STRING))), '') AS match_status",
                "TRIM(CAST(COALESCE(winning_team_number, winner_team_number) AS STRING)) AS winning_team_number_raw",
            ],
        ),
        business_key_duplicate_count_sql=f"""
WITH valid_rows AS (
    SELECT DISTINCT
        source.match_id,
        source.batch_id,
        batch.batch_sk,
        source.region_id,
        region.region_sk,
        TO_DATE(source.match_date_raw) AS match_date,
        source.match_type,
        source.competition_category,
        source.match_status,
        CAST(source.winning_team_number_raw AS INT) AS winning_team_number,
        CASE
            WHEN source.match_status IN ('COMPLETED', 'FINAL')
                 OR CAST(source.winning_team_number_raw AS INT) IS NOT NULL THEN true
            ELSE false
        END AS completed_flag
    FROM (
        SELECT
            NULLIF(TRIM(CAST(COALESCE(match_id, id) AS STRING)), '') AS match_id,
            NULLIF(TRIM(CAST(batch_id AS STRING)), '') AS batch_id,
            NULLIF(TRIM(CAST(region_id AS STRING)), '') AS region_id,
            TRIM(CAST(COALESCE(match_date, date) AS STRING)) AS match_date_raw,
            NULLIF(UPPER(TRIM(CAST(match_type AS STRING))), '') AS match_type,
            NULLIF(UPPER(TRIM(CAST(COALESCE(competition_category, category) AS STRING))), '') AS competition_category,
            NULLIF(UPPER(TRIM(CAST(COALESCE(match_status, status) AS STRING))), '') AS match_status,
            TRIM(CAST(COALESCE(winning_team_number, winner_team_number) AS STRING)) AS winning_team_number_raw
        FROM {source_table_fqn}
    ) source
    LEFT JOIN {monthly_batches_fqn} batch
        ON source.batch_id = batch.batch_id
    LEFT JOIN {regions_fqn} region
        ON source.region_id = region.region_id
    WHERE source.match_id IS NOT NULL
      AND NOT (
          (source.batch_id IS NOT NULL AND batch.batch_sk IS NULL)
          OR (source.region_id IS NOT NULL AND region.region_sk IS NULL)
          OR (source.match_date_raw IS NOT NULL AND source.match_date_raw <> '' AND TO_DATE(source.match_date_raw) IS NULL)
          OR (
                source.winning_team_number_raw IS NOT NULL AND source.winning_team_number_raw <> ''
                AND (CAST(source.winning_team_number_raw AS INT) IS NULL OR CAST(source.winning_team_number_raw AS INT) NOT IN (1, 2))
          )
          OR (
                CASE
                    WHEN source.match_status IN ('COMPLETED', 'FINAL')
                         OR CAST(source.winning_team_number_raw AS INT) IS NOT NULL THEN true
                    ELSE false
                END = true
                AND CAST(source.winning_team_number_raw AS INT) IS NULL
          )
          OR (
                source.match_status IN ('CANCELLED', 'FORFEITED')
                AND (
                    CASE
                        WHEN source.match_status IN ('COMPLETED', 'FINAL')
                             OR CAST(source.winning_team_number_raw AS INT) IS NOT NULL THEN true
                        ELSE false
                    END = true
                )
          )
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_id
            ORDER BY
                (
                    CASE WHEN batch_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN region_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN match_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN match_type IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN competition_category IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN match_status IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN winning_team_number IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(match_id, '<NULL>'),
                        coalesce(batch_id, '<NULL>'),
                        coalesce(region_id, '<NULL>'),
                        coalesce(cast(match_date as string), '<NULL>'),
                        coalesce(match_type, '<NULL>'),
                        coalesce(competition_category, '<NULL>'),
                        coalesce(match_status, '<NULL>'),
                        coalesce(cast(winning_team_number as string), '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
)
SELECT COUNT(*) AS value
FROM ranked_rows
WHERE duplicate_rank > 1
""".strip(),
    )


def _build_match_teams_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    matches_fqn = f"{silver_schema_fqn}.matches"
    teams_fqn = f"{silver_schema_fqn}.teams"
    expected_match_team_count = int(config.data["thresholds"]["expected_match_team_count"])
    metadata_sql = _metadata_sql(
        context,
        source_table="match_teams",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(match_team_id, '<NULL>'), "
            "coalesce(match_id, '<NULL>'), coalesce(team_id, '<NULL>'), "
            "coalesce(cast(team_number as string), '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH normalized_source AS (
    SELECT
        NULLIF(TRIM(CAST(COALESCE(match_team_id, id) AS STRING)), '') AS match_team_id,
        NULLIF(TRIM(CAST(match_id AS STRING)), '') AS match_id,
        NULLIF(TRIM(CAST(team_id AS STRING)), '') AS team_id,
        TRIM(CAST(COALESCE(team_number, side_number) AS STRING)) AS team_number_raw,
        NULLIF(TRIM(CAST(COALESCE(pre_match_team_rating, team_rating_before) AS STRING)), '') AS pre_match_team_rating_raw,
        NULLIF(TRIM(CAST(COALESCE(post_match_team_rating, team_rating_after) AS STRING)), '') AS post_match_team_rating_raw
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT *
    FROM normalized_source
),
typed_source AS (
    SELECT
        source.*,
        CAST(source.team_number_raw AS INT) AS team_number,
        CAST(source.pre_match_team_rating_raw AS DOUBLE) AS pre_match_team_rating,
        CAST(source.post_match_team_rating_raw AS DOUBLE) AS post_match_team_rating
    FROM deduped_source source
),
validated_source AS (
    SELECT
        source.*,
        match.match_sk,
        match.match_date,
        match.winning_team_number,
        team.team_sk
    FROM typed_source source
    LEFT JOIN {matches_fqn} match
        ON source.match_id = match.match_id
    LEFT JOIN {teams_fqn} team
        ON source.team_id = team.team_id
),
invalid_rows AS (
    SELECT
        'match_teams' AS source_table,
        'match_teams' AS target_table,
        COALESCE(match_team_id, '<NULL>') AS source_business_key,
        CASE
            WHEN match_team_id IS NULL THEN 'MISSING_PRIMARY_KEY'
            WHEN match_id IS NULL OR match_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN team_number_raw IS NULL OR team_number_raw = '' THEN 'MISSING_REQUIRED_COLUMN'
            WHEN team_number IS NULL OR team_number NOT IN (1, 2) THEN 'VALUE_OUT_OF_RANGE'
            WHEN pre_match_team_rating_raw IS NOT NULL AND pre_match_team_rating IS NULL THEN 'INVALID_DATA_TYPE'
            WHEN post_match_team_rating_raw IS NOT NULL AND post_match_team_rating IS NULL THEN 'INVALID_DATA_TYPE'
        END AS reject_reason,
        CASE
            WHEN match_team_id IS NULL THEN 'MATCH_TEAM_001'
            WHEN match_id IS NULL OR match_sk IS NULL THEN 'MATCH_TEAM_002'
            WHEN team_number_raw IS NULL OR team_number_raw = '' THEN 'MATCH_TEAM_003'
            WHEN team_number IS NULL OR team_number NOT IN (1, 2) THEN 'MATCH_TEAM_003'
            WHEN pre_match_team_rating_raw IS NOT NULL AND pre_match_team_rating IS NULL THEN 'MATCH_TEAM_004'
            WHEN post_match_team_rating_raw IS NOT NULL AND post_match_team_rating IS NULL THEN 'MATCH_TEAM_005'
        END AS rule_id,
        CASE
            WHEN match_team_id IS NULL THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN match_team_id IS NULL THEN 'match_team_id could not be resolved.'
            WHEN match_id IS NULL OR match_sk IS NULL THEN concat('match_id ''', match_id, ''' was not found in accepted matches.')
            WHEN team_number_raw IS NULL OR team_number_raw = '' THEN 'team_number is required.'
            WHEN team_number IS NULL OR team_number NOT IN (1, 2) THEN 'team_number must be 1 or 2.'
            WHEN pre_match_team_rating_raw IS NOT NULL AND pre_match_team_rating IS NULL THEN concat('Invalid pre_match_team_rating value ''', pre_match_team_rating_raw, '''.')
            WHEN post_match_team_rating_raw IS NOT NULL AND post_match_team_rating IS NULL THEN concat('Invalid post_match_team_rating value ''', post_match_team_rating_raw, '''.')
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'match_team_id', match_team_id,
                'match_id', match_id,
                'team_id', team_id,
                'team_number_raw', team_number_raw,
                'pre_match_team_rating_raw', pre_match_team_rating_raw,
                'post_match_team_rating_raw', post_match_team_rating_raw
            )
        ) AS source_record_json
    FROM validated_source
    WHERE match_team_id IS NULL
       OR match_id IS NULL
       OR match_sk IS NULL
       OR team_number_raw IS NULL
       OR team_number_raw = ''
       OR team_number IS NULL
       OR team_number NOT IN (1, 2)
       OR (pre_match_team_rating_raw IS NOT NULL AND pre_match_team_rating IS NULL)
       OR (post_match_team_rating_raw IS NOT NULL AND post_match_team_rating IS NULL)
),
valid_rows AS (
    SELECT
        match_team_id,
        match_id,
        match_sk,
        match_date,
        winning_team_number,
        team_id,
        team_sk,
        team_number,
        pre_match_team_rating,
        post_match_team_rating
    FROM validated_source
    WHERE match_team_id IS NOT NULL
      AND match_id IS NOT NULL
      AND match_sk IS NOT NULL
      AND team_number IS NOT NULL
      AND team_number IN (1, 2)
      AND NOT (
          (pre_match_team_rating_raw IS NOT NULL AND pre_match_team_rating IS NULL)
          OR (post_match_team_rating_raw IS NOT NULL AND post_match_team_rating IS NULL)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_team_id
            ORDER BY
                (
                    CASE WHEN match_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_number IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN pre_match_team_rating IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN post_match_team_rating IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(match_team_id, '<NULL>'),
                        coalesce(match_id, '<NULL>'),
                        coalesce(team_id, '<NULL>'),
                        coalesce(cast(team_number as string), '<NULL>'),
                        coalesce(cast(pre_match_team_rating as string), '<NULL>'),
                        coalesce(cast(post_match_team_rating as string), '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
),
accepted_rows AS (
    SELECT
        match_team_id,
        sha2(coalesce(match_team_id, '<NULL>'), 256) AS match_team_sk,
        match_id,
        match_sk,
        match_date,
        team_id,
        team_sk,
        team_number,
        CASE
            WHEN winning_team_number IS NOT NULL THEN winning_team_number = team_number
            ELSE NULL
        END AS winner_flag,
        pre_match_team_rating,
        post_match_team_rating,
        CASE
            WHEN pre_match_team_rating IS NOT NULL AND post_match_team_rating IS NOT NULL
                THEN post_match_team_rating - pre_match_team_rating
            ELSE NULL
        END AS rating_change,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY match_id) <> {expected_match_team_count} THEN true
            ELSE false
        END AS side_cardinality_warning_flag
    FROM ranked_rows
    WHERE duplicate_rank = 1
)
""".strip()

    accepted_sql = f"""
{base_ctes}
SELECT
    match_team_id,
    match_team_sk,
    match_id,
    match_sk,
    match_date,
    team_id,
    team_sk,
    team_number,
    winner_flag,
    pre_match_team_rating,
    post_match_team_rating,
    rating_change,
    side_cardinality_warning_flag,
    {metadata_sql}
FROM accepted_rows
""".strip()

    rejected_sql = f"""
{base_ctes}
SELECT
    source_table,
    target_table,
    source_business_key,
    reject_reason,
    reject_reason AS reject_reason_code,
    reject_reason_detail,
    rule_id,
    rule_severity,
    pipeline_run_id,
    _pipeline_run_id,
    _source_dataset,
    load_ts,
    _load_ts,
    source_record_json,
    sha2(source_record_json, 256) AS _record_hash
FROM invalid_rows
UNION ALL
SELECT
    'match_teams' AS source_table,
    'match_teams' AS target_table,
    match_team_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'MATCH_TEAM_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'match_team_id', match_team_id,
            'match_id', match_id,
            'team_id', team_id,
            'team_number', team_number,
            'pre_match_team_rating', pre_match_team_rating,
            'post_match_team_rating', post_match_team_rating
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'match_team_id', match_team_id,
                'match_id', match_id,
                'team_id', team_id,
                'team_number', team_number,
                'pre_match_team_rating', pre_match_team_rating,
                'post_match_team_rating', post_match_team_rating
            )
        ),
        256
    ) AS _record_hash
FROM ranked_rows
WHERE duplicate_rank > 1
""".strip()

    return SqlReferenceBuildPlan(
        accepted_sql=accepted_sql,
        rejected_sql=rejected_sql,
        bronze_row_count_sql=f"SELECT COUNT(*) AS value FROM {source_table_fqn}",
        exact_duplicate_count_sql=_exact_duplicate_count_sql(
            source_table_fqn,
            [
                "NULLIF(TRIM(CAST(COALESCE(match_team_id, id) AS STRING)), '') AS match_team_id",
                "NULLIF(TRIM(CAST(match_id AS STRING)), '') AS match_id",
                "NULLIF(TRIM(CAST(team_id AS STRING)), '') AS team_id",
                "TRIM(CAST(COALESCE(team_number, side_number) AS STRING)) AS team_number_raw",
                "NULLIF(TRIM(CAST(COALESCE(pre_match_team_rating, team_rating_before) AS STRING)), '') AS pre_match_team_rating_raw",
                "NULLIF(TRIM(CAST(COALESCE(post_match_team_rating, team_rating_after) AS STRING)), '') AS post_match_team_rating_raw",
            ],
        ),
        business_key_duplicate_count_sql=f"""
WITH valid_rows AS (
    SELECT DISTINCT
        source.match_team_id,
        source.match_id,
        source.team_id,
        match.match_sk,
        match.match_date,
        match.winning_team_number,
        team.team_sk,
        CAST(source.team_number_raw AS INT) AS team_number,
        CAST(source.pre_match_team_rating_raw AS DOUBLE) AS pre_match_team_rating,
        CAST(source.post_match_team_rating_raw AS DOUBLE) AS post_match_team_rating
    FROM (
        SELECT
            NULLIF(TRIM(CAST(COALESCE(match_team_id, id) AS STRING)), '') AS match_team_id,
            NULLIF(TRIM(CAST(match_id AS STRING)), '') AS match_id,
            NULLIF(TRIM(CAST(team_id AS STRING)), '') AS team_id,
            TRIM(CAST(COALESCE(team_number, side_number) AS STRING)) AS team_number_raw,
            NULLIF(TRIM(CAST(COALESCE(pre_match_team_rating, team_rating_before) AS STRING)), '') AS pre_match_team_rating_raw,
            NULLIF(TRIM(CAST(COALESCE(post_match_team_rating, team_rating_after) AS STRING)), '') AS post_match_team_rating_raw
        FROM {source_table_fqn}
    ) source
    LEFT JOIN {matches_fqn} match
        ON source.match_id = match.match_id
    LEFT JOIN {teams_fqn} team
        ON source.team_id = team.team_id
    WHERE source.match_team_id IS NOT NULL
      AND source.match_id IS NOT NULL
      AND match.match_sk IS NOT NULL
      AND CAST(source.team_number_raw AS INT) IN (1, 2)
      AND source.team_number_raw IS NOT NULL
      AND source.team_number_raw <> ''
      AND NOT (
          (source.pre_match_team_rating_raw IS NOT NULL AND CAST(source.pre_match_team_rating_raw AS DOUBLE) IS NULL)
          OR (source.post_match_team_rating_raw IS NOT NULL AND CAST(source.post_match_team_rating_raw AS DOUBLE) IS NULL)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_team_id
            ORDER BY
                (
                    CASE WHEN match_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_number IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN pre_match_team_rating IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN post_match_team_rating IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(match_team_id, '<NULL>'),
                        coalesce(match_id, '<NULL>'),
                        coalesce(team_id, '<NULL>'),
                        coalesce(cast(team_number as string), '<NULL>'),
                        coalesce(cast(pre_match_team_rating as string), '<NULL>'),
                        coalesce(cast(post_match_team_rating as string), '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
)
SELECT COUNT(*) AS value
FROM ranked_rows
WHERE duplicate_rank > 1
""".strip(),
        warning_count_sql=f"""
SELECT COUNT(*) AS value
FROM (
    SELECT match_id
    FROM {silver_schema_fqn}.match_teams
    GROUP BY match_id
    HAVING COUNT(*) <> {expected_match_team_count}
)
""".strip(),
    )


def _build_match_team_players_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    match_teams_fqn = f"{silver_schema_fqn}.match_teams"
    players_fqn = f"{silver_schema_fqn}.players"
    team_memberships_fqn = f"{silver_schema_fqn}.team_memberships"
    expected_player_count = int(config.data["thresholds"]["expected_match_team_player_count"])
    position_expr = _domain_case_expression("position_input", config.data["domains"]["player_position"])
    position_expr_from_source = position_expr.replace(
        "position_input",
        "COALESCE(source.player_position_raw, source.position_alias_raw)",
    )
    metadata_sql = _metadata_sql(
        context,
        source_table="match_team_players",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(match_team_player_id, '<NULL>'), "
            "coalesce(match_team_id, '<NULL>'), coalesce(player_id, '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH normalized_source AS (
    SELECT
        NULLIF(TRIM(CAST(COALESCE(match_team_player_id, id) AS STRING)), '') AS match_team_player_id,
        NULLIF(TRIM(CAST(match_team_id AS STRING)), '') AS match_team_id,
        NULLIF(TRIM(CAST(player_id AS STRING)), '') AS player_id,
        NULLIF(UPPER(TRIM(CAST(player_position AS STRING))), '') AS player_position_raw,
        NULLIF(UPPER(TRIM(CAST(position AS STRING))), '') AS position_alias_raw,
        NULLIF(TRIM(CAST(COALESCE(player_rating_at_match, rating_at_match) AS STRING)), '') AS player_rating_at_match_raw
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT *
    FROM normalized_source
),
typed_source AS (
    SELECT
        source.*,
        COALESCE(source.player_position_raw, source.position_alias_raw) AS position_input,
        {position_expr_from_source} AS player_position,
        CAST(source.player_rating_at_match_raw AS DOUBLE) AS player_rating_at_match
    FROM deduped_source source
),
validated_source AS (
    SELECT
        source.*,
        match_team.match_team_sk,
        match_team.match_id,
        match_team.match_sk,
        match_team.match_date,
        match_team.team_id,
        match_team.team_sk,
        player.player_sk
    FROM typed_source source
    LEFT JOIN {match_teams_fqn} match_team
        ON source.match_team_id = match_team.match_team_id
    LEFT JOIN {players_fqn} player
        ON source.player_id = player.player_id
),
invalid_rows AS (
    SELECT
        'match_team_players' AS source_table,
        'match_team_players' AS target_table,
        COALESCE(match_team_player_id, '<NULL>') AS source_business_key,
        CASE
            WHEN match_team_player_id IS NULL THEN 'MISSING_PRIMARY_KEY'
            WHEN match_team_id IS NULL OR match_team_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN player_id IS NULL OR player_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN player_position_raw IS NOT NULL AND player_position IS NULL THEN 'INVALID_DOMAIN_VALUE'
            WHEN player_rating_at_match_raw IS NOT NULL AND player_rating_at_match IS NULL THEN 'INVALID_DATA_TYPE'
        END AS reject_reason,
        CASE
            WHEN match_team_player_id IS NULL THEN 'MATCH_TEAM_PLAYER_001'
            WHEN match_team_id IS NULL OR match_team_sk IS NULL THEN 'MATCH_TEAM_PLAYER_002'
            WHEN player_id IS NULL OR player_sk IS NULL THEN 'MATCH_TEAM_PLAYER_003'
            WHEN player_position_raw IS NOT NULL AND player_position IS NULL THEN 'MATCH_TEAM_PLAYER_004'
            WHEN player_rating_at_match_raw IS NOT NULL AND player_rating_at_match IS NULL THEN 'MATCH_TEAM_PLAYER_005'
        END AS rule_id,
        CASE
            WHEN match_team_player_id IS NULL THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN match_team_player_id IS NULL THEN 'match_team_player_id could not be resolved.'
            WHEN match_team_id IS NULL OR match_team_sk IS NULL THEN concat('match_team_id ''', match_team_id, ''' was not found in accepted match_teams.')
            WHEN player_id IS NULL OR player_sk IS NULL THEN concat('player_id ''', player_id, ''' was not found in accepted players.')
            WHEN player_position_raw IS NOT NULL AND player_position IS NULL THEN concat('Invalid player_position value ''', player_position_raw, '''.')
            WHEN player_rating_at_match_raw IS NOT NULL AND player_rating_at_match IS NULL THEN concat('Invalid player_rating_at_match value ''', player_rating_at_match_raw, '''.')
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'match_team_player_id', match_team_player_id,
                'match_team_id', match_team_id,
                'player_id', player_id,
                'player_position_raw', player_position_raw,
                'position_alias_raw', position_alias_raw,
                'position_input', position_input,
                'player_rating_at_match_raw', player_rating_at_match_raw
            )
        ) AS source_record_json
    FROM validated_source
    WHERE match_team_player_id IS NULL
       OR match_team_id IS NULL
       OR match_team_sk IS NULL
       OR player_id IS NULL
       OR player_sk IS NULL
       OR (player_position_raw IS NOT NULL AND player_position IS NULL)
       OR (player_rating_at_match_raw IS NOT NULL AND player_rating_at_match IS NULL)
),
valid_rows AS (
    SELECT
        match_team_player_id,
        match_team_id,
        match_team_sk,
        match_id,
        match_sk,
        match_date,
        team_id,
        team_sk,
        player_id,
        player_sk,
        player_position,
        player_rating_at_match
    FROM validated_source
    WHERE match_team_player_id IS NOT NULL
      AND match_team_id IS NOT NULL
      AND match_team_sk IS NOT NULL
      AND player_id IS NOT NULL
      AND player_sk IS NOT NULL
      AND NOT (
          (player_position_raw IS NOT NULL AND player_position IS NULL)
          OR (player_rating_at_match_raw IS NOT NULL AND player_rating_at_match IS NULL)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_team_player_id
            ORDER BY
                (
                    CASE WHEN match_team_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN player_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN player_position IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN player_rating_at_match IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(match_team_player_id, '<NULL>'),
                        coalesce(match_team_id, '<NULL>'),
                        coalesce(player_id, '<NULL>'),
                        coalesce(player_position, '<NULL>'),
                        coalesce(cast(player_rating_at_match as string), '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
),
base_accepted AS (
    SELECT
        match_team_player_id,
        sha2(coalesce(match_team_player_id, '<NULL>'), 256) AS match_team_player_sk,
        match_team_id,
        match_team_sk,
        match_id,
        match_sk,
        match_date,
        team_id,
        team_sk,
        player_id,
        player_sk,
        player_position,
        player_rating_at_match
    FROM ranked_rows
    WHERE duplicate_rank = 1
),
structural_rejects AS (
    SELECT
        source_table,
        target_table,
        source_business_key,
        reject_reason,
        rule_id,
        rule_severity,
        reject_reason_detail,
        pipeline_run_id,
        _pipeline_run_id,
        _source_dataset,
        load_ts,
        _load_ts,
        source_record_json
    FROM (
        SELECT
            'match_team_players' AS source_table,
            'match_team_players' AS target_table,
            match_team_player_id AS source_business_key,
            CASE
                WHEN same_side_count > 1 THEN 'INVALID_PARTICIPANT_CARDINALITY'
                WHEN match_player_count > 1 AND min_match_team_id <> max_match_team_id THEN 'PLAYER_ON_BOTH_MATCH_SIDES'
            END AS reject_reason,
            CASE
                WHEN same_side_count > 1 THEN 'MATCH_TEAM_PLAYER_006'
                WHEN match_player_count > 1 AND min_match_team_id <> max_match_team_id THEN 'MATCH_TEAM_PLAYER_007'
            END AS rule_id,
            'ERROR' AS rule_severity,
            CASE
                WHEN same_side_count > 1 THEN 'player appears more than once on the same match side.'
                WHEN match_player_count > 1 AND min_match_team_id <> max_match_team_id THEN 'player appears on both sides of the same match.'
            END AS reject_reason_detail,
            {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
            {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
            {sql_literal(context.release_name)} AS _source_dataset,
            current_timestamp() AS load_ts,
            current_timestamp() AS _load_ts,
            TO_JSON(
                NAMED_STRUCT(
                    'match_team_player_id', match_team_player_id,
                    'match_team_id', match_team_id,
                    'match_id', match_id,
                    'team_id', team_id,
                    'player_id', player_id,
                    'player_position', player_position,
                    'player_rating_at_match', player_rating_at_match
                )
            ) AS source_record_json
        FROM (
            SELECT
                base.*,
                COUNT(*) OVER (PARTITION BY match_team_id, player_id) AS same_side_count,
                COUNT(*) OVER (PARTITION BY match_id, player_id) AS match_player_count,
                MIN(match_team_id) OVER (PARTITION BY match_id, player_id) AS min_match_team_id,
                MAX(match_team_id) OVER (PARTITION BY match_id, player_id) AS max_match_team_id
            FROM base_accepted base
        )
    )
    WHERE reject_reason IS NOT NULL
),
accepted_rows AS (
    SELECT
        base.*,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM {team_memberships_fqn} membership
                WHERE membership.team_id = base.team_id
                  AND membership.player_id = base.player_id
            ) AND NOT EXISTS (
                SELECT 1
                FROM {team_memberships_fqn} membership
                WHERE membership.team_id = base.team_id
                  AND membership.player_id = base.player_id
                  AND (membership.membership_start_date IS NULL OR membership.membership_start_date <= base.match_date)
                  AND (membership.membership_end_date IS NULL OR membership.membership_end_date >= base.match_date)
            ) THEN true
            ELSE false
        END AS membership_history_warning_flag
    FROM base_accepted base
    WHERE NOT EXISTS (
        SELECT 1
        FROM structural_rejects reject
        WHERE reject.source_business_key = base.match_team_player_id
    )
)
""".strip()

    accepted_sql = f"""
{base_ctes}
SELECT
    match_team_player_id,
    match_team_player_sk,
    match_team_id,
    match_team_sk,
    match_id,
    match_sk,
    match_date,
    team_id,
    team_sk,
    player_id,
    player_sk,
    player_position,
    player_rating_at_match,
    membership_history_warning_flag,
    {metadata_sql}
FROM accepted_rows
""".strip()

    rejected_sql = f"""
{base_ctes}
SELECT
    source_table,
    target_table,
    source_business_key,
    reject_reason,
    reject_reason AS reject_reason_code,
    reject_reason_detail,
    rule_id,
    rule_severity,
    pipeline_run_id,
    _pipeline_run_id,
    _source_dataset,
    load_ts,
    _load_ts,
    source_record_json,
    sha2(source_record_json, 256) AS _record_hash
FROM invalid_rows
UNION ALL
SELECT
    'match_team_players' AS source_table,
    'match_team_players' AS target_table,
    match_team_player_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'MATCH_TEAM_PLAYER_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'match_team_player_id', match_team_player_id,
            'match_team_id', match_team_id,
            'player_id', player_id,
            'player_position', player_position,
            'player_rating_at_match', player_rating_at_match
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'match_team_player_id', match_team_player_id,
                'match_team_id', match_team_id,
                'player_id', player_id,
                'player_position', player_position,
                'player_rating_at_match', player_rating_at_match
            )
        ),
        256
    ) AS _record_hash
FROM ranked_rows
WHERE duplicate_rank > 1
UNION ALL
SELECT
    source_table,
    target_table,
    source_business_key,
    reject_reason,
    reject_reason AS reject_reason_code,
    reject_reason_detail,
    rule_id,
    rule_severity,
    pipeline_run_id,
    _pipeline_run_id,
    _source_dataset,
    load_ts,
    _load_ts,
    source_record_json,
    sha2(source_record_json, 256) AS _record_hash
FROM structural_rejects
""".strip()

    return SqlReferenceBuildPlan(
        accepted_sql=accepted_sql,
        rejected_sql=rejected_sql,
        bronze_row_count_sql=f"SELECT COUNT(*) AS value FROM {source_table_fqn}",
        exact_duplicate_count_sql=_exact_duplicate_count_sql(
            source_table_fqn,
            [
                "NULLIF(TRIM(CAST(COALESCE(match_team_player_id, id) AS STRING)), '') AS match_team_player_id",
                "NULLIF(TRIM(CAST(match_team_id AS STRING)), '') AS match_team_id",
                "NULLIF(TRIM(CAST(player_id AS STRING)), '') AS player_id",
                "NULLIF(UPPER(TRIM(CAST(player_position AS STRING))), '') AS player_position_raw",
                "NULLIF(UPPER(TRIM(CAST(position AS STRING))), '') AS position_alias_raw",
                "COALESCE(NULLIF(UPPER(TRIM(CAST(player_position AS STRING))), ''), NULLIF(UPPER(TRIM(CAST(position AS STRING))), '')) AS position_input",
                "NULLIF(TRIM(CAST(COALESCE(player_rating_at_match, rating_at_match) AS STRING)), '') AS player_rating_at_match_raw",
            ],
        ),
        business_key_duplicate_count_sql=f"""
WITH valid_rows AS (
    SELECT DISTINCT
        source.match_team_player_id,
        source.match_team_id,
        source.player_id,
        match_team.match_team_sk,
        player.player_sk,
        {position_expr_from_source} AS player_position,
        CAST(source.player_rating_at_match_raw AS DOUBLE) AS player_rating_at_match
    FROM (
        SELECT
            NULLIF(TRIM(CAST(COALESCE(match_team_player_id, id) AS STRING)), '') AS match_team_player_id,
            NULLIF(TRIM(CAST(match_team_id AS STRING)), '') AS match_team_id,
            NULLIF(TRIM(CAST(player_id AS STRING)), '') AS player_id,
            NULLIF(UPPER(TRIM(CAST(player_position AS STRING))), '') AS player_position_raw,
            NULLIF(UPPER(TRIM(CAST(position AS STRING))), '') AS position_alias_raw,
            NULLIF(TRIM(CAST(COALESCE(player_rating_at_match, rating_at_match) AS STRING)), '') AS player_rating_at_match_raw
        FROM {source_table_fqn}
    ) source
    LEFT JOIN {match_teams_fqn} match_team
        ON source.match_team_id = match_team.match_team_id
    LEFT JOIN {players_fqn} player
        ON source.player_id = player.player_id
    WHERE source.match_team_player_id IS NOT NULL
      AND source.match_team_id IS NOT NULL
      AND match_team.match_team_sk IS NOT NULL
      AND source.player_id IS NOT NULL
      AND player.player_sk IS NOT NULL
      AND NOT (
          (source.player_position_raw IS NOT NULL AND {position_expr_from_source} IS NULL)
          OR (source.player_rating_at_match_raw IS NOT NULL AND CAST(source.player_rating_at_match_raw AS DOUBLE) IS NULL)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_team_player_id
            ORDER BY
                (
                    CASE WHEN match_team_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN player_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN player_position IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN player_rating_at_match IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(match_team_player_id, '<NULL>'),
                        coalesce(match_team_id, '<NULL>'),
                        coalesce(player_id, '<NULL>'),
                        coalesce(player_position, '<NULL>'),
                        coalesce(cast(player_rating_at_match as string), '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
)
SELECT COUNT(*) AS value
FROM ranked_rows
WHERE duplicate_rank > 1
""".strip(),
        warning_count_sql=f"""
SELECT
    COALESCE((
        SELECT COUNT(*)
        FROM {silver_schema_fqn}.match_team_players
        WHERE membership_history_warning_flag = true
    ), 0)
    +
    COALESCE((
        SELECT COUNT(*)
        FROM (
            SELECT match_team_id
            FROM {silver_schema_fqn}.match_team_players
            GROUP BY match_team_id
            HAVING COUNT(*) <> {expected_player_count}
        )
    ), 0) AS value
""".strip(),
    )


def _build_match_games_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    matches_fqn = f"{silver_schema_fqn}.matches"
    close_game_margin = int(config.data["thresholds"]["close_game_margin"])
    score_share_tolerance = float(config.data["thresholds"]["score_share_tolerance"])
    metadata_sql = _metadata_sql(
        context,
        source_table="match_games",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(match_game_id, '<NULL>'), "
            "coalesce(match_id, '<NULL>'), coalesce(cast(game_number as string), '<NULL>'), "
            "coalesce(cast(team_one_score as string), '<NULL>'), "
            "coalesce(cast(team_two_score as string), '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH normalized_source AS (
    SELECT
        NULLIF(TRIM(CAST(COALESCE(match_game_id, id) AS STRING)), '') AS match_game_id,
        NULLIF(TRIM(CAST(match_id AS STRING)), '') AS match_id,
        TRIM(CAST(game_number AS STRING)) AS game_number_raw,
        TRIM(CAST(team_one_score AS STRING)) AS team_one_score_raw,
        TRIM(CAST(team_two_score AS STRING)) AS team_two_score_raw,
        TRIM(CAST(COALESCE(winning_team_number, winner_team_number) AS STRING)) AS winning_team_number_raw,
        TRIM(CAST(target_score AS STRING)) AS target_score_raw,
        TRIM(CAST(win_by AS STRING)) AS win_by_raw,
        NULLIF(TRIM(CAST(actual_team_one_score_share AS STRING)), '') AS actual_team_one_score_share_raw
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT *
    FROM normalized_source
),
typed_source AS (
    SELECT
        source.*,
        CAST(source.game_number_raw AS INT) AS game_number,
        CAST(source.team_one_score_raw AS INT) AS team_one_score,
        CAST(source.team_two_score_raw AS INT) AS team_two_score,
        CAST(source.winning_team_number_raw AS INT) AS winning_team_number,
        CAST(source.target_score_raw AS INT) AS target_score,
        CAST(source.win_by_raw AS INT) AS win_by,
        CAST(source.actual_team_one_score_share_raw AS DOUBLE) AS actual_team_one_score_share
    FROM deduped_source source
),
validated_source AS (
    SELECT
        source.*,
        match.match_sk,
        CASE
            WHEN source.team_one_score IS NOT NULL AND source.team_two_score IS NOT NULL
                THEN source.team_one_score + source.team_two_score
            ELSE NULL
        END AS total_points,
        CASE
            WHEN source.team_one_score > source.team_two_score THEN 1
            WHEN source.team_two_score > source.team_one_score THEN 2
            ELSE NULL
        END AS derived_winner
    FROM typed_source source
    LEFT JOIN {matches_fqn} match
        ON source.match_id = match.match_id
),
invalid_rows AS (
    SELECT
        'match_games' AS source_table,
        'match_games' AS target_table,
        COALESCE(match_game_id, '<NULL>') AS source_business_key,
        CASE
            WHEN match_game_id IS NULL THEN 'MISSING_PRIMARY_KEY'
            WHEN match_id IS NULL OR match_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN game_number_raw IS NULL OR game_number_raw = '' THEN 'MISSING_REQUIRED_COLUMN'
            WHEN game_number IS NULL OR game_number <= 0 THEN 'VALUE_OUT_OF_RANGE'
            WHEN team_one_score_raw IS NULL OR team_one_score_raw = '' THEN 'MISSING_REQUIRED_COLUMN'
            WHEN team_one_score IS NULL THEN 'INVALID_DATA_TYPE'
            WHEN team_one_score < 0 THEN 'VALUE_OUT_OF_RANGE'
            WHEN team_two_score_raw IS NULL OR team_two_score_raw = '' THEN 'MISSING_REQUIRED_COLUMN'
            WHEN team_two_score IS NULL THEN 'INVALID_DATA_TYPE'
            WHEN team_two_score < 0 THEN 'VALUE_OUT_OF_RANGE'
            WHEN winning_team_number_raw IS NULL OR winning_team_number_raw = '' THEN 'MISSING_REQUIRED_COLUMN'
            WHEN winning_team_number IS NULL OR winning_team_number NOT IN (1, 2) THEN 'VALUE_OUT_OF_RANGE'
            WHEN derived_winner IS NULL OR winning_team_number <> derived_winner THEN 'GAME_SCORE_WINNER_MISMATCH'
            WHEN target_score_raw IS NOT NULL AND target_score_raw <> '' AND target_score IS NULL THEN 'INVALID_DATA_TYPE'
            WHEN target_score < 0 THEN 'VALUE_OUT_OF_RANGE'
            WHEN win_by_raw IS NOT NULL AND win_by_raw <> '' AND win_by IS NULL THEN 'INVALID_DATA_TYPE'
            WHEN win_by < 0 THEN 'VALUE_OUT_OF_RANGE'
            WHEN actual_team_one_score_share_raw IS NOT NULL AND actual_team_one_score_share IS NULL THEN 'INVALID_DATA_TYPE'
            WHEN actual_team_one_score_share IS NOT NULL AND total_points > 0
                 AND ABS(actual_team_one_score_share - (CAST(team_one_score AS DOUBLE) / total_points)) > {score_share_tolerance} THEN 'VALUE_OUT_OF_RANGE'
        END AS reject_reason,
        CASE
            WHEN match_game_id IS NULL THEN 'MATCH_GAME_001'
            WHEN match_id IS NULL OR match_sk IS NULL THEN 'MATCH_GAME_002'
            WHEN game_number_raw IS NULL OR game_number_raw = '' OR game_number IS NULL OR game_number <= 0 THEN 'MATCH_GAME_003'
            WHEN team_one_score_raw IS NULL OR team_one_score_raw = '' OR team_one_score IS NULL OR team_one_score < 0 THEN 'MATCH_GAME_004'
            WHEN team_two_score_raw IS NULL OR team_two_score_raw = '' OR team_two_score IS NULL OR team_two_score < 0 THEN 'MATCH_GAME_005'
            WHEN winning_team_number_raw IS NULL OR winning_team_number_raw = '' OR winning_team_number IS NULL OR winning_team_number NOT IN (1, 2) THEN 'MATCH_GAME_006'
            WHEN derived_winner IS NULL OR winning_team_number <> derived_winner THEN 'MATCH_GAME_007'
            WHEN target_score_raw IS NOT NULL AND target_score_raw <> '' AND (target_score IS NULL OR target_score < 0) THEN 'MATCH_GAME_008'
            WHEN win_by_raw IS NOT NULL AND win_by_raw <> '' AND (win_by IS NULL OR win_by < 0) THEN 'MATCH_GAME_009'
            WHEN actual_team_one_score_share_raw IS NOT NULL AND actual_team_one_score_share IS NULL THEN 'MATCH_GAME_010'
            WHEN actual_team_one_score_share IS NOT NULL AND total_points > 0
                 AND ABS(actual_team_one_score_share - (CAST(team_one_score AS DOUBLE) / total_points)) > {score_share_tolerance} THEN 'MATCH_GAME_011'
        END AS rule_id,
        CASE
            WHEN match_game_id IS NULL THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN match_game_id IS NULL THEN 'match_game_id could not be resolved.'
            WHEN match_id IS NULL OR match_sk IS NULL THEN concat('match_id ''', match_id, ''' was not found in accepted matches.')
            WHEN game_number_raw IS NULL OR game_number_raw = '' THEN 'game_number is required.'
            WHEN game_number IS NULL THEN concat('Invalid game_number value ''', game_number_raw, '''.')
            WHEN game_number <= 0 THEN 'game_number must be positive.'
            WHEN team_one_score_raw IS NULL OR team_one_score_raw = '' THEN 'team_one_score is required.'
            WHEN team_one_score IS NULL THEN concat('Invalid team_one_score value ''', team_one_score_raw, '''.')
            WHEN team_one_score < 0 THEN 'team_one_score must be non-negative.'
            WHEN team_two_score_raw IS NULL OR team_two_score_raw = '' THEN 'team_two_score is required.'
            WHEN team_two_score IS NULL THEN concat('Invalid team_two_score value ''', team_two_score_raw, '''.')
            WHEN team_two_score < 0 THEN 'team_two_score must be non-negative.'
            WHEN winning_team_number_raw IS NULL OR winning_team_number_raw = '' THEN 'winning_team_number is required.'
            WHEN winning_team_number IS NULL OR winning_team_number NOT IN (1, 2) THEN 'winning_team_number must be 1 or 2.'
            WHEN derived_winner IS NULL OR winning_team_number <> derived_winner THEN 'winning_team_number is inconsistent with the game scores.'
            WHEN target_score_raw IS NOT NULL AND target_score_raw <> '' AND target_score IS NULL THEN concat('Invalid target_score value ''', target_score_raw, '''.')
            WHEN target_score < 0 THEN 'target_score must be non-negative.'
            WHEN win_by_raw IS NOT NULL AND win_by_raw <> '' AND win_by IS NULL THEN concat('Invalid win_by value ''', win_by_raw, '''.')
            WHEN win_by < 0 THEN 'win_by must be non-negative.'
            WHEN actual_team_one_score_share_raw IS NOT NULL AND actual_team_one_score_share IS NULL THEN concat('Invalid actual_team_one_score_share value ''', actual_team_one_score_share_raw, '''.')
            WHEN actual_team_one_score_share IS NOT NULL AND total_points > 0
                 AND ABS(actual_team_one_score_share - (CAST(team_one_score AS DOUBLE) / total_points)) > {score_share_tolerance} THEN 'actual_team_one_score_share does not reconcile with the scores.'
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'match_game_id', match_game_id,
                'match_id', match_id,
                'game_number_raw', game_number_raw,
                'team_one_score_raw', team_one_score_raw,
                'team_two_score_raw', team_two_score_raw,
                'winning_team_number_raw', winning_team_number_raw,
                'target_score_raw', target_score_raw,
                'win_by_raw', win_by_raw,
                'actual_team_one_score_share_raw', actual_team_one_score_share_raw
            )
        ) AS source_record_json
    FROM validated_source
    WHERE match_game_id IS NULL
       OR match_id IS NULL
       OR match_sk IS NULL
       OR game_number_raw IS NULL
       OR game_number_raw = ''
       OR game_number IS NULL
       OR game_number <= 0
       OR team_one_score_raw IS NULL
       OR team_one_score_raw = ''
       OR team_one_score IS NULL
       OR team_one_score < 0
       OR team_two_score_raw IS NULL
       OR team_two_score_raw = ''
       OR team_two_score IS NULL
       OR team_two_score < 0
       OR winning_team_number_raw IS NULL
       OR winning_team_number_raw = ''
       OR winning_team_number IS NULL
       OR winning_team_number NOT IN (1, 2)
       OR derived_winner IS NULL
       OR winning_team_number <> derived_winner
       OR (target_score_raw IS NOT NULL AND target_score_raw <> '' AND (target_score IS NULL OR target_score < 0))
       OR (win_by_raw IS NOT NULL AND win_by_raw <> '' AND (win_by IS NULL OR win_by < 0))
       OR (actual_team_one_score_share_raw IS NOT NULL AND actual_team_one_score_share IS NULL)
       OR (
            actual_team_one_score_share IS NOT NULL AND total_points > 0
            AND ABS(actual_team_one_score_share - (CAST(team_one_score AS DOUBLE) / total_points)) > {score_share_tolerance}
       )
),
valid_rows AS (
    SELECT
        match_game_id,
        match_id,
        match_sk,
        game_number,
        team_one_score,
        team_two_score,
        winning_team_number,
        target_score,
        win_by,
        actual_team_one_score_share,
        ABS(team_one_score - team_two_score) AS score_margin,
        team_one_score + team_two_score AS total_points,
        GREATEST(team_one_score, team_two_score) AS winning_score
    FROM validated_source
    WHERE match_game_id IS NOT NULL
      AND match_id IS NOT NULL
      AND match_sk IS NOT NULL
      AND game_number IS NOT NULL
      AND game_number > 0
      AND team_one_score IS NOT NULL
      AND team_one_score >= 0
      AND team_two_score IS NOT NULL
      AND team_two_score >= 0
      AND winning_team_number IS NOT NULL
      AND winning_team_number IN (1, 2)
      AND derived_winner IS NOT NULL
      AND winning_team_number = derived_winner
      AND NOT (
          (target_score_raw IS NOT NULL AND target_score_raw <> '' AND (target_score IS NULL OR target_score < 0))
          OR (win_by_raw IS NOT NULL AND win_by_raw <> '' AND (win_by IS NULL OR win_by < 0))
          OR (actual_team_one_score_share_raw IS NOT NULL AND actual_team_one_score_share IS NULL)
          OR (
                actual_team_one_score_share IS NOT NULL AND total_points > 0
                AND ABS(actual_team_one_score_share - (CAST(team_one_score AS DOUBLE) / total_points)) > {score_share_tolerance}
          )
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_game_id
            ORDER BY
                (
                    CASE WHEN match_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN game_number IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_one_score IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_two_score IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN target_score IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN win_by IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN actual_team_one_score_share IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(match_game_id, '<NULL>'),
                        coalesce(match_id, '<NULL>'),
                        coalesce(cast(game_number as string), '<NULL>'),
                        coalesce(cast(team_one_score as string), '<NULL>'),
                        coalesce(cast(team_two_score as string), '<NULL>'),
                        coalesce(cast(target_score as string), '<NULL>'),
                        coalesce(cast(win_by as string), '<NULL>'),
                        coalesce(cast(actual_team_one_score_share as string), '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
)
""".strip()

    accepted_sql = f"""
{base_ctes}
SELECT
    match_game_id,
    sha2(coalesce(match_game_id, '<NULL>'), 256) AS match_game_sk,
    match_id,
    match_sk,
    game_number,
    team_one_score,
    team_two_score,
    winning_team_number,
    target_score,
    win_by,
    actual_team_one_score_share,
    score_margin,
    total_points,
    score_margin <= {close_game_margin} AS close_game_flag,
    CASE
        WHEN target_score IS NOT NULL AND winning_score > target_score THEN true
        ELSE false
    END AS extended_game_flag,
    {metadata_sql}
FROM ranked_rows
WHERE duplicate_rank = 1
""".strip()

    rejected_sql = f"""
{base_ctes}
SELECT
    source_table,
    target_table,
    source_business_key,
    reject_reason,
    reject_reason AS reject_reason_code,
    reject_reason_detail,
    rule_id,
    rule_severity,
    pipeline_run_id,
    _pipeline_run_id,
    _source_dataset,
    load_ts,
    _load_ts,
    source_record_json,
    sha2(source_record_json, 256) AS _record_hash
FROM invalid_rows
UNION ALL
SELECT
    'match_games' AS source_table,
    'match_games' AS target_table,
    match_game_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'MATCH_GAME_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'match_game_id', match_game_id,
            'match_id', match_id,
            'game_number', game_number,
            'team_one_score', team_one_score,
            'team_two_score', team_two_score,
            'winning_team_number', winning_team_number,
            'target_score', target_score,
            'win_by', win_by,
            'actual_team_one_score_share', actual_team_one_score_share
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'match_game_id', match_game_id,
                'match_id', match_id,
                'game_number', game_number,
                'team_one_score', team_one_score,
                'team_two_score', team_two_score,
                'winning_team_number', winning_team_number,
                'target_score', target_score,
                'win_by', win_by,
                'actual_team_one_score_share', actual_team_one_score_share
            )
        ),
        256
    ) AS _record_hash
FROM ranked_rows
WHERE duplicate_rank > 1
""".strip()

    return SqlReferenceBuildPlan(
        accepted_sql=accepted_sql,
        rejected_sql=rejected_sql,
        bronze_row_count_sql=f"SELECT COUNT(*) AS value FROM {source_table_fqn}",
        exact_duplicate_count_sql=_exact_duplicate_count_sql(
            source_table_fqn,
            [
                "NULLIF(TRIM(CAST(COALESCE(match_game_id, id) AS STRING)), '') AS match_game_id",
                "NULLIF(TRIM(CAST(match_id AS STRING)), '') AS match_id",
                "TRIM(CAST(game_number AS STRING)) AS game_number_raw",
                "TRIM(CAST(team_one_score AS STRING)) AS team_one_score_raw",
                "TRIM(CAST(team_two_score AS STRING)) AS team_two_score_raw",
                "TRIM(CAST(COALESCE(winning_team_number, winner_team_number) AS STRING)) AS winning_team_number_raw",
                "TRIM(CAST(target_score AS STRING)) AS target_score_raw",
                "TRIM(CAST(win_by AS STRING)) AS win_by_raw",
                "NULLIF(TRIM(CAST(actual_team_one_score_share AS STRING)), '') AS actual_team_one_score_share_raw",
            ],
        ),
        business_key_duplicate_count_sql=f"""
WITH valid_rows AS (
    SELECT DISTINCT
        source.match_game_id,
        source.match_id,
        match.match_sk,
        CAST(source.game_number_raw AS INT) AS game_number,
        CAST(source.team_one_score_raw AS INT) AS team_one_score,
        CAST(source.team_two_score_raw AS INT) AS team_two_score,
        CAST(source.winning_team_number_raw AS INT) AS winning_team_number,
        CAST(source.target_score_raw AS INT) AS target_score,
        CAST(source.win_by_raw AS INT) AS win_by,
        CAST(source.actual_team_one_score_share_raw AS DOUBLE) AS actual_team_one_score_share
    FROM (
        SELECT
            NULLIF(TRIM(CAST(COALESCE(match_game_id, id) AS STRING)), '') AS match_game_id,
            NULLIF(TRIM(CAST(match_id AS STRING)), '') AS match_id,
            TRIM(CAST(game_number AS STRING)) AS game_number_raw,
            TRIM(CAST(team_one_score AS STRING)) AS team_one_score_raw,
            TRIM(CAST(team_two_score AS STRING)) AS team_two_score_raw,
            TRIM(CAST(COALESCE(winning_team_number, winner_team_number) AS STRING)) AS winning_team_number_raw,
            TRIM(CAST(target_score AS STRING)) AS target_score_raw,
            TRIM(CAST(win_by AS STRING)) AS win_by_raw,
            NULLIF(TRIM(CAST(actual_team_one_score_share AS STRING)), '') AS actual_team_one_score_share_raw
        FROM {source_table_fqn}
    ) source
    LEFT JOIN {matches_fqn} match
        ON source.match_id = match.match_id
    WHERE source.match_game_id IS NOT NULL
      AND source.match_id IS NOT NULL
      AND match.match_sk IS NOT NULL
      AND CAST(source.game_number_raw AS INT) > 0
      AND CAST(source.team_one_score_raw AS INT) >= 0
      AND CAST(source.team_two_score_raw AS INT) >= 0
      AND CAST(source.winning_team_number_raw AS INT) IN (1, 2)
      AND (
            CASE
                WHEN CAST(source.team_one_score_raw AS INT) > CAST(source.team_two_score_raw AS INT) THEN 1
                WHEN CAST(source.team_two_score_raw AS INT) > CAST(source.team_one_score_raw AS INT) THEN 2
                ELSE NULL
            END
          ) = CAST(source.winning_team_number_raw AS INT)
      AND NOT (
          (source.target_score_raw IS NOT NULL AND source.target_score_raw <> '' AND (CAST(source.target_score_raw AS INT) IS NULL OR CAST(source.target_score_raw AS INT) < 0))
          OR (source.win_by_raw IS NOT NULL AND source.win_by_raw <> '' AND (CAST(source.win_by_raw AS INT) IS NULL OR CAST(source.win_by_raw AS INT) < 0))
          OR (source.actual_team_one_score_share_raw IS NOT NULL AND CAST(source.actual_team_one_score_share_raw AS DOUBLE) IS NULL)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_game_id
            ORDER BY
                (
                    CASE WHEN match_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN game_number IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_one_score IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_two_score IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN target_score IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN win_by IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN actual_team_one_score_share IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(match_game_id, '<NULL>'),
                        coalesce(match_id, '<NULL>'),
                        coalesce(cast(game_number as string), '<NULL>'),
                        coalesce(cast(team_one_score as string), '<NULL>'),
                        coalesce(cast(team_two_score as string), '<NULL>'),
                        coalesce(cast(target_score as string), '<NULL>'),
                        coalesce(cast(win_by as string), '<NULL>'),
                        coalesce(cast(actual_team_one_score_share as string), '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
)
SELECT COUNT(*) AS value
FROM ranked_rows
WHERE duplicate_rank > 1
""".strip(),
    )


def _metadata_sql(
    context: PipelineContext,
    *,
    source_table: str,
    record_hash_expr: str,
) -> str:
    return f"""
{sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
{sql_literal(context.pipeline_version)} AS _pipeline_version,
{sql_literal(context.release_name)} AS _source_dataset,
{sql_literal(source_table)} AS _source_table,
current_timestamp() AS _load_ts,
{record_hash_expr} AS _record_hash,
'ACCEPTED' AS _data_quality_status
""".strip()


def _domain_case_expression(column_name: str, domain_config: dict[str, object]) -> str:
    synonyms = {
        str(key).upper(): str(value).upper()
        for key, value in dict(domain_config.get("synonyms", {})).items()
    }
    allowed = [str(item).upper() for item in list(domain_config.get("allowed", []))]
    lines = [f"WHEN {column_name} = {sql_literal(source)} THEN {sql_literal(target)}" for source, target in sorted(synonyms.items())]
    lines.extend(
        f"WHEN {column_name} = {sql_literal(value)} THEN {sql_literal(value)}"
        for value in allowed
        if value not in synonyms
    )
    return "CASE " + " ".join(lines) + " ELSE NULL END"


def _exact_duplicate_count_sql(source_table_fqn: str, projections: list[str]) -> str:
    projection_sql = ",\n        ".join(projections)
    return f"""
WITH normalized_source AS (
    SELECT
        {projection_sql}
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT *
    FROM normalized_source
)
SELECT
    (SELECT COUNT(*) FROM normalized_source) - (SELECT COUNT(*) FROM deduped_source) AS value
""".strip()
