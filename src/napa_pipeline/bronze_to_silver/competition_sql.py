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
