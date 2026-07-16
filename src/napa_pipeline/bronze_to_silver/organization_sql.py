"""Spark SQL plans for Bronze-to-Silver organization and partnership execution."""

from __future__ import annotations

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.operations import PipelineContext
from napa_pipeline.bronze_to_silver.reference_sql import SqlReferenceBuildPlan, sql_literal


def supports_organization_sql_transform(transform_name: str) -> bool:
    """Return whether an organization transform has a Spark SQL execution plan."""
    return transform_name in {
        "build_clubs",
        "build_teams",
        "build_club_memberships",
        "build_team_memberships",
    }


def build_organization_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    target_table: str,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    """Return the SQL execution plan for one supported organization table."""
    if target_table == "clubs":
        return _build_clubs_sql_plan(config, context, source_table_fqn, silver_schema_fqn)
    if target_table == "teams":
        return _build_teams_sql_plan(config, context, source_table_fqn, silver_schema_fqn)
    if target_table == "club_memberships":
        return _build_club_memberships_sql_plan(context, source_table_fqn, silver_schema_fqn)
    if target_table == "team_memberships":
        return _build_team_memberships_sql_plan(config, context, source_table_fqn, silver_schema_fqn)
    raise ValueError(f"No SQL organization plan is defined for target table '{target_table}'.")


def _build_clubs_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    regions_fqn = f"{silver_schema_fqn}.regions"
    country_expr = _domain_case_expression("country_input", config.data["domains"]["country_code"])
    metadata_sql = _metadata_sql(
        context,
        source_table="clubs",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(club_id, '<NULL>'), "
            "coalesce(club_name, '<NULL>'), coalesce(region_id, '<NULL>'), "
            "coalesce(country_code, '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH normalized_source AS (
    SELECT
        NULLIF(TRIM(CAST(COALESCE(club_id, id) AS STRING)), '') AS club_id,
        NULLIF(TRIM(CAST(COALESCE(club_name, name) AS STRING)), '') AS club_name,
        NULLIF(TRIM(CAST(region_id AS STRING)), '') AS region_id,
        NULLIF(UPPER(TRIM(CAST(COALESCE(country_code, country) AS STRING))), '') AS country_input,
        TRIM(CAST(COALESCE(open_date, start_date, formation_date) AS STRING)) AS open_date_raw,
        TRIM(CAST(COALESCE(close_date, end_date, dissolution_date) AS STRING)) AS close_date_raw,
        NULLIF(UPPER(TRIM(CAST(COALESCE(active_flag, status) AS STRING))), '') AS status_input
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT * FROM normalized_source
),
typed_source AS (
    SELECT
        source.*,
        TO_DATE(source.open_date_raw) AS open_date,
        TO_DATE(source.close_date_raw) AS close_date,
        {country_expr} AS country_code
    FROM deduped_source source
),
validated_source AS (
    SELECT
        source.*,
        region.region_sk
    FROM typed_source source
    LEFT JOIN {regions_fqn} region
        ON source.region_id = region.region_id
),
invalid_rows AS (
    SELECT
        'clubs' AS source_table,
        'clubs' AS target_table,
        COALESCE(club_id, '<NULL>') AS source_business_key,
        CASE
            WHEN club_id IS NULL THEN 'MISSING_PRIMARY_KEY'
            WHEN club_name IS NULL THEN 'MISSING_REQUIRED_COLUMN'
            WHEN region_id IS NULL OR region_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN country_input IS NOT NULL AND country_code IS NULL THEN 'INVALID_DOMAIN_VALUE'
            WHEN open_date_raw IS NOT NULL AND open_date_raw <> '' AND open_date IS NULL THEN 'INVALID_DATE'
            WHEN close_date_raw IS NOT NULL AND close_date_raw <> '' AND close_date IS NULL THEN 'INVALID_DATE'
            WHEN open_date IS NOT NULL AND close_date IS NOT NULL AND close_date < open_date THEN 'INVALID_DATE_RANGE'
        END AS reject_reason,
        CASE
            WHEN club_id IS NULL THEN 'CLUB_001'
            WHEN club_name IS NULL THEN 'CLUB_002'
            WHEN region_id IS NULL OR region_sk IS NULL THEN 'CLUB_003'
            WHEN country_input IS NOT NULL AND country_code IS NULL THEN 'CLUB_004'
            WHEN open_date_raw IS NOT NULL AND open_date_raw <> '' AND open_date IS NULL THEN 'CLUB_005'
            WHEN close_date_raw IS NOT NULL AND close_date_raw <> '' AND close_date IS NULL THEN 'CLUB_006'
            WHEN open_date IS NOT NULL AND close_date IS NOT NULL AND close_date < open_date THEN 'CLUB_007'
        END AS rule_id,
        CASE
            WHEN club_id IS NULL OR club_name IS NULL THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN club_id IS NULL THEN 'club_id could not be resolved.'
            WHEN club_name IS NULL THEN 'club_name could not be resolved.'
            WHEN region_id IS NULL OR region_sk IS NULL THEN concat('region_id ''', region_id, ''' was not found in accepted regions.')
            WHEN country_input IS NOT NULL AND country_code IS NULL THEN concat('Invalid country value ''', country_input, '''.')
            WHEN open_date_raw IS NOT NULL AND open_date_raw <> '' AND open_date IS NULL THEN concat('Invalid open_date value ''', open_date_raw, '''.')
            WHEN close_date_raw IS NOT NULL AND close_date_raw <> '' AND close_date IS NULL THEN concat('Invalid close_date value ''', close_date_raw, '''.')
            WHEN open_date IS NOT NULL AND close_date IS NOT NULL AND close_date < open_date THEN 'close_date cannot be before open_date.'
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'club_id', club_id,
                'club_name', club_name,
                'region_id', region_id,
                'country_input', country_input,
                'open_date_raw', open_date_raw,
                'close_date_raw', close_date_raw,
                'status_input', status_input
            )
        ) AS source_record_json
    FROM validated_source
    WHERE club_id IS NULL
       OR club_name IS NULL
       OR region_id IS NULL
       OR region_sk IS NULL
       OR (country_input IS NOT NULL AND country_code IS NULL)
       OR (open_date_raw IS NOT NULL AND open_date_raw <> '' AND open_date IS NULL)
       OR (close_date_raw IS NOT NULL AND close_date_raw <> '' AND close_date IS NULL)
       OR (open_date IS NOT NULL AND close_date IS NOT NULL AND close_date < open_date)
),
valid_rows AS (
    SELECT
        club_id,
        club_name,
        region_id,
        region_sk,
        country_code,
        open_date,
        close_date,
        CASE
            WHEN status_input IN ('TRUE', 'T', 'YES', 'Y', '1', 'ACTIVE') THEN true
            WHEN status_input IN ('FALSE', 'F', 'NO', 'N', '0', 'INACTIVE') THEN false
            ELSE NULL
        END AS active_flag
    FROM validated_source
    WHERE club_id IS NOT NULL
      AND club_name IS NOT NULL
      AND region_id IS NOT NULL
      AND region_sk IS NOT NULL
      AND NOT (
          (country_input IS NOT NULL AND country_code IS NULL)
          OR (open_date_raw IS NOT NULL AND open_date_raw <> '' AND open_date IS NULL)
          OR (close_date_raw IS NOT NULL AND close_date_raw <> '' AND close_date IS NULL)
          OR (open_date IS NOT NULL AND close_date IS NOT NULL AND close_date < open_date)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY club_id
            ORDER BY
                (
                    CASE WHEN club_name IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN region_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN open_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN close_date IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(club_id, '<NULL>'),
                        coalesce(club_name, '<NULL>'),
                        coalesce(region_id, '<NULL>'),
                        coalesce(country_code, '<NULL>'),
                        coalesce(cast(open_date as string), '<NULL>'),
                        coalesce(cast(close_date as string), '<NULL>')
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
    club_id,
    sha2(coalesce(club_id, '<NULL>'), 256) AS club_sk,
    club_name,
    region_id,
    region_sk,
    country_code,
    open_date,
    close_date,
    active_flag,
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
    'clubs' AS source_table,
    'clubs' AS target_table,
    club_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'CLUB_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'club_id', club_id,
            'club_name', club_name,
            'region_id', region_id,
            'country_code', country_code,
            'open_date', open_date,
            'close_date', close_date
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'club_id', club_id,
                'club_name', club_name,
                'region_id', region_id,
                'country_code', country_code,
                'open_date', open_date,
                'close_date', close_date
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
                "NULLIF(TRIM(CAST(COALESCE(club_id, id) AS STRING)), '') AS club_id",
                "NULLIF(TRIM(CAST(COALESCE(club_name, name) AS STRING)), '') AS club_name",
                "NULLIF(TRIM(CAST(region_id AS STRING)), '') AS region_id",
                "NULLIF(UPPER(TRIM(CAST(COALESCE(country_code, country) AS STRING))), '') AS country_input",
                "TRIM(CAST(COALESCE(open_date, start_date, formation_date) AS STRING)) AS open_date_raw",
                "TRIM(CAST(COALESCE(close_date, end_date, dissolution_date) AS STRING)) AS close_date_raw",
                "NULLIF(UPPER(TRIM(CAST(COALESCE(active_flag, status) AS STRING))), '') AS status_input",
            ],
        ),
        business_key_duplicate_count_sql=f"""
WITH valid_rows AS (
    SELECT DISTINCT
        NULLIF(TRIM(CAST(COALESCE(club_id, id) AS STRING)), '') AS club_id,
        NULLIF(TRIM(CAST(COALESCE(club_name, name) AS STRING)), '') AS club_name,
        NULLIF(TRIM(CAST(region_id AS STRING)), '') AS region_id,
        {country_expr} AS country_code,
        TO_DATE(TRIM(CAST(COALESCE(open_date, start_date, formation_date) AS STRING))) AS open_date,
        TO_DATE(TRIM(CAST(COALESCE(close_date, end_date, dissolution_date) AS STRING))) AS close_date
    FROM {source_table_fqn} source
    LEFT JOIN {regions_fqn} region
        ON NULLIF(TRIM(CAST(source.region_id AS STRING)), '') = region.region_id
    WHERE NULLIF(TRIM(CAST(COALESCE(club_id, id) AS STRING)), '') IS NOT NULL
      AND NULLIF(TRIM(CAST(COALESCE(club_name, name) AS STRING)), '') IS NOT NULL
      AND NULLIF(TRIM(CAST(source.region_id AS STRING)), '') IS NOT NULL
      AND region.region_sk IS NOT NULL
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY club_id
            ORDER BY
                (
                    CASE WHEN club_name IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN region_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN open_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN close_date IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(club_id, '<NULL>'),
                        coalesce(club_name, '<NULL>'),
                        coalesce(region_id, '<NULL>'),
                        coalesce(country_code, '<NULL>'),
                        coalesce(cast(open_date as string), '<NULL>'),
                        coalesce(cast(close_date as string), '<NULL>')
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


def _build_teams_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    monthly_batches_fqn = f"{silver_schema_fqn}.monthly_batches"
    team_category_expr = _domain_case_expression("team_category_input", config.data["domains"]["team_type"])
    team_status_expr = _domain_case_expression("team_status_input", config.data["domains"]["team_status"])
    country_expr = _domain_case_expression("country_input", config.data["domains"]["country_code"])
    metadata_sql = _metadata_sql(
        context,
        source_table="teams",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(team_id, '<NULL>'), "
            "coalesce(team_name, '<NULL>'), coalesce(team_category, '<NULL>'), "
            "coalesce(country_code, '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH release_context AS (
    SELECT MAX(batch_date) AS as_of_date
    FROM {monthly_batches_fqn}
),
normalized_source AS (
    SELECT
        NULLIF(TRIM(CAST(COALESCE(team_id, id) AS STRING)), '') AS team_id,
        NULLIF(TRIM(CAST(COALESCE(team_name, name) AS STRING)), '') AS team_name,
        NULLIF(UPPER(TRIM(CAST(COALESCE(team_category, category, team_type) AS STRING))), '') AS team_category_input,
        NULLIF(UPPER(TRIM(CAST(COALESCE(team_status, status) AS STRING))), '') AS team_status_input,
        NULLIF(UPPER(TRIM(CAST(COALESCE(country_code, country) AS STRING))), '') AS country_input,
        TRIM(CAST(COALESCE(formation_date, start_date) AS STRING)) AS formation_date_raw,
        TRIM(CAST(COALESCE(dissolution_date, end_date) AS STRING)) AS dissolution_date_raw,
        NULLIF(UPPER(TRIM(CAST(COALESCE(active_flag, status) AS STRING))), '') AS status_input
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT * FROM normalized_source
),
typed_source AS (
    SELECT
        source.*,
        TO_DATE(source.formation_date_raw) AS formation_date,
        TO_DATE(source.dissolution_date_raw) AS dissolution_date,
        {team_category_expr} AS team_category,
        {team_status_expr} AS team_status,
        {country_expr} AS country_code
    FROM deduped_source source
),
validated_source AS (
    SELECT
        source.*,
        release_context.as_of_date
    FROM typed_source source
    CROSS JOIN release_context
),
invalid_rows AS (
    SELECT
        'teams' AS source_table,
        'teams' AS target_table,
        COALESCE(team_id, '<NULL>') AS source_business_key,
        CASE
            WHEN team_id IS NULL THEN 'MISSING_PRIMARY_KEY'
            WHEN team_category_input IS NOT NULL AND team_category IS NULL THEN 'INVALID_DOMAIN_VALUE'
            WHEN team_status_input IS NOT NULL AND team_status IS NULL THEN 'INVALID_DOMAIN_VALUE'
            WHEN country_input IS NOT NULL AND country_code IS NULL THEN 'INVALID_DOMAIN_VALUE'
            WHEN formation_date_raw IS NOT NULL AND formation_date_raw <> '' AND formation_date IS NULL THEN 'INVALID_DATE'
            WHEN dissolution_date_raw IS NOT NULL AND dissolution_date_raw <> '' AND dissolution_date IS NULL THEN 'INVALID_DATE'
            WHEN formation_date IS NOT NULL AND dissolution_date IS NOT NULL AND dissolution_date < formation_date THEN 'INVALID_DATE_RANGE'
        END AS reject_reason,
        CASE
            WHEN team_id IS NULL THEN 'TEAM_001'
            WHEN team_category_input IS NOT NULL AND team_category IS NULL THEN 'TEAM_002'
            WHEN team_status_input IS NOT NULL AND team_status IS NULL THEN 'TEAM_003'
            WHEN country_input IS NOT NULL AND country_code IS NULL THEN 'TEAM_004'
            WHEN formation_date_raw IS NOT NULL AND formation_date_raw <> '' AND formation_date IS NULL THEN 'TEAM_005'
            WHEN dissolution_date_raw IS NOT NULL AND dissolution_date_raw <> '' AND dissolution_date IS NULL THEN 'TEAM_006'
            WHEN formation_date IS NOT NULL AND dissolution_date IS NOT NULL AND dissolution_date < formation_date THEN 'TEAM_007'
        END AS rule_id,
        CASE
            WHEN team_id IS NULL THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN team_id IS NULL THEN 'team_id could not be resolved.'
            WHEN team_category_input IS NOT NULL AND team_category IS NULL THEN concat('Invalid team category ''', team_category_input, '''.')
            WHEN team_status_input IS NOT NULL AND team_status IS NULL THEN concat('Invalid team status ''', team_status_input, '''.')
            WHEN country_input IS NOT NULL AND country_code IS NULL THEN concat('Invalid country value ''', country_input, '''.')
            WHEN formation_date_raw IS NOT NULL AND formation_date_raw <> '' AND formation_date IS NULL THEN concat('Invalid formation_date value ''', formation_date_raw, '''.')
            WHEN dissolution_date_raw IS NOT NULL AND dissolution_date_raw <> '' AND dissolution_date IS NULL THEN concat('Invalid dissolution_date value ''', dissolution_date_raw, '''.')
            WHEN formation_date IS NOT NULL AND dissolution_date IS NOT NULL AND dissolution_date < formation_date THEN 'dissolution_date cannot be before formation_date.'
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'team_id', team_id,
                'team_name', team_name,
                'team_category_input', team_category_input,
                'team_status_input', team_status_input,
                'country_input', country_input,
                'formation_date_raw', formation_date_raw,
                'dissolution_date_raw', dissolution_date_raw,
                'status_input', status_input
            )
        ) AS source_record_json
    FROM validated_source
    WHERE team_id IS NULL
       OR (team_category_input IS NOT NULL AND team_category IS NULL)
       OR (team_status_input IS NOT NULL AND team_status IS NULL)
       OR (country_input IS NOT NULL AND country_code IS NULL)
       OR (formation_date_raw IS NOT NULL AND formation_date_raw <> '' AND formation_date IS NULL)
       OR (dissolution_date_raw IS NOT NULL AND dissolution_date_raw <> '' AND dissolution_date IS NULL)
       OR (formation_date IS NOT NULL AND dissolution_date IS NOT NULL AND dissolution_date < formation_date)
),
valid_rows AS (
    SELECT
        team_id,
        team_name,
        team_category,
        country_code,
        team_status,
        formation_date,
        dissolution_date,
        CASE
            WHEN status_input IN ('TRUE', 'T', 'YES', 'Y', '1', 'ACTIVE') THEN true
            WHEN status_input IN ('FALSE', 'F', 'NO', 'N', '0', 'INACTIVE') THEN false
            WHEN team_status = 'ACTIVE' THEN true
            WHEN team_status IS NOT NULL THEN false
            ELSE NULL
        END AS active_flag,
        CASE
            WHEN formation_date IS NOT NULL AND as_of_date IS NOT NULL THEN DATEDIFF(as_of_date, formation_date)
            ELSE NULL
        END AS team_age_days
    FROM validated_source
    WHERE team_id IS NOT NULL
      AND NOT (
          (team_category_input IS NOT NULL AND team_category IS NULL)
          OR (team_status_input IS NOT NULL AND team_status IS NULL)
          OR (country_input IS NOT NULL AND country_code IS NULL)
          OR (formation_date_raw IS NOT NULL AND formation_date_raw <> '' AND formation_date IS NULL)
          OR (dissolution_date_raw IS NOT NULL AND dissolution_date_raw <> '' AND dissolution_date IS NULL)
          OR (formation_date IS NOT NULL AND dissolution_date IS NOT NULL AND dissolution_date < formation_date)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY team_id
            ORDER BY
                (
                    CASE WHEN team_name IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_category IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_status IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN formation_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN dissolution_date IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(team_id, '<NULL>'),
                        coalesce(team_name, '<NULL>'),
                        coalesce(team_category, '<NULL>'),
                        coalesce(country_code, '<NULL>'),
                        coalesce(team_status, '<NULL>'),
                        coalesce(cast(formation_date as string), '<NULL>'),
                        coalesce(cast(dissolution_date as string), '<NULL>')
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
    team_id,
    sha2(coalesce(team_id, '<NULL>'), 256) AS team_sk,
    team_name,
    team_category,
    country_code,
    team_status,
    formation_date,
    dissolution_date,
    active_flag,
    team_age_days,
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
    'teams' AS source_table,
    'teams' AS target_table,
    team_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'TEAM_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'team_id', team_id,
            'team_name', team_name,
            'team_category', team_category,
            'country_code', country_code,
            'team_status', team_status,
            'formation_date', formation_date,
            'dissolution_date', dissolution_date
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'team_id', team_id,
                'team_name', team_name,
                'team_category', team_category,
                'country_code', country_code,
                'team_status', team_status,
                'formation_date', formation_date,
                'dissolution_date', dissolution_date
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
                "NULLIF(TRIM(CAST(COALESCE(team_id, id) AS STRING)), '') AS team_id",
                "NULLIF(TRIM(CAST(COALESCE(team_name, name) AS STRING)), '') AS team_name",
                "NULLIF(UPPER(TRIM(CAST(COALESCE(team_category, category, team_type) AS STRING))), '') AS team_category_input",
                "NULLIF(UPPER(TRIM(CAST(COALESCE(team_status, status) AS STRING))), '') AS team_status_input",
                "NULLIF(UPPER(TRIM(CAST(COALESCE(country_code, country) AS STRING))), '') AS country_input",
                "TRIM(CAST(COALESCE(formation_date, start_date) AS STRING)) AS formation_date_raw",
                "TRIM(CAST(COALESCE(dissolution_date, end_date) AS STRING)) AS dissolution_date_raw",
                "NULLIF(UPPER(TRIM(CAST(COALESCE(active_flag, status) AS STRING))), '') AS status_input",
            ],
        ),
        business_key_duplicate_count_sql=f"""
WITH release_context AS (
    SELECT MAX(batch_date) AS as_of_date
    FROM {monthly_batches_fqn}
),
valid_rows AS (
    SELECT DISTINCT
        NULLIF(TRIM(CAST(COALESCE(team_id, id) AS STRING)), '') AS team_id,
        NULLIF(TRIM(CAST(COALESCE(team_name, name) AS STRING)), '') AS team_name,
        {team_category_expr} AS team_category,
        {country_expr} AS country_code,
        {team_status_expr} AS team_status,
        TO_DATE(TRIM(CAST(COALESCE(formation_date, start_date) AS STRING))) AS formation_date,
        TO_DATE(TRIM(CAST(COALESCE(dissolution_date, end_date) AS STRING))) AS dissolution_date
    FROM {source_table_fqn}
    CROSS JOIN release_context
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY team_id
            ORDER BY
                (
                    CASE WHEN team_name IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_category IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN team_status IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN formation_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN dissolution_date IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(team_id, '<NULL>'),
                        coalesce(team_name, '<NULL>'),
                        coalesce(team_category, '<NULL>'),
                        coalesce(country_code, '<NULL>'),
                        coalesce(team_status, '<NULL>'),
                        coalesce(cast(formation_date as string), '<NULL>'),
                        coalesce(cast(dissolution_date as string), '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
    WHERE team_id IS NOT NULL
      AND NOT (
          (formation_date IS NOT NULL AND dissolution_date IS NOT NULL AND dissolution_date < formation_date)
      )
)
SELECT COUNT(*) AS value
FROM ranked_rows
WHERE duplicate_rank > 1
""".strip(),
    )


def _build_club_memberships_sql_plan(
    context: PipelineContext,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    players_fqn = f"{silver_schema_fqn}.players"
    clubs_fqn = f"{silver_schema_fqn}.clubs"
    monthly_batches_fqn = f"{silver_schema_fqn}.monthly_batches"
    metadata_sql = _metadata_sql(
        context,
        source_table="club_memberships",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(club_membership_id, '<NULL>'), "
            "coalesce(player_id, '<NULL>'), coalesce(club_id, '<NULL>'), "
            "coalesce(cast(membership_start_date as string), '<NULL>'), "
            "coalesce(cast(membership_end_date as string), '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH release_context AS (
    SELECT MAX(batch_date) AS as_of_date
    FROM {monthly_batches_fqn}
),
normalized_source AS (
    SELECT
        NULLIF(TRIM(CAST(COALESCE(club_membership_id, id) AS STRING)), '') AS club_membership_id,
        NULLIF(TRIM(CAST(player_id AS STRING)), '') AS player_id,
        NULLIF(TRIM(CAST(club_id AS STRING)), '') AS club_id,
        TRIM(CAST(COALESCE(membership_start_date, start_date) AS STRING)) AS membership_start_date_raw,
        TRIM(CAST(COALESCE(membership_end_date, end_date) AS STRING)) AS membership_end_date_raw
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT * FROM normalized_source
),
typed_source AS (
    SELECT
        source.*,
        TO_DATE(source.membership_start_date_raw) AS membership_start_date,
        TO_DATE(source.membership_end_date_raw) AS membership_end_date
    FROM deduped_source source
),
validated_source AS (
    SELECT
        source.*,
        player.player_sk,
        club.club_sk,
        release_context.as_of_date
    FROM typed_source source
    CROSS JOIN release_context
    LEFT JOIN {players_fqn} player
        ON source.player_id = player.player_id
    LEFT JOIN {clubs_fqn} club
        ON source.club_id = club.club_id
),
invalid_rows AS (
    SELECT
        'club_memberships' AS source_table,
        'club_memberships' AS target_table,
        COALESCE(club_membership_id, '<NULL>') AS source_business_key,
        CASE
            WHEN club_membership_id IS NULL THEN 'MISSING_PRIMARY_KEY'
            WHEN player_id IS NULL OR player_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN club_id IS NULL OR club_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN membership_start_date_raw IS NOT NULL AND membership_start_date_raw <> '' AND membership_start_date IS NULL THEN 'INVALID_DATE'
            WHEN membership_end_date_raw IS NOT NULL AND membership_end_date_raw <> '' AND membership_end_date IS NULL THEN 'INVALID_DATE'
            WHEN membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL AND membership_end_date < membership_start_date THEN 'INVALID_DATE_RANGE'
        END AS reject_reason,
        CASE
            WHEN club_membership_id IS NULL THEN 'CLUB_MEMBERSHIP_001'
            WHEN player_id IS NULL OR player_sk IS NULL THEN 'CLUB_MEMBERSHIP_002'
            WHEN club_id IS NULL OR club_sk IS NULL THEN 'CLUB_MEMBERSHIP_003'
            WHEN membership_start_date_raw IS NOT NULL AND membership_start_date_raw <> '' AND membership_start_date IS NULL THEN 'CLUB_MEMBERSHIP_004'
            WHEN membership_end_date_raw IS NOT NULL AND membership_end_date_raw <> '' AND membership_end_date IS NULL THEN 'CLUB_MEMBERSHIP_005'
            WHEN membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL AND membership_end_date < membership_start_date THEN 'CLUB_MEMBERSHIP_006'
        END AS rule_id,
        CASE
            WHEN club_membership_id IS NULL THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN club_membership_id IS NULL THEN 'club_membership_id could not be resolved.'
            WHEN player_id IS NULL OR player_sk IS NULL THEN concat('player_id ''', player_id, ''' was not found in accepted players.')
            WHEN club_id IS NULL OR club_sk IS NULL THEN concat('club_id ''', club_id, ''' was not found in accepted clubs.')
            WHEN membership_start_date_raw IS NOT NULL AND membership_start_date_raw <> '' AND membership_start_date IS NULL THEN concat('Invalid membership_start_date value ''', membership_start_date_raw, '''.')
            WHEN membership_end_date_raw IS NOT NULL AND membership_end_date_raw <> '' AND membership_end_date IS NULL THEN concat('Invalid membership_end_date value ''', membership_end_date_raw, '''.')
            WHEN membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL AND membership_end_date < membership_start_date THEN 'membership_end_date cannot be before membership_start_date.'
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'club_membership_id', club_membership_id,
                'player_id', player_id,
                'club_id', club_id,
                'membership_start_date_raw', membership_start_date_raw,
                'membership_end_date_raw', membership_end_date_raw
            )
        ) AS source_record_json
    FROM validated_source
    WHERE club_membership_id IS NULL
       OR player_id IS NULL
       OR player_sk IS NULL
       OR club_id IS NULL
       OR club_sk IS NULL
       OR (membership_start_date_raw IS NOT NULL AND membership_start_date_raw <> '' AND membership_start_date IS NULL)
       OR (membership_end_date_raw IS NOT NULL AND membership_end_date_raw <> '' AND membership_end_date IS NULL)
       OR (membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL AND membership_end_date < membership_start_date)
),
valid_rows AS (
    SELECT
        club_membership_id,
        player_id,
        player_sk,
        club_id,
        club_sk,
        membership_start_date,
        membership_end_date,
        CASE
            WHEN membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL
                THEN DATEDIFF(membership_end_date, membership_start_date)
            ELSE NULL
        END AS membership_duration_days,
        CASE
            WHEN COALESCE(as_of_date, membership_start_date) IS NULL THEN NULL
            WHEN membership_start_date IS NOT NULL AND membership_start_date > COALESCE(as_of_date, membership_start_date) THEN false
            WHEN membership_end_date IS NOT NULL AND membership_end_date < COALESCE(as_of_date, membership_start_date) THEN false
            ELSE true
        END AS current_membership_flag
    FROM validated_source
    WHERE club_membership_id IS NOT NULL
      AND player_id IS NOT NULL
      AND player_sk IS NOT NULL
      AND club_id IS NOT NULL
      AND club_sk IS NOT NULL
      AND NOT (
          (membership_start_date_raw IS NOT NULL AND membership_start_date_raw <> '' AND membership_start_date IS NULL)
          OR (membership_end_date_raw IS NOT NULL AND membership_end_date_raw <> '' AND membership_end_date IS NULL)
          OR (membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL AND membership_end_date < membership_start_date)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY club_membership_id
            ORDER BY
                (
                    CASE WHEN player_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN club_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN membership_start_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN membership_end_date IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(club_membership_id, '<NULL>'),
                        coalesce(player_id, '<NULL>'),
                        coalesce(club_id, '<NULL>'),
                        coalesce(cast(membership_start_date as string), '<NULL>'),
                        coalesce(cast(membership_end_date as string), '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
),
accepted_rows AS (
    SELECT
        club_membership_id,
        sha2(coalesce(club_membership_id, '<NULL>'), 256) AS club_membership_sk,
        player_id,
        player_sk,
        club_id,
        club_sk,
        membership_start_date,
        membership_end_date,
        membership_duration_days,
        current_membership_flag,
        CASE
            WHEN LAG(
                CASE
                    WHEN membership_end_date IS NOT NULL THEN membership_end_date
                    ELSE DATE '9999-12-31'
                END
            ) OVER (
                PARTITION BY player_id, club_id
                ORDER BY membership_start_date ASC NULLS FIRST,
                         membership_end_date ASC NULLS LAST,
                         club_membership_id ASC
            ) IS NOT NULL
             AND membership_start_date IS NOT NULL
             AND membership_start_date <= LAG(
                CASE
                    WHEN membership_end_date IS NOT NULL THEN membership_end_date
                    ELSE DATE '9999-12-31'
                END
            ) OVER (
                PARTITION BY player_id, club_id
                ORDER BY membership_start_date ASC NULLS FIRST,
                         membership_end_date ASC NULLS LAST,
                         club_membership_id ASC
            )
                THEN true
            ELSE false
        END AS membership_overlap_flag
    FROM ranked_rows
    WHERE duplicate_rank = 1
)
""".strip()
    accepted_sql = f"""
{base_ctes}
SELECT
    club_membership_id,
    club_membership_sk,
    player_id,
    player_sk,
    club_id,
    club_sk,
    membership_start_date,
    membership_end_date,
    membership_duration_days,
    current_membership_flag,
    membership_overlap_flag,
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
    'club_memberships' AS source_table,
    'club_memberships' AS target_table,
    club_membership_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'CLUB_MEMBERSHIP_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'club_membership_id', club_membership_id,
            'player_id', player_id,
            'club_id', club_id,
            'membership_start_date', membership_start_date,
            'membership_end_date', membership_end_date
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'club_membership_id', club_membership_id,
                'player_id', player_id,
                'club_id', club_id,
                'membership_start_date', membership_start_date,
                'membership_end_date', membership_end_date
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
                "NULLIF(TRIM(CAST(COALESCE(club_membership_id, id) AS STRING)), '') AS club_membership_id",
                "NULLIF(TRIM(CAST(player_id AS STRING)), '') AS player_id",
                "NULLIF(TRIM(CAST(club_id AS STRING)), '') AS club_id",
                "TRIM(CAST(COALESCE(membership_start_date, start_date) AS STRING)) AS membership_start_date_raw",
                "TRIM(CAST(COALESCE(membership_end_date, end_date) AS STRING)) AS membership_end_date_raw",
            ],
        ),
        business_key_duplicate_count_sql=f"""
WITH valid_rows AS (
    SELECT DISTINCT
        NULLIF(TRIM(CAST(COALESCE(club_membership_id, id) AS STRING)), '') AS club_membership_id,
        NULLIF(TRIM(CAST(player_id AS STRING)), '') AS player_id,
        NULLIF(TRIM(CAST(club_id AS STRING)), '') AS club_id,
        TO_DATE(TRIM(CAST(COALESCE(membership_start_date, start_date) AS STRING))) AS membership_start_date,
        TO_DATE(TRIM(CAST(COALESCE(membership_end_date, end_date) AS STRING))) AS membership_end_date
    FROM {source_table_fqn} source
    LEFT JOIN {players_fqn} player
        ON NULLIF(TRIM(CAST(source.player_id AS STRING)), '') = player.player_id
    LEFT JOIN {clubs_fqn} club
        ON NULLIF(TRIM(CAST(source.club_id AS STRING)), '') = club.club_id
    WHERE NULLIF(TRIM(CAST(COALESCE(club_membership_id, id) AS STRING)), '') IS NOT NULL
      AND NULLIF(TRIM(CAST(source.player_id AS STRING)), '') IS NOT NULL
      AND player.player_sk IS NOT NULL
      AND NULLIF(TRIM(CAST(source.club_id AS STRING)), '') IS NOT NULL
      AND club.club_sk IS NOT NULL
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY club_membership_id
            ORDER BY
                (
                    CASE WHEN player_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN club_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN membership_start_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN membership_end_date IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(club_membership_id, '<NULL>'),
                        coalesce(player_id, '<NULL>'),
                        coalesce(club_id, '<NULL>'),
                        coalesce(cast(membership_start_date as string), '<NULL>'),
                        coalesce(cast(membership_end_date as string), '<NULL>')
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
        warning_count_sql="""
SELECT COUNT(*) AS value
FROM {silver_schema_fqn}.club_memberships
WHERE membership_overlap_flag = true
""".strip().format(silver_schema_fqn=silver_schema_fqn),
    )


def _build_team_memberships_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    players_fqn = f"{silver_schema_fqn}.players"
    teams_fqn = f"{silver_schema_fqn}.teams"
    monthly_batches_fqn = f"{silver_schema_fqn}.monthly_batches"
    position_expr = _domain_case_expression("position_input", config.data["domains"]["player_position"])
    metadata_sql = _metadata_sql(
        context,
        source_table="team_memberships",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(team_membership_id, '<NULL>'), "
            "coalesce(team_id, '<NULL>'), coalesce(player_id, '<NULL>'), "
            "coalesce(cast(membership_start_date as string), '<NULL>'), "
            "coalesce(cast(membership_end_date as string), '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH release_context AS (
    SELECT MAX(batch_date) AS as_of_date
    FROM {monthly_batches_fqn}
),
normalized_source AS (
    SELECT
        NULLIF(TRIM(CAST(COALESCE(team_membership_id, id) AS STRING)), '') AS team_membership_id,
        NULLIF(TRIM(CAST(player_id AS STRING)), '') AS player_id,
        NULLIF(TRIM(CAST(team_id AS STRING)), '') AS team_id,
        TRIM(CAST(COALESCE(membership_start_date, start_date) AS STRING)) AS membership_start_date_raw,
        TRIM(CAST(COALESCE(membership_end_date, end_date) AS STRING)) AS membership_end_date_raw,
        NULLIF(UPPER(TRIM(CAST(COALESCE(player_role, role) AS STRING))), '') AS player_role,
        NULLIF(UPPER(TRIM(CAST(COALESCE(player_position, preferred_side, position) AS STRING))), '') AS position_input
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT * FROM normalized_source
),
typed_source AS (
    SELECT
        source.*,
        TO_DATE(source.membership_start_date_raw) AS membership_start_date,
        TO_DATE(source.membership_end_date_raw) AS membership_end_date,
        {position_expr} AS player_position
    FROM deduped_source source
),
validated_source AS (
    SELECT
        source.*,
        player.player_sk,
        team.team_sk,
        release_context.as_of_date
    FROM typed_source source
    CROSS JOIN release_context
    LEFT JOIN {players_fqn} player
        ON source.player_id = player.player_id
    LEFT JOIN {teams_fqn} team
        ON source.team_id = team.team_id
),
invalid_rows AS (
    SELECT
        'team_memberships' AS source_table,
        'team_memberships' AS target_table,
        COALESCE(team_membership_id, '<NULL>') AS source_business_key,
        CASE
            WHEN team_membership_id IS NULL THEN 'MISSING_PRIMARY_KEY'
            WHEN player_id IS NULL OR player_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN team_id IS NULL OR team_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN membership_start_date_raw IS NOT NULL AND membership_start_date_raw <> '' AND membership_start_date IS NULL THEN 'INVALID_DATE'
            WHEN membership_end_date_raw IS NOT NULL AND membership_end_date_raw <> '' AND membership_end_date IS NULL THEN 'INVALID_DATE'
            WHEN membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL AND membership_end_date < membership_start_date THEN 'INVALID_DATE_RANGE'
            WHEN position_input IS NOT NULL AND player_position IS NULL THEN 'INVALID_DOMAIN_VALUE'
        END AS reject_reason,
        CASE
            WHEN team_membership_id IS NULL THEN 'TEAM_MEMBERSHIP_001'
            WHEN player_id IS NULL OR player_sk IS NULL THEN 'TEAM_MEMBERSHIP_002'
            WHEN team_id IS NULL OR team_sk IS NULL THEN 'TEAM_MEMBERSHIP_003'
            WHEN membership_start_date_raw IS NOT NULL AND membership_start_date_raw <> '' AND membership_start_date IS NULL THEN 'TEAM_MEMBERSHIP_004'
            WHEN membership_end_date_raw IS NOT NULL AND membership_end_date_raw <> '' AND membership_end_date IS NULL THEN 'TEAM_MEMBERSHIP_005'
            WHEN membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL AND membership_end_date < membership_start_date THEN 'TEAM_MEMBERSHIP_006'
            WHEN position_input IS NOT NULL AND player_position IS NULL THEN 'TEAM_MEMBERSHIP_007'
        END AS rule_id,
        CASE
            WHEN team_membership_id IS NULL THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN team_membership_id IS NULL THEN 'team_membership_id could not be resolved.'
            WHEN player_id IS NULL OR player_sk IS NULL THEN concat('player_id ''', player_id, ''' was not found in accepted players.')
            WHEN team_id IS NULL OR team_sk IS NULL THEN concat('team_id ''', team_id, ''' was not found in accepted teams.')
            WHEN membership_start_date_raw IS NOT NULL AND membership_start_date_raw <> '' AND membership_start_date IS NULL THEN concat('Invalid membership_start_date value ''', membership_start_date_raw, '''.')
            WHEN membership_end_date_raw IS NOT NULL AND membership_end_date_raw <> '' AND membership_end_date IS NULL THEN concat('Invalid membership_end_date value ''', membership_end_date_raw, '''.')
            WHEN membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL AND membership_end_date < membership_start_date THEN 'membership_end_date cannot be before membership_start_date.'
            WHEN position_input IS NOT NULL AND player_position IS NULL THEN concat('Invalid player_position value ''', position_input, '''.')
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'team_membership_id', team_membership_id,
                'player_id', player_id,
                'team_id', team_id,
                'membership_start_date_raw', membership_start_date_raw,
                'membership_end_date_raw', membership_end_date_raw,
                'player_role', player_role,
                'position_input', position_input
            )
        ) AS source_record_json
    FROM validated_source
    WHERE team_membership_id IS NULL
       OR player_id IS NULL
       OR player_sk IS NULL
       OR team_id IS NULL
       OR team_sk IS NULL
       OR (membership_start_date_raw IS NOT NULL AND membership_start_date_raw <> '' AND membership_start_date IS NULL)
       OR (membership_end_date_raw IS NOT NULL AND membership_end_date_raw <> '' AND membership_end_date IS NULL)
       OR (membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL AND membership_end_date < membership_start_date)
       OR (position_input IS NOT NULL AND player_position IS NULL)
),
valid_rows AS (
    SELECT
        team_membership_id,
        team_id,
        team_sk,
        player_id,
        player_sk,
        membership_start_date,
        membership_end_date,
        player_role,
        player_position,
        CASE
            WHEN membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL
                THEN DATEDIFF(membership_end_date, membership_start_date)
            ELSE NULL
        END AS membership_duration_days,
        CASE
            WHEN COALESCE(as_of_date, membership_start_date) IS NULL THEN NULL
            WHEN membership_start_date IS NOT NULL AND membership_start_date > COALESCE(as_of_date, membership_start_date) THEN false
            WHEN membership_end_date IS NOT NULL AND membership_end_date < COALESCE(as_of_date, membership_start_date) THEN false
            ELSE true
        END AS current_membership_flag
    FROM validated_source
    WHERE team_membership_id IS NOT NULL
      AND team_id IS NOT NULL
      AND team_sk IS NOT NULL
      AND player_id IS NOT NULL
      AND player_sk IS NOT NULL
      AND NOT (
          (membership_start_date_raw IS NOT NULL AND membership_start_date_raw <> '' AND membership_start_date IS NULL)
          OR (membership_end_date_raw IS NOT NULL AND membership_end_date_raw <> '' AND membership_end_date IS NULL)
          OR (membership_start_date IS NOT NULL AND membership_end_date IS NOT NULL AND membership_end_date < membership_start_date)
          OR (position_input IS NOT NULL AND player_position IS NULL)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY team_membership_id
            ORDER BY
                (
                    CASE WHEN team_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN player_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN membership_start_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN membership_end_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN player_position IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(team_membership_id, '<NULL>'),
                        coalesce(team_id, '<NULL>'),
                        coalesce(player_id, '<NULL>'),
                        coalesce(cast(membership_start_date as string), '<NULL>'),
                        coalesce(cast(membership_end_date as string), '<NULL>'),
                        coalesce(player_position, '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM valid_rows
),
accepted_rows AS (
    SELECT
        team_membership_id,
        sha2(coalesce(team_membership_id, '<NULL>'), 256) AS team_membership_sk,
        team_id,
        team_sk,
        player_id,
        player_sk,
        membership_start_date,
        membership_end_date,
        player_role,
        player_position,
        membership_duration_days,
        current_membership_flag,
        CASE
            WHEN LAG(
                CASE
                    WHEN membership_end_date IS NOT NULL THEN membership_end_date
                    ELSE DATE '9999-12-31'
                END
            ) OVER (
                PARTITION BY player_id, team_id
                ORDER BY membership_start_date ASC NULLS FIRST,
                         membership_end_date ASC NULLS LAST,
                         team_membership_id ASC
            ) IS NOT NULL
             AND membership_start_date IS NOT NULL
             AND membership_start_date <= LAG(
                CASE
                    WHEN membership_end_date IS NOT NULL THEN membership_end_date
                    ELSE DATE '9999-12-31'
                END
            ) OVER (
                PARTITION BY player_id, team_id
                ORDER BY membership_start_date ASC NULLS FIRST,
                         membership_end_date ASC NULLS LAST,
                         team_membership_id ASC
            )
                THEN true
            ELSE false
        END AS membership_overlap_flag
    FROM ranked_rows
    WHERE duplicate_rank = 1
)
""".strip()
    accepted_sql = f"""
{base_ctes}
SELECT
    team_membership_id,
    team_membership_sk,
    team_id,
    team_sk,
    player_id,
    player_sk,
    membership_start_date,
    membership_end_date,
    player_role,
    player_position,
    membership_duration_days,
    current_membership_flag,
    membership_overlap_flag,
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
    'team_memberships' AS source_table,
    'team_memberships' AS target_table,
    team_membership_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'TEAM_MEMBERSHIP_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'team_membership_id', team_membership_id,
            'team_id', team_id,
            'player_id', player_id,
            'membership_start_date', membership_start_date,
            'membership_end_date', membership_end_date,
            'player_role', player_role,
            'player_position', player_position
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'team_membership_id', team_membership_id,
                'team_id', team_id,
                'player_id', player_id,
                'membership_start_date', membership_start_date,
                'membership_end_date', membership_end_date,
                'player_role', player_role,
                'player_position', player_position
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
                "NULLIF(TRIM(CAST(COALESCE(team_membership_id, id) AS STRING)), '') AS team_membership_id",
                "NULLIF(TRIM(CAST(player_id AS STRING)), '') AS player_id",
                "NULLIF(TRIM(CAST(team_id AS STRING)), '') AS team_id",
                "TRIM(CAST(COALESCE(membership_start_date, start_date) AS STRING)) AS membership_start_date_raw",
                "TRIM(CAST(COALESCE(membership_end_date, end_date) AS STRING)) AS membership_end_date_raw",
                "NULLIF(UPPER(TRIM(CAST(COALESCE(player_role, role) AS STRING))), '') AS player_role",
                "NULLIF(UPPER(TRIM(CAST(COALESCE(player_position, preferred_side, position) AS STRING))), '') AS position_input",
            ],
        ),
        business_key_duplicate_count_sql=f"""
WITH valid_rows AS (
    SELECT DISTINCT
        NULLIF(TRIM(CAST(COALESCE(team_membership_id, id) AS STRING)), '') AS team_membership_id,
        NULLIF(TRIM(CAST(player_id AS STRING)), '') AS player_id,
        NULLIF(TRIM(CAST(team_id AS STRING)), '') AS team_id,
        TO_DATE(TRIM(CAST(COALESCE(membership_start_date, start_date) AS STRING))) AS membership_start_date,
        TO_DATE(TRIM(CAST(COALESCE(membership_end_date, end_date) AS STRING))) AS membership_end_date,
        {position_expr} AS player_position
    FROM {source_table_fqn} source
    LEFT JOIN {players_fqn} player
        ON NULLIF(TRIM(CAST(source.player_id AS STRING)), '') = player.player_id
    LEFT JOIN {teams_fqn} team
        ON NULLIF(TRIM(CAST(source.team_id AS STRING)), '') = team.team_id
    WHERE NULLIF(TRIM(CAST(COALESCE(team_membership_id, id) AS STRING)), '') IS NOT NULL
      AND NULLIF(TRIM(CAST(source.player_id AS STRING)), '') IS NOT NULL
      AND player.player_sk IS NOT NULL
      AND NULLIF(TRIM(CAST(source.team_id AS STRING)), '') IS NOT NULL
      AND team.team_sk IS NOT NULL
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY team_membership_id
            ORDER BY
                (
                    CASE WHEN team_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN player_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN membership_start_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN membership_end_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN player_position IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(team_membership_id, '<NULL>'),
                        coalesce(team_id, '<NULL>'),
                        coalesce(player_id, '<NULL>'),
                        coalesce(cast(membership_start_date as string), '<NULL>'),
                        coalesce(cast(membership_end_date as string), '<NULL>'),
                        coalesce(player_position, '<NULL>')
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
        warning_count_sql="""
SELECT COUNT(*) AS value
FROM {silver_schema_fqn}.team_memberships
WHERE membership_overlap_flag = true
""".strip().format(silver_schema_fqn=silver_schema_fqn),
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
