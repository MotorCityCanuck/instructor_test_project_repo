"""Spark SQL plans for Bronze-to-Silver athlete-table execution."""

from __future__ import annotations

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.operations import PipelineContext
from napa_pipeline.bronze_to_silver.reference_sql import SqlReferenceBuildPlan, sql_literal


def supports_athlete_sql_transform(transform_name: str) -> bool:
    """Return whether an athlete transform has a Spark SQL execution plan."""
    return transform_name == "build_players"


def build_athlete_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    target_table: str,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    """Return the SQL execution plan for one supported athlete table."""
    if target_table == "players":
        return _build_players_sql_plan(config, context, source_table_fqn, silver_schema_fqn)
    raise ValueError(f"No SQL athlete plan is defined for target table '{target_table}'.")


def _build_players_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    source_table_fqn: str,
    silver_schema_fqn: str,
) -> SqlReferenceBuildPlan:
    regions_fqn = f"{silver_schema_fqn}.regions"
    monthly_batches_fqn = f"{silver_schema_fqn}.monthly_batches"
    gender_expr = _domain_case_expression("gender_input", config.data["domains"]["gender"])
    dominant_hand_expr = _domain_case_expression("dominant_hand_input", config.data["domains"]["dominant_hand"])
    preferred_side_expr = _domain_case_expression("preferred_side_input", config.data["domains"]["player_position"])
    country_expr = _domain_case_expression("country_input", config.data["domains"]["country_code"])
    metadata_sql = _metadata_sql(
        context,
        source_table="player_master",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(player_id, '<NULL>'), "
            "coalesce(first_name, '<NULL>'), coalesce(last_name, '<NULL>'), "
            "coalesce(cast(birth_date as string), '<NULL>'), coalesce(home_region_id, '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH release_context AS (
    SELECT MAX(batch_date) AS as_of_date
    FROM {monthly_batches_fqn}
),
normalized_source AS (
    SELECT
        NULLIF(TRIM(CAST(COALESCE(player_id, id) AS STRING)), '') AS player_id,
        NULLIF(TRIM(CAST(first_name AS STRING)), '') AS first_name,
        NULLIF(TRIM(CAST(last_name AS STRING)), '') AS last_name,
        NULLIF(TRIM(CAST(COALESCE(display_name, full_name) AS STRING)), '') AS explicit_display_name,
        TRIM(CAST(COALESCE(birth_date, date_of_birth, dob) AS STRING)) AS birth_date_raw,
        NULLIF(UPPER(TRIM(CAST(gender AS STRING))), '') AS gender_input,
        NULLIF(UPPER(TRIM(CAST(COALESCE(dominant_hand, handedness) AS STRING))), '') AS dominant_hand_input,
        NULLIF(UPPER(TRIM(CAST(COALESCE(preferred_side, preferred_position) AS STRING))), '') AS preferred_side_input,
        NULLIF(TRIM(CAST(COALESCE(home_region_id, region_id) AS STRING)), '') AS home_region_id,
        NULLIF(UPPER(TRIM(CAST(COALESCE(country_code, country) AS STRING))), '') AS country_input,
        NULLIF(TRIM(CAST(COALESCE(rating, player_rating) AS STRING)), '') AS rating_raw,
        NULLIF(TRIM(CAST(COALESCE(rating_confidence, confidence) AS STRING)), '') AS rating_confidence_raw,
        NULLIF(UPPER(TRIM(CAST(COALESCE(active_flag, status) AS STRING))), '') AS status_input
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT
        player_id,
        first_name,
        last_name,
        explicit_display_name,
        birth_date_raw,
        gender_input,
        dominant_hand_input,
        preferred_side_input,
        home_region_id,
        country_input,
        rating_raw,
        rating_confidence_raw,
        status_input
    FROM normalized_source
),
typed_source AS (
    SELECT
        source.*,
        TO_DATE(source.birth_date_raw) AS birth_date,
        CAST(source.rating_raw AS DOUBLE) AS rating_value,
        CAST(source.rating_confidence_raw AS DOUBLE) AS rating_confidence_value,
        {gender_expr} AS gender,
        {dominant_hand_expr} AS dominant_hand,
        {preferred_side_expr} AS preferred_side,
        {country_expr} AS country_code
    FROM deduped_source source
),
validated_source AS (
    SELECT
        source.*,
        region.region_sk AS home_region_sk,
        release_context.as_of_date
    FROM typed_source source
    CROSS JOIN release_context
    LEFT JOIN {regions_fqn} region
        ON source.home_region_id = region.region_id
),
invalid_rows AS (
    SELECT
        'player_master' AS source_table,
        'players' AS target_table,
        COALESCE(player_id, '<NULL>') AS source_business_key,
        CASE
            WHEN player_id IS NULL THEN 'MISSING_PRIMARY_KEY'
            WHEN birth_date_raw IS NOT NULL AND birth_date_raw <> '' AND birth_date IS NULL THEN 'INVALID_DATE'
            WHEN birth_date IS NOT NULL AND as_of_date IS NOT NULL AND birth_date > as_of_date THEN 'INVALID_DATE_RANGE'
            WHEN home_region_id IS NOT NULL AND home_region_sk IS NULL THEN 'ORPHAN_FOREIGN_KEY'
            WHEN gender_input IS NOT NULL AND gender IS NULL THEN 'INVALID_DOMAIN_VALUE'
            WHEN dominant_hand_input IS NOT NULL AND dominant_hand IS NULL THEN 'INVALID_DOMAIN_VALUE'
            WHEN preferred_side_input IS NOT NULL AND preferred_side IS NULL THEN 'INVALID_DOMAIN_VALUE'
            WHEN country_input IS NOT NULL AND country_code IS NULL THEN 'INVALID_DOMAIN_VALUE'
            WHEN rating_raw IS NOT NULL AND rating_value IS NULL THEN 'INVALID_DATA_TYPE'
            WHEN rating_confidence_raw IS NOT NULL AND rating_confidence_value IS NULL THEN 'INVALID_DATA_TYPE'
        END AS reject_reason,
        CASE
            WHEN player_id IS NULL THEN 'PLAYER_001'
            WHEN birth_date_raw IS NOT NULL AND birth_date_raw <> '' AND birth_date IS NULL THEN 'PLAYER_005'
            WHEN birth_date IS NOT NULL AND as_of_date IS NOT NULL AND birth_date > as_of_date THEN 'PLAYER_005'
            WHEN home_region_id IS NOT NULL AND home_region_sk IS NULL THEN 'PLAYER_002'
            WHEN gender_input IS NOT NULL AND gender IS NULL THEN 'PLAYER_003'
            WHEN dominant_hand_input IS NOT NULL AND dominant_hand IS NULL THEN 'PLAYER_004'
            WHEN preferred_side_input IS NOT NULL AND preferred_side IS NULL THEN 'PLAYER_006'
            WHEN country_input IS NOT NULL AND country_code IS NULL THEN 'PLAYER_007'
            WHEN rating_raw IS NOT NULL AND rating_value IS NULL THEN 'PLAYER_008'
            WHEN rating_confidence_raw IS NOT NULL AND rating_confidence_value IS NULL THEN 'PLAYER_009'
        END AS rule_id,
        CASE
            WHEN player_id IS NULL THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN player_id IS NULL THEN 'player_id could not be resolved.'
            WHEN birth_date_raw IS NOT NULL AND birth_date_raw <> '' AND birth_date IS NULL
                THEN concat('Invalid birth date ''', birth_date_raw, '''.')
            WHEN birth_date IS NOT NULL AND as_of_date IS NOT NULL AND birth_date > as_of_date
                THEN 'birth_date cannot be after the release as-of date.'
            WHEN home_region_id IS NOT NULL AND home_region_sk IS NULL
                THEN concat('home_region_id ''', home_region_id, ''' was not found in accepted regions.')
            WHEN gender_input IS NOT NULL AND gender IS NULL
                THEN concat('Invalid gender value ''', gender_input, '''.')
            WHEN dominant_hand_input IS NOT NULL AND dominant_hand IS NULL
                THEN concat('Invalid dominant_hand value ''', dominant_hand_input, '''.')
            WHEN preferred_side_input IS NOT NULL AND preferred_side IS NULL
                THEN concat('Invalid preferred_side value ''', preferred_side_input, '''.')
            WHEN country_input IS NOT NULL AND country_code IS NULL
                THEN concat('Invalid country value ''', country_input, '''.')
            WHEN rating_raw IS NOT NULL AND rating_value IS NULL
                THEN concat('Invalid rating value ''', rating_raw, '''.')
            WHEN rating_confidence_raw IS NOT NULL AND rating_confidence_value IS NULL
                THEN concat('Invalid rating_confidence value ''', rating_confidence_raw, '''.')
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'player_id', player_id,
                'first_name', first_name,
                'last_name', last_name,
                'explicit_display_name', explicit_display_name,
                'birth_date_raw', birth_date_raw,
                'gender_input', gender_input,
                'dominant_hand_input', dominant_hand_input,
                'preferred_side_input', preferred_side_input,
                'home_region_id', home_region_id,
                'country_input', country_input,
                'rating_raw', rating_raw,
                'rating_confidence_raw', rating_confidence_raw,
                'status_input', status_input
            )
        ) AS source_record_json
    FROM validated_source
    WHERE player_id IS NULL
       OR (birth_date_raw IS NOT NULL AND birth_date_raw <> '' AND birth_date IS NULL)
       OR (birth_date IS NOT NULL AND as_of_date IS NOT NULL AND birth_date > as_of_date)
       OR (home_region_id IS NOT NULL AND home_region_sk IS NULL)
       OR (gender_input IS NOT NULL AND gender IS NULL)
       OR (dominant_hand_input IS NOT NULL AND dominant_hand IS NULL)
       OR (preferred_side_input IS NOT NULL AND preferred_side IS NULL)
       OR (country_input IS NOT NULL AND country_code IS NULL)
       OR (rating_raw IS NOT NULL AND rating_value IS NULL)
       OR (rating_confidence_raw IS NOT NULL AND rating_confidence_value IS NULL)
),
valid_rows AS (
    SELECT
        player_id,
        first_name,
        last_name,
        CASE
            WHEN explicit_display_name IS NOT NULL THEN explicit_display_name
            WHEN first_name IS NOT NULL AND last_name IS NOT NULL THEN concat(first_name, ' ', last_name)
            WHEN first_name IS NOT NULL THEN first_name
            WHEN last_name IS NOT NULL THEN last_name
            ELSE NULL
        END AS display_name,
        birth_date,
        gender,
        dominant_hand,
        preferred_side,
        home_region_id,
        home_region_sk,
        country_code,
        CASE
            WHEN status_input IN ('TRUE', 'T', 'YES', 'Y', '1', 'ACTIVE') THEN true
            WHEN status_input IN ('FALSE', 'F', 'NO', 'N', '0', 'INACTIVE') THEN false
            ELSE NULL
        END AS active_flag,
        CASE
            WHEN birth_date IS NOT NULL AND as_of_date IS NOT NULL
                THEN YEAR(as_of_date) - YEAR(birth_date)
                     - CASE
                         WHEN MONTH(as_of_date) < MONTH(birth_date)
                              OR (MONTH(as_of_date) = MONTH(birth_date) AND DAY(as_of_date) < DAY(birth_date))
                             THEN 1
                         ELSE 0
                       END
            ELSE NULL
        END AS age,
        CAST(NULL AS STRING) AS age_group,
        rating_value AS rating,
        rating_confidence_value AS rating_confidence
    FROM validated_source
    WHERE player_id IS NOT NULL
      AND NOT (
          (birth_date_raw IS NOT NULL AND birth_date_raw <> '' AND birth_date IS NULL)
          OR (birth_date IS NOT NULL AND as_of_date IS NOT NULL AND birth_date > as_of_date)
          OR (home_region_id IS NOT NULL AND home_region_sk IS NULL)
          OR (gender_input IS NOT NULL AND gender IS NULL)
          OR (dominant_hand_input IS NOT NULL AND dominant_hand IS NULL)
          OR (preferred_side_input IS NOT NULL AND preferred_side IS NULL)
          OR (country_input IS NOT NULL AND country_code IS NULL)
          OR (rating_raw IS NOT NULL AND rating_value IS NULL)
          OR (rating_confidence_raw IS NOT NULL AND rating_confidence_value IS NULL)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY
                (
                    CASE WHEN first_name IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN last_name IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN birth_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN home_region_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN rating IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN rating_confidence IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(player_id, '<NULL>'),
                        coalesce(first_name, '<NULL>'),
                        coalesce(last_name, '<NULL>'),
                        coalesce(cast(birth_date as string), '<NULL>'),
                        coalesce(home_region_id, '<NULL>'),
                        coalesce(country_code, '<NULL>'),
                        coalesce(cast(rating as string), '<NULL>'),
                        coalesce(cast(rating_confidence as string), '<NULL>')
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
    player_id,
    sha2(coalesce(player_id, '<NULL>'), 256) AS player_sk,
    first_name,
    last_name,
    display_name,
    birth_date,
    gender,
    dominant_hand,
    preferred_side,
    home_region_id,
    home_region_sk,
    country_code,
    active_flag,
    age,
    age_group,
    rating,
    rating_confidence,
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
    'player_master' AS source_table,
    'players' AS target_table,
    player_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'PLAYER_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'player_id', player_id,
            'first_name', first_name,
            'last_name', last_name,
            'display_name', display_name,
            'birth_date', birth_date,
            'gender', gender,
            'dominant_hand', dominant_hand,
            'preferred_side', preferred_side,
            'home_region_id', home_region_id,
            'country_code', country_code,
            'rating', rating,
            'rating_confidence', rating_confidence
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'player_id', player_id,
                'first_name', first_name,
                'last_name', last_name,
                'display_name', display_name,
                'birth_date', birth_date,
                'gender', gender,
                'dominant_hand', dominant_hand,
                'preferred_side', preferred_side,
                'home_region_id', home_region_id,
                'country_code', country_code,
                'rating', rating,
                'rating_confidence', rating_confidence
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
        exact_duplicate_count_sql=f"""
WITH normalized_source AS (
    SELECT
        NULLIF(TRIM(CAST(COALESCE(player_id, id) AS STRING)), '') AS player_id,
        NULLIF(TRIM(CAST(first_name AS STRING)), '') AS first_name,
        NULLIF(TRIM(CAST(last_name AS STRING)), '') AS last_name,
        NULLIF(TRIM(CAST(COALESCE(display_name, full_name) AS STRING)), '') AS explicit_display_name,
        TRIM(CAST(COALESCE(birth_date, date_of_birth, dob) AS STRING)) AS birth_date_raw,
        NULLIF(UPPER(TRIM(CAST(gender AS STRING))), '') AS gender_input,
        NULLIF(UPPER(TRIM(CAST(COALESCE(dominant_hand, handedness) AS STRING))), '') AS dominant_hand_input,
        NULLIF(UPPER(TRIM(CAST(COALESCE(preferred_side, preferred_position) AS STRING))), '') AS preferred_side_input,
        NULLIF(TRIM(CAST(COALESCE(home_region_id, region_id) AS STRING)), '') AS home_region_id,
        NULLIF(UPPER(TRIM(CAST(COALESCE(country_code, country) AS STRING))), '') AS country_input,
        NULLIF(TRIM(CAST(COALESCE(rating, player_rating) AS STRING)), '') AS rating_raw,
        NULLIF(TRIM(CAST(COALESCE(rating_confidence, confidence) AS STRING)), '') AS rating_confidence_raw,
        NULLIF(UPPER(TRIM(CAST(COALESCE(active_flag, status) AS STRING))), '') AS status_input
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT *
    FROM normalized_source
)
SELECT
    (SELECT COUNT(*) FROM normalized_source) - (SELECT COUNT(*) FROM deduped_source) AS value
""".strip(),
        business_key_duplicate_count_sql=f"""
WITH release_context AS (
    SELECT MAX(batch_date) AS as_of_date
    FROM {monthly_batches_fqn}
),
valid_rows AS (
    SELECT DISTINCT
        source.player_id,
        source.first_name,
        source.last_name,
        CASE
            WHEN source.explicit_display_name IS NOT NULL THEN source.explicit_display_name
            WHEN source.first_name IS NOT NULL AND source.last_name IS NOT NULL THEN concat(source.first_name, ' ', source.last_name)
            WHEN source.first_name IS NOT NULL THEN source.first_name
            WHEN source.last_name IS NOT NULL THEN source.last_name
            ELSE NULL
        END AS display_name,
        TO_DATE(source.birth_date_raw) AS birth_date,
        {gender_expr} AS gender,
        {dominant_hand_expr} AS dominant_hand,
        {preferred_side_expr} AS preferred_side,
        source.home_region_id,
        region.region_sk AS home_region_sk,
        {country_expr} AS country_code,
        CAST(source.rating_raw AS DOUBLE) AS rating,
        CAST(source.rating_confidence_raw AS DOUBLE) AS rating_confidence
    FROM (
        SELECT
            NULLIF(TRIM(CAST(COALESCE(player_id, id) AS STRING)), '') AS player_id,
            NULLIF(TRIM(CAST(first_name AS STRING)), '') AS first_name,
            NULLIF(TRIM(CAST(last_name AS STRING)), '') AS last_name,
            NULLIF(TRIM(CAST(COALESCE(display_name, full_name) AS STRING)), '') AS explicit_display_name,
            TRIM(CAST(COALESCE(birth_date, date_of_birth, dob) AS STRING)) AS birth_date_raw,
            NULLIF(UPPER(TRIM(CAST(gender AS STRING))), '') AS gender_input,
            NULLIF(UPPER(TRIM(CAST(COALESCE(dominant_hand, handedness) AS STRING))), '') AS dominant_hand_input,
            NULLIF(UPPER(TRIM(CAST(COALESCE(preferred_side, preferred_position) AS STRING))), '') AS preferred_side_input,
            NULLIF(TRIM(CAST(COALESCE(home_region_id, region_id) AS STRING)), '') AS home_region_id,
            NULLIF(UPPER(TRIM(CAST(COALESCE(country_code, country) AS STRING))), '') AS country_input,
            NULLIF(TRIM(CAST(COALESCE(rating, player_rating) AS STRING)), '') AS rating_raw,
            NULLIF(TRIM(CAST(COALESCE(rating_confidence, confidence) AS STRING)), '') AS rating_confidence_raw
        FROM {source_table_fqn}
    ) source
    CROSS JOIN release_context
    LEFT JOIN {regions_fqn} region
        ON source.home_region_id = region.region_id
    WHERE source.player_id IS NOT NULL
      AND NOT (
          (source.birth_date_raw IS NOT NULL AND source.birth_date_raw <> '' AND TO_DATE(source.birth_date_raw) IS NULL)
          OR (TO_DATE(source.birth_date_raw) IS NOT NULL AND release_context.as_of_date IS NOT NULL AND TO_DATE(source.birth_date_raw) > release_context.as_of_date)
          OR (source.home_region_id IS NOT NULL AND region.region_sk IS NULL)
          OR (source.gender_input IS NOT NULL AND {gender_expr} IS NULL)
          OR (source.dominant_hand_input IS NOT NULL AND {dominant_hand_expr} IS NULL)
          OR (source.preferred_side_input IS NOT NULL AND {preferred_side_expr} IS NULL)
          OR (source.country_input IS NOT NULL AND {country_expr} IS NULL)
          OR (source.rating_raw IS NOT NULL AND CAST(source.rating_raw AS DOUBLE) IS NULL)
          OR (source.rating_confidence_raw IS NOT NULL AND CAST(source.rating_confidence_raw AS DOUBLE) IS NULL)
      )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY
                (
                    CASE WHEN first_name IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN last_name IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN birth_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN home_region_id IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN rating IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN rating_confidence IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(player_id, '<NULL>'),
                        coalesce(first_name, '<NULL>'),
                        coalesce(last_name, '<NULL>'),
                        coalesce(cast(birth_date as string), '<NULL>'),
                        coalesce(home_region_id, '<NULL>'),
                        coalesce(country_code, '<NULL>'),
                        coalesce(cast(rating as string), '<NULL>'),
                        coalesce(cast(rating_confidence as string), '<NULL>')
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
