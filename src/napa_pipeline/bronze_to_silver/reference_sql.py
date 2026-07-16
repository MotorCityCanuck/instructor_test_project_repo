"""Spark SQL plans for Bronze-to-Silver reference-table execution."""

from __future__ import annotations

from dataclasses import dataclass

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.operations import PipelineContext


@dataclass(frozen=True)
class SqlReferenceBuildPlan:
    """SQL statements and count queries for one reference-table build."""

    accepted_sql: str
    rejected_sql: str
    bronze_row_count_sql: str
    exact_duplicate_count_sql: str
    business_key_duplicate_count_sql: str
    warning_count_sql: str | None = None


def supports_reference_sql_transform(transform_name: str) -> bool:
    """Return whether a reference transform has a Spark SQL execution plan."""
    return transform_name in {"build_monthly_batches", "build_regions"}


def build_reference_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    target_table: str,
    source_table_fqn: str,
) -> SqlReferenceBuildPlan:
    """Return the SQL execution plan for one supported reference table."""
    if target_table == "monthly_batches":
        return _build_monthly_batches_sql_plan(context, source_table_fqn)
    if target_table == "regions":
        return _build_regions_sql_plan(config, context, source_table_fqn)
    raise ValueError(f"No SQL reference plan is defined for target table '{target_table}'.")


def _build_monthly_batches_sql_plan(
    context: PipelineContext,
    source_table_fqn: str,
) -> SqlReferenceBuildPlan:
    metadata_sql = _metadata_sql(
        context,
        source_table="monthly_batches",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(batch_id, '<NULL>'), "
            "coalesce(cast(batch_sequence as string), '<NULL>'), "
            "coalesce(cast(batch_date as string), '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH normalized_source AS (
    SELECT
        TRIM(CAST(id AS STRING)) AS batch_id,
        TRIM(CAST(COALESCE(batch_date, release_date, date) AS STRING)) AS batch_date_raw,
        TRIM(CAST(COALESCE(batch_sequence, sequence) AS STRING)) AS batch_sequence_raw,
        UPPER(NULLIF(TRIM(CAST(batch_type AS STRING)), '')) AS batch_type,
        UPPER(NULLIF(TRIM(CAST(COALESCE(batch_status, status) AS STRING)), '')) AS batch_status
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT
        batch_id,
        batch_date_raw,
        batch_sequence_raw,
        batch_type,
        batch_status
    FROM normalized_source
),
typed_source AS (
    SELECT
        batch_id,
        batch_date_raw,
        batch_sequence_raw,
        batch_type,
        batch_status,
        TO_DATE(batch_date_raw) AS batch_date,
        CAST(batch_sequence_raw AS INT) AS batch_sequence
    FROM deduped_source
),
invalid_rows AS (
    SELECT
        'monthly_batches' AS source_table,
        'monthly_batches' AS target_table,
        COALESCE(batch_id, '<NULL>') AS source_business_key,
        CASE
            WHEN batch_id IS NULL OR batch_id = '' THEN 'MISSING_PRIMARY_KEY'
            WHEN batch_date_raw IS NOT NULL AND batch_date_raw <> '' AND batch_date IS NULL THEN 'INVALID_DATE'
            WHEN batch_sequence_raw IS NOT NULL AND batch_sequence_raw <> ''
                 AND (batch_sequence IS NULL OR batch_sequence < 0) THEN 'VALUE_OUT_OF_RANGE'
        END AS reject_reason,
        CASE
            WHEN batch_id IS NULL OR batch_id = '' THEN 'BATCH_001'
            WHEN batch_date_raw IS NOT NULL AND batch_date_raw <> '' AND batch_date IS NULL THEN 'BATCH_002'
            WHEN batch_sequence_raw IS NOT NULL AND batch_sequence_raw <> ''
                 AND (batch_sequence IS NULL OR batch_sequence < 0) THEN 'BATCH_003'
        END AS rule_id,
        CASE
            WHEN batch_id IS NULL OR batch_id = '' THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN batch_id IS NULL OR batch_id = '' THEN 'batch_id could not be resolved from id.'
            WHEN batch_date_raw IS NOT NULL AND batch_date_raw <> '' AND batch_date IS NULL
                THEN concat('Invalid batch date ''', batch_date_raw, '''.')
            WHEN batch_sequence_raw IS NOT NULL AND batch_sequence_raw <> ''
                 AND batch_sequence IS NULL THEN concat('Invalid batch sequence ''', batch_sequence_raw, '''.')
            WHEN batch_sequence_raw IS NOT NULL AND batch_sequence_raw <> ''
                 AND batch_sequence < 0 THEN 'Batch sequence must be non-negative.'
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'batch_id', batch_id,
                'batch_date_raw', batch_date_raw,
                'batch_sequence_raw', batch_sequence_raw,
                'batch_type', batch_type,
                'batch_status', batch_status
            )
        ) AS source_record_json
    FROM typed_source
    WHERE batch_id IS NULL
       OR batch_id = ''
       OR (batch_date_raw IS NOT NULL AND batch_date_raw <> '' AND batch_date IS NULL)
       OR (batch_sequence_raw IS NOT NULL AND batch_sequence_raw <> ''
           AND (batch_sequence IS NULL OR batch_sequence < 0))
),
valid_rows AS (
    SELECT
        batch_id,
        batch_sequence,
        batch_date,
        batch_type,
        batch_status
    FROM typed_source
    WHERE NOT EXISTS (
        SELECT 1
        FROM invalid_rows invalid_rows
        WHERE invalid_rows.source_business_key = COALESCE(typed_source.batch_id, '<NULL>')
          AND invalid_rows.source_record_json = TO_JSON(
              NAMED_STRUCT(
                  'batch_id', typed_source.batch_id,
                  'batch_date_raw', typed_source.batch_date_raw,
                  'batch_sequence_raw', typed_source.batch_sequence_raw,
                  'batch_type', typed_source.batch_type,
                  'batch_status', typed_source.batch_status
              )
          )
    )
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY batch_id
            ORDER BY
                (
                    CASE WHEN batch_sequence IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN batch_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN batch_type IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN batch_status IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(batch_id, '<NULL>'),
                        coalesce(cast(batch_sequence as string), '<NULL>'),
                        coalesce(cast(batch_date as string), '<NULL>'),
                        coalesce(batch_type, '<NULL>'),
                        coalesce(batch_status, '<NULL>')
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
    batch_id,
    sha2(coalesce(batch_id, '<NULL>'), 256) AS batch_sk,
    batch_sequence,
    batch_date,
    YEAR(batch_date) AS batch_year,
    MONTH(batch_date) AS batch_month,
    CAST((((MONTH(batch_date) - 1) DIV 3) + 1) AS INT) AS batch_quarter,
    batch_type,
    batch_status,
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
    'monthly_batches' AS source_table,
    'monthly_batches' AS target_table,
    batch_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'BATCH_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'batch_id', batch_id,
            'batch_sequence', batch_sequence,
            'batch_date', batch_date,
            'batch_type', batch_type,
            'batch_status', batch_status
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'batch_id', batch_id,
                'batch_sequence', batch_sequence,
                'batch_date', batch_date,
                'batch_type', batch_type,
                'batch_status', batch_status
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
        TRIM(CAST(id AS STRING)) AS batch_id,
        TRIM(CAST(COALESCE(batch_date, release_date, date) AS STRING)) AS batch_date_raw,
        TRIM(CAST(COALESCE(batch_sequence, sequence) AS STRING)) AS batch_sequence_raw,
        UPPER(NULLIF(TRIM(CAST(batch_type AS STRING)), '')) AS batch_type,
        UPPER(NULLIF(TRIM(CAST(COALESCE(batch_status, status) AS STRING)), '')) AS batch_status
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT
        batch_id,
        batch_date_raw,
        batch_sequence_raw,
        batch_type,
        batch_status
    FROM normalized_source
)
SELECT
    (SELECT COUNT(*) FROM normalized_source) - (SELECT COUNT(*) FROM deduped_source) AS value
""".strip(),
        business_key_duplicate_count_sql=f"""
WITH valid_rows AS (
    SELECT
        TRIM(CAST(id AS STRING)) AS batch_id,
        CAST(TRIM(CAST(COALESCE(batch_sequence, sequence) AS STRING)) AS INT) AS batch_sequence,
        TO_DATE(TRIM(CAST(COALESCE(batch_date, release_date, date) AS STRING))) AS batch_date,
        UPPER(NULLIF(TRIM(CAST(batch_type AS STRING)), '')) AS batch_type,
        UPPER(NULLIF(TRIM(CAST(COALESCE(batch_status, status) AS STRING)), '')) AS batch_status
    FROM {source_table_fqn}
    WHERE TRIM(CAST(id AS STRING)) IS NOT NULL
      AND TRIM(CAST(id AS STRING)) <> ''
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY batch_id
            ORDER BY
                (
                    CASE WHEN batch_sequence IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN batch_date IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN batch_type IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN batch_status IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(batch_id, '<NULL>'),
                        coalesce(cast(batch_sequence as string), '<NULL>'),
                        coalesce(cast(batch_date as string), '<NULL>'),
                        coalesce(batch_type, '<NULL>'),
                        coalesce(batch_status, '<NULL>')
                    ),
                    256
                ) ASC
        ) AS duplicate_rank
    FROM (
        SELECT DISTINCT * FROM valid_rows
    )
)
SELECT COUNT(*) AS value
FROM ranked_rows
WHERE duplicate_rank > 1
""".strip(),
    )


def _build_regions_sql_plan(
    config: BronzeToSilverConfig,
    context: PipelineContext,
    source_table_fqn: str,
) -> SqlReferenceBuildPlan:
    country_expr = _domain_case_expression(
        "country_input",
        config.data["domains"]["country_code"],
    )
    metadata_sql = _metadata_sql(
        context,
        source_table="regions",
        record_hash_expr=(
            "sha2(concat_ws('|', coalesce(region_id, '<NULL>'), "
            "coalesce(region_name, '<NULL>'), coalesce(country_code, '<NULL>')), 256)"
        ),
    )
    base_ctes = f"""
WITH normalized_source AS (
    SELECT
        TRIM(CAST(id AS STRING)) AS region_id,
        NULLIF(TRIM(CAST(COALESCE(region_name, name) AS STRING)), '') AS region_name,
        UPPER(NULLIF(TRIM(CAST(COALESCE(country_code, country, country_name) AS STRING)), '')) AS country_input,
        UPPER(NULLIF(TRIM(CAST(COALESCE(province_state, province, state) AS STRING)), '')) AS province_state,
        UPPER(NULLIF(TRIM(CAST(COALESCE(active_flag, status) AS STRING)), '')) AS status_input
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT
        region_id,
        region_name,
        country_input,
        province_state,
        status_input
    FROM normalized_source
),
typed_source AS (
    SELECT
        region_id,
        region_name,
        country_input,
        province_state,
        CASE
            WHEN status_input IN ('TRUE', 'T', 'YES', 'Y', '1', 'ACTIVE') THEN true
            WHEN status_input IN ('FALSE', 'F', 'NO', 'N', '0', 'INACTIVE') THEN false
            ELSE NULL
        END AS active_flag,
        {country_expr} AS country_code
    FROM deduped_source
),
invalid_rows AS (
    SELECT
        'regions' AS source_table,
        'regions' AS target_table,
        COALESCE(region_id, '<NULL>') AS source_business_key,
        CASE
            WHEN region_id IS NULL OR region_id = '' THEN 'MISSING_PRIMARY_KEY'
            WHEN region_name IS NULL THEN 'MISSING_REQUIRED_COLUMN'
            WHEN country_code IS NULL THEN 'INVALID_DOMAIN_VALUE'
        END AS reject_reason,
        CASE
            WHEN region_id IS NULL OR region_id = '' THEN 'REGION_001'
            WHEN region_name IS NULL THEN 'REGION_002'
            WHEN country_code IS NULL THEN 'REGION_003'
        END AS rule_id,
        CASE
            WHEN region_id IS NULL OR region_id = '' THEN 'CRITICAL'
            WHEN region_name IS NULL THEN 'CRITICAL'
            ELSE 'ERROR'
        END AS rule_severity,
        CASE
            WHEN region_id IS NULL OR region_id = '' THEN 'region_id could not be resolved from id.'
            WHEN region_name IS NULL THEN 'region_name could not be resolved.'
            WHEN country_code IS NULL THEN concat('Invalid country value ''', country_input, '''.')
        END AS reject_reason_detail,
        {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
        {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
        {sql_literal(context.release_name)} AS _source_dataset,
        current_timestamp() AS load_ts,
        current_timestamp() AS _load_ts,
        TO_JSON(
            NAMED_STRUCT(
                'region_id', region_id,
                'region_name', region_name,
                'country_input', country_input,
                'province_state', province_state,
                'status_input', status_input
            )
        ) AS source_record_json
    FROM (
        SELECT
            region_id,
            region_name,
            country_input,
            province_state,
            status_input,
            country_code
        FROM (
            SELECT
                region_id,
                region_name,
                country_input,
                province_state,
                CASE
                    WHEN status_input IN ('TRUE', 'T', 'YES', 'Y', '1', 'ACTIVE') THEN true
                    WHEN status_input IN ('FALSE', 'F', 'NO', 'N', '0', 'INACTIVE') THEN false
                    ELSE NULL
                END AS active_flag,
                status_input,
                {country_expr} AS country_code
            FROM deduped_source
        )
    )
    WHERE region_id IS NULL
       OR region_id = ''
       OR region_name IS NULL
       OR country_code IS NULL
),
valid_rows AS (
    SELECT
        region_id,
        region_name,
        province_state,
        country_code,
        active_flag
    FROM typed_source
    WHERE region_id IS NOT NULL
      AND region_id <> ''
      AND region_name IS NOT NULL
      AND country_code IS NOT NULL
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY region_id
            ORDER BY
                (
                    CASE WHEN region_name IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN province_state IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN active_flag IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(region_id, '<NULL>'),
                        coalesce(region_name, '<NULL>'),
                        coalesce(country_code, '<NULL>'),
                        coalesce(province_state, '<NULL>'),
                        coalesce(cast(active_flag as string), '<NULL>')
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
    region_id,
    sha2(coalesce(region_id, '<NULL>'), 256) AS region_sk,
    region_name,
    province_state,
    country_code,
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
    'regions' AS source_table,
    'regions' AS target_table,
    region_id AS source_business_key,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason,
    'DUPLICATE_BUSINESS_KEY' AS reject_reason_code,
    'Duplicate business key lost deterministic tie-break.' AS reject_reason_detail,
    'REGION_DUPLICATE' AS rule_id,
    'ERROR' AS rule_severity,
    {sql_literal(context.pipeline_run_id)} AS pipeline_run_id,
    {sql_literal(context.pipeline_run_id)} AS _pipeline_run_id,
    {sql_literal(context.release_name)} AS _source_dataset,
    current_timestamp() AS load_ts,
    current_timestamp() AS _load_ts,
    TO_JSON(
        NAMED_STRUCT(
            'region_id', region_id,
            'region_name', region_name,
            'province_state', province_state,
            'country_code', country_code,
            'active_flag', active_flag
        )
    ) AS source_record_json,
    sha2(
        TO_JSON(
            NAMED_STRUCT(
                'region_id', region_id,
                'region_name', region_name,
                'province_state', province_state,
                'country_code', country_code,
                'active_flag', active_flag
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
        TRIM(CAST(id AS STRING)) AS region_id,
        NULLIF(TRIM(CAST(COALESCE(region_name, name) AS STRING)), '') AS region_name,
        UPPER(NULLIF(TRIM(CAST(COALESCE(country_code, country, country_name) AS STRING)), '')) AS country_input,
        UPPER(NULLIF(TRIM(CAST(COALESCE(province_state, province, state) AS STRING)), '')) AS province_state,
        UPPER(NULLIF(TRIM(CAST(COALESCE(active_flag, status) AS STRING)), '')) AS status_input
    FROM {source_table_fqn}
),
deduped_source AS (
    SELECT DISTINCT
        region_id,
        region_name,
        country_input,
        province_state,
        status_input
    FROM normalized_source
)
SELECT
    (SELECT COUNT(*) FROM normalized_source) - (SELECT COUNT(*) FROM deduped_source) AS value
""".strip(),
        business_key_duplicate_count_sql=f"""
WITH normalized_source AS (
    SELECT
        TRIM(CAST(id AS STRING)) AS region_id,
        NULLIF(TRIM(CAST(COALESCE(region_name, name) AS STRING)), '') AS region_name,
        UPPER(NULLIF(TRIM(CAST(COALESCE(country_code, country, country_name) AS STRING)), '')) AS country_input,
        UPPER(NULLIF(TRIM(CAST(COALESCE(province_state, province, state) AS STRING)), '')) AS province_state,
        UPPER(NULLIF(TRIM(CAST(COALESCE(active_flag, status) AS STRING)), '')) AS status_input
    FROM {source_table_fqn}
),
valid_rows AS (
    SELECT DISTINCT
        region_id,
        region_name,
        province_state,
        CASE
            WHEN status_input IN ('TRUE', 'T', 'YES', 'Y', '1', 'ACTIVE') THEN true
            WHEN status_input IN ('FALSE', 'F', 'NO', 'N', '0', 'INACTIVE') THEN false
            ELSE NULL
        END AS active_flag,
        {country_expr} AS country_code
    FROM normalized_source
    WHERE region_id IS NOT NULL
      AND region_id <> ''
      AND region_name IS NOT NULL
      AND {country_expr} IS NOT NULL
),
ranked_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY region_id
            ORDER BY
                (
                    CASE WHEN region_name IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN province_state IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END
                  + CASE WHEN active_flag IS NOT NULL THEN 1 ELSE 0 END
                ) DESC,
                sha2(
                    concat_ws('|',
                        coalesce(region_id, '<NULL>'),
                        coalesce(region_name, '<NULL>'),
                        coalesce(country_code, '<NULL>'),
                        coalesce(province_state, '<NULL>'),
                        coalesce(cast(active_flag as string), '<NULL>')
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


def sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
