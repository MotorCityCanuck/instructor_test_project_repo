"""Bronze publication helpers for the Raw-to-Bronze pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from napa_pipeline.raw_to_bronze.config import RawToBronzeConfig
from napa_pipeline.raw_to_bronze.environment import ReleaseEnvironment
from napa_pipeline.raw_to_bronze.inventory import SourceReadinessRecord
from napa_pipeline.raw_to_bronze.operations import PipelineContext, calculate_schema_hash

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


BRONZE_METADATA_COLUMNS = (
    "_pipeline_run_id",
    "_pipeline_name",
    "_pipeline_version",
    "_release_name",
    "_source_file_name",
    "_source_file_path",
    "_source_file_size",
    "_source_file_modification_ts",
    "_ingested_ts",
    "_source_record_hash",
)


class BronzePublicationError(RuntimeError):
    """Raised when Bronze table publication fails."""


@dataclass(frozen=True)
class BronzeTableBuildResult:
    """Result metadata for one Bronze table build."""

    source_name: str
    source_file_name: str
    target_table_fqn: str
    source_row_count: int
    bronze_row_count: int
    source_schema_hash: str
    bronze_schema_hash: str
    source_file_size: int | None
    bronze_schema_fields: tuple[dict[str, Any], ...]


def get_bronze_target_table_fqn(
    environment: ReleaseEnvironment,
    source_config: dict[str, Any],
) -> str:
    """Return the fully qualified Bronze target table name for one source."""
    return (
        f"{environment.catalog}.{environment.bronze_schema}."
        f"{source_config['bronze_table']}"
    )


def get_bronze_table_comment(
    release_name: str,
    source_name: str,
) -> str:
    """Return the standard Bronze table comment."""
    return (
        "Raw-preserving Delta representation of the NAPA "
        f"{source_name} Parquet file for release {release_name}. "
        "Business values are unchanged; operational ingestion metadata is appended."
    )


def get_bronze_table_properties(context: PipelineContext) -> dict[str, str]:
    """Return standard Bronze table properties."""
    return {
        "napa.layer": "bronze",
        "napa.release": context.release_name,
        "napa.pipeline": context.pipeline_name,
        "napa.processing_mode": context.processing_mode,
    }


def read_raw_source(spark: Any, source_readiness: SourceReadinessRecord) -> DataFrame:
    """Read one source Parquet file from the Raw volume."""
    return spark.read.parquet(source_readiness.file_path)


def build_bronze_dataframe(
    source_df: DataFrame,
    context: PipelineContext,
    source_readiness: SourceReadinessRecord,
    ingested_ts: datetime | None = None,
) -> DataFrame:
    """Add operational metadata columns to a Raw source DataFrame."""
    from pyspark.sql import functions as F

    _validate_metadata_collisions(source_df.columns)

    bronze_df = source_df
    bronze_df = bronze_df.withColumn(
        "_pipeline_run_id", F.lit(context.pipeline_run_id)
    ).withColumn(
        "_pipeline_name", F.lit(context.pipeline_name)
    ).withColumn(
        "_pipeline_version", F.lit(context.pipeline_version)
    ).withColumn(
        "_release_name", F.lit(context.release_name)
    ).withColumn(
        "_source_file_name", F.lit(source_readiness.file_name)
    ).withColumn(
        "_source_file_path", F.lit(source_readiness.file_path)
    ).withColumn(
        "_source_file_size", F.lit(source_readiness.file_size)
    ).withColumn(
        "_source_file_modification_ts", F.lit(source_readiness.modification_ts)
    ).withColumn(
        "_ingested_ts", F.lit(ingested_ts or datetime.utcnow())
    ).withColumn(
        "_source_record_hash",
        _build_source_record_hash_expression(source_df.columns),
    )
    return bronze_df


def publish_bronze_table(
    bronze_df: DataFrame,
    config: RawToBronzeConfig,
    target_table_fqn: str,
) -> None:
    """Publish one Bronze table using the configured full-refresh mode."""
    bronze_df.write.format(str(config.data["publication"]["format"])).mode(
        str(config.data["publication"]["mode"])
    ).option(
        "overwriteSchema",
        str(config.data["publication"]["overwrite_schema"]).lower(),
    ).saveAsTable(target_table_fqn)


def finalize_bronze_table_metadata(
    spark: Any,
    context: PipelineContext,
    source_name: str,
    target_table_fqn: str,
) -> None:
    """Apply standard table comment and properties to a Bronze table."""
    comment = get_bronze_table_comment(context.release_name, source_name).replace(
        "'", "''"
    )
    spark.sql(f"COMMENT ON TABLE {target_table_fqn} IS '{comment}'")

    properties = ", ".join(
        f"'{key}' = '{value}'"
        for key, value in get_bronze_table_properties(context).items()
    )
    spark.sql(f"ALTER TABLE {target_table_fqn} SET TBLPROPERTIES ({properties})")


def build_bronze_table(
    spark: Any,
    config: RawToBronzeConfig,
    context: PipelineContext,
    environment: ReleaseEnvironment,
    source_config: dict[str, Any],
    source_readiness: SourceReadinessRecord,
    ingested_ts: datetime | None = None,
) -> BronzeTableBuildResult:
    """Read, enrich, publish, and describe one Bronze table."""
    target_table_fqn = get_bronze_target_table_fqn(environment, source_config)

    try:
        source_df = read_raw_source(spark, source_readiness)
        bronze_df = build_bronze_dataframe(
            source_df,
            context,
            source_readiness,
            ingested_ts=ingested_ts,
        )
        bronze_row_count = bronze_df.count()
        bronze_schema_fields = tuple(_spark_schema_to_fields(bronze_df.schema))
        bronze_schema_hash = calculate_schema_hash(list(bronze_schema_fields))

        publish_bronze_table(bronze_df, config, target_table_fqn)
        finalize_bronze_table_metadata(
            spark,
            context,
            source_config["source_name"],
            target_table_fqn,
        )
    except Exception as exc:
        raise BronzePublicationError(
            "Failed to build Bronze table for source "
            f"{source_config['source_name']} ({source_readiness.file_name})."
        ) from exc

    return BronzeTableBuildResult(
        source_name=str(source_config["source_name"]),
        source_file_name=source_readiness.file_name,
        target_table_fqn=target_table_fqn,
        source_row_count=source_readiness.row_count,
        bronze_row_count=bronze_row_count,
        source_schema_hash=source_readiness.schema_hash,
        bronze_schema_hash=bronze_schema_hash,
        source_file_size=source_readiness.file_size,
        bronze_schema_fields=bronze_schema_fields,
    )


def _validate_metadata_collisions(source_columns: list[str]) -> None:
    collisions = sorted(set(source_columns) & set(BRONZE_METADATA_COLUMNS))
    if collisions:
        raise BronzePublicationError(
            "Raw source columns collide with Bronze metadata columns: "
            f"{', '.join(collisions)}."
        )


def _build_source_record_hash_expression(source_columns: list[str]) -> Any:
    from pyspark.sql import functions as F

    normalized_columns = [
        F.coalesce(F.col(column_name).cast("string"), F.lit("__NULL__"))
        for column_name in source_columns
    ]
    return F.sha2(F.concat_ws("||", *normalized_columns), 256)


def _spark_schema_to_fields(schema: Any) -> list[dict[str, Any]]:
    return [
        {
            "column_name": field.name,
            "data_type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in schema.fields
    ]
