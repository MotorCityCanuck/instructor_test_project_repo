"""Bronze reconciliation helpers for the Raw-to-Bronze pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from napa_pipeline.raw_to_bronze.bronze import (
    BRONZE_METADATA_COLUMNS,
    get_bronze_target_table_fqn,
)
from napa_pipeline.raw_to_bronze.environment import ReleaseEnvironment
from napa_pipeline.raw_to_bronze.inventory import SourceReadinessRecord


@dataclass(frozen=True)
class BronzeReconciliationResult:
    """Reconciliation outcome for one Bronze table."""

    source_name: str
    source_file_name: str
    bronze_table: str
    target_table_fqn: str
    raw_row_count: int
    bronze_row_count: int
    row_count_difference: int
    raw_business_column_count: int
    bronze_business_column_count: int
    metadata_column_count: int
    missing_metadata_columns: tuple[str, ...]
    missing_business_columns: tuple[str, ...]
    unexpected_business_columns: tuple[str, ...]
    status: str


class ReconciliationError(RuntimeError):
    """Raised when Bronze reconciliation cannot be completed."""


def reconcile_bronze_table(
    spark: Any,
    environment: ReleaseEnvironment,
    source_config: dict[str, Any],
    source_readiness: SourceReadinessRecord,
) -> BronzeReconciliationResult:
    """Reconcile one Bronze table against its validated Raw source contract."""
    target_table_fqn = get_bronze_target_table_fqn(environment, source_config)

    try:
        bronze_df = spark.table(target_table_fqn)
    except Exception as exc:
        raise ReconciliationError(
            f"Could not read Bronze table {target_table_fqn} for reconciliation."
        ) from exc

    bronze_columns = list(bronze_df.columns)
    bronze_business_columns = [
        column_name
        for column_name in bronze_columns
        if column_name not in BRONZE_METADATA_COLUMNS
    ]
    metadata_columns = [
        column_name
        for column_name in bronze_columns
        if column_name in BRONZE_METADATA_COLUMNS
    ]
    raw_business_columns = [
        field["column_name"] for field in source_readiness.schema_fields
    ]

    raw_row_count = source_readiness.row_count
    bronze_row_count = bronze_df.count()
    row_count_difference = bronze_row_count - raw_row_count

    missing_metadata_columns = tuple(
        column_name
        for column_name in BRONZE_METADATA_COLUMNS
        if column_name not in metadata_columns
    )
    missing_business_columns = tuple(
        column_name
        for column_name in raw_business_columns
        if column_name not in bronze_business_columns
    )
    unexpected_business_columns = tuple(
        column_name
        for column_name in bronze_business_columns
        if column_name not in raw_business_columns
    )

    status = "MATCHED"
    if (
        row_count_difference != 0
        or missing_metadata_columns
        or missing_business_columns
        or unexpected_business_columns
        or bronze_business_columns != raw_business_columns
    ):
        status = "MISMATCH"

    return BronzeReconciliationResult(
        source_name=str(source_config["source_name"]),
        source_file_name=source_readiness.file_name,
        bronze_table=str(source_config["bronze_table"]),
        target_table_fqn=target_table_fqn,
        raw_row_count=raw_row_count,
        bronze_row_count=bronze_row_count,
        row_count_difference=row_count_difference,
        raw_business_column_count=len(raw_business_columns),
        bronze_business_column_count=len(bronze_business_columns),
        metadata_column_count=len(metadata_columns),
        missing_metadata_columns=missing_metadata_columns,
        missing_business_columns=missing_business_columns,
        unexpected_business_columns=unexpected_business_columns,
        status=status,
    )
