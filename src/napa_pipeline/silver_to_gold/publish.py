"""Publication helpers for Silver-to-Gold staged table promotion."""

from __future__ import annotations

from typing import Any, Callable


class PublicationError(RuntimeError):
    """Raised when Gold publication cannot complete."""


def publish_sql_table(
    spark: Any,
    table_fqn: str,
    select_sql: str,
) -> int:
    """Publish a table directly from SQL and return its row count when available."""
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {table_fqn} USING DELTA AS {select_sql}")
        try:
            return int(spark.table(table_fqn).count())
        except Exception:
            return 0
    except Exception as exc:
        raise PublicationError(f"Could not publish table {table_fqn}.") from exc


def publish_stage_to_gold_table(
    spark: Any,
    *,
    stage_table_fqn: str,
    target_table_fqn: str,
    stage_sql: str,
    validation_fn: Callable[[Any, str], None] | None = None,
    count_fn: Callable[[Any, str], int] | None = None,
) -> tuple[int, int]:
    """Build a Gold target in stage, validate it, then overwrite the final target."""
    source_count_fn = count_fn or _count_rows

    try:
        stage_row_count = publish_sql_table(spark, stage_table_fqn, stage_sql)
        if validation_fn is not None:
            validation_fn(spark, stage_table_fqn)

        target_row_count = publish_sql_table(
            spark,
            target_table_fqn,
            f"SELECT * FROM {stage_table_fqn}",
        )
        verified_row_count = source_count_fn(spark, target_table_fqn)
    except Exception as exc:
        if isinstance(exc, PublicationError):
            raise
        raise PublicationError(
            f"Could not stage and publish Gold table {target_table_fqn} from {stage_table_fqn}."
        ) from exc

    if target_row_count != verified_row_count:
        raise PublicationError(
            f"Published Gold table {target_table_fqn} did not verify: "
            f"published_row_count={target_row_count}, verified_row_count={verified_row_count}."
        )

    return stage_row_count, verified_row_count


def _count_rows(spark: Any, table_fqn: str) -> int:
    return int(spark.table(table_fqn).count())
