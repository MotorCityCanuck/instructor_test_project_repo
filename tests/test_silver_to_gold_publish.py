"""Tests for Silver-to-Gold stage-to-target publication helpers."""

import pytest

from napa_pipeline.silver_to_gold.publish import PublicationError, publish_stage_to_gold_table


class FakeTable:
    def __init__(self, row_count: int):
        self._row_count = row_count

    def count(self) -> int:
        return self._row_count


class FakeSparkSession:
    def __init__(self, counts_by_table=None, fail_on_sql=None):
        self.counts_by_table = counts_by_table or {}
        self.fail_on_sql = set(fail_on_sql or [])
        self.sql_queries: list[str] = []

    def sql(self, query: str):
        self.sql_queries.append(query)
        if query in self.fail_on_sql:
            raise RuntimeError("simulated sql failure")
        return None

    def table(self, table_name: str):
        return FakeTable(self.counts_by_table.get(table_name, 0))


def test_publish_stage_to_gold_table_builds_stage_then_target() -> None:
    spark = FakeSparkSession(
        counts_by_table={
            "workspace.instructor_5k_gold_stage.test_table": 3,
            "workspace.instructor_5k_gold.test_table": 3,
        }
    )
    validations: list[str] = []

    stage_count, target_count = publish_stage_to_gold_table(
        spark,
        stage_table_fqn="workspace.instructor_5k_gold_stage.test_table",
        target_table_fqn="workspace.instructor_5k_gold.test_table",
        stage_sql="SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3",
        validation_fn=lambda _spark, table_fqn: validations.append(table_fqn),
    )

    assert stage_count == 3
    assert target_count == 3
    assert validations == ["workspace.instructor_5k_gold_stage.test_table"]
    assert spark.sql_queries[0].startswith(
        "CREATE OR REPLACE TABLE workspace.instructor_5k_gold_stage.test_table"
    )
    assert spark.sql_queries[1].startswith(
        "CREATE OR REPLACE TABLE workspace.instructor_5k_gold.test_table"
    )


def test_publish_stage_to_gold_table_raises_on_validation_failure() -> None:
    spark = FakeSparkSession(
        counts_by_table={"workspace.instructor_5k_gold_stage.test_table": 1}
    )

    with pytest.raises(PublicationError, match="Could not stage and publish Gold table"):
        publish_stage_to_gold_table(
            spark,
            stage_table_fqn="workspace.instructor_5k_gold_stage.test_table",
            target_table_fqn="workspace.instructor_5k_gold.test_table",
            stage_sql="SELECT 1 AS id",
            validation_fn=lambda _spark, _table_fqn: (_ for _ in ()).throw(
                RuntimeError("validation failed")
            ),
        )


def test_publish_stage_to_gold_table_raises_on_row_count_verification_failure() -> None:
    spark = FakeSparkSession(
        counts_by_table={
            "workspace.instructor_5k_gold_stage.test_table": 2,
            "workspace.instructor_5k_gold.test_table": 1,
        }
    )

    with pytest.raises(PublicationError, match="did not verify"):
        publish_stage_to_gold_table(
            spark,
            stage_table_fqn="workspace.instructor_5k_gold_stage.test_table",
            target_table_fqn="workspace.instructor_5k_gold.test_table",
            stage_sql="SELECT 1 AS id UNION ALL SELECT 2",
            count_fn=lambda _spark, _table_fqn: 0,
        )
