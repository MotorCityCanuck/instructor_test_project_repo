"""Tests for Silver-to-Gold stage-to-target publication helpers."""

import pytest

from napa_pipeline.silver_to_gold.publish import (
    PublicationError,
    publish_records_table,
    publish_stage_records_to_gold_table,
    publish_stage_to_gold_table,
)


class FakeTable:
    def __init__(self, row_count: int):
        self._row_count = row_count

    def count(self) -> int:
        return self._row_count

    def limit(self, _value: int):
        return self

    @property
    def write(self):
        return FakeWriteBuilder()


class FakeCatalog:
    def __init__(self, existing_tables=None):
        self._existing_tables = set(existing_tables or [])

    def tableExists(self, table_name: str) -> bool:
        return table_name in self._existing_tables


class FakeWriteBuilder:
    def format(self, _value: str):
        return self

    def mode(self, _value: str):
        return self

    def saveAsTable(self, _value: str):
        return None


class FakeDataFrame:
    def __init__(self):
        self.write = FakeWriteBuilder()


class FakeSparkSession:
    def __init__(self, counts_by_table=None, fail_on_sql=None, *, existing_tables=None):
        self.counts_by_table = counts_by_table or {}
        self.catalog = FakeCatalog(existing_tables)
        self.fail_on_sql = set(fail_on_sql or [])
        self.sql_queries: list[str] = []
        self.created_records = None
        self.created_schema = None

    def sql(self, query: str):
        self.sql_queries.append(query)
        if query in self.fail_on_sql:
            raise RuntimeError("simulated sql failure")
        return None

    def table(self, table_name: str):
        return FakeTable(self.counts_by_table.get(table_name, 0))

    def createDataFrame(self, records, schema=None):
        self.created_records = list(records)
        self.created_schema = schema
        return FakeDataFrame()


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


def test_publish_records_table_passes_explicit_schema_to_create_dataframe() -> None:
    spark = FakeSparkSession(
        counts_by_table={"workspace.instructor_5k_gold_stage.test_table": 1}
    )
    schema = object()

    row_count = publish_records_table(
        spark,
        "workspace.instructor_5k_gold_stage.test_table",
        [{"player_id": "player-1", "rating_change_from_prior": None}],
        schema=schema,
    )

    assert row_count == 1
    assert spark.created_records == [{"player_id": "player-1", "rating_change_from_prior": None}]
    assert spark.created_schema is schema


def test_publish_stage_records_to_gold_table_forwards_explicit_schema() -> None:
    spark = FakeSparkSession(
        counts_by_table={
            "workspace.instructor_5k_gold_stage.test_table": 1,
            "workspace.instructor_5k_gold.test_table": 1,
        }
    )
    schema = object()
    validations: list[str] = []

    stage_count, target_count = publish_stage_records_to_gold_table(
        spark,
        stage_table_fqn="workspace.instructor_5k_gold_stage.test_table",
        target_table_fqn="workspace.instructor_5k_gold.test_table",
        records=[{"player_id": "player-1", "rating_change_from_prior": None}],
        schema=schema,
        validation_fn=lambda _spark, table_fqn: validations.append(table_fqn),
    )

    assert stage_count == 1
    assert target_count == 1
    assert validations == ["workspace.instructor_5k_gold_stage.test_table"]
    assert spark.created_schema is schema
