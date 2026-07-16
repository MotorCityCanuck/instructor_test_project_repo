"""Cross-pipeline contract tests for Raw-to-Bronze and Bronze-to-Silver."""

from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config


RELEASE_NAMES = ("napa_5k", "napa_50k", "napa_250k")


def test_bronze_to_silver_consumes_all_raw_to_bronze_outputs() -> None:
    raw_config = load_raw_to_bronze_config("napa_5k")
    silver_config = load_bronze_to_silver_config("napa_5k")

    raw_sources = raw_config.data["sources"]
    silver_sources = silver_config.data["sources"]

    assert set(silver_sources) == set(raw_sources)
    for source_name, raw_source in raw_sources.items():
        silver_source = silver_sources[source_name]
        assert silver_source["source_file"] == raw_source["file_name"]
        assert silver_source["bronze_table"] == raw_source["bronze_table"]
        assert silver_source["natural_key"] == raw_source["key_columns"]


def test_bronze_to_silver_silver_tables_reference_configured_bronze_sources() -> None:
    silver_config = load_bronze_to_silver_config("napa_5k")
    configured_sources = set(silver_config.data["sources"])

    for table_name, table_config in silver_config.data["silver_tables"].items():
        assert table_config["source"] in configured_sources, table_name


def test_release_bronze_schemas_match_between_pipelines() -> None:
    for release_name in RELEASE_NAMES:
        raw_config = load_raw_to_bronze_config(release_name)
        silver_config = load_bronze_to_silver_config(release_name)

        assert silver_config.data["runtime"]["catalog"] == raw_config.data["runtime"]["catalog"]
        assert silver_config.data["schemas"]["bronze"] == raw_config.data["schemas"]["bronze"]
        assert silver_config.data["schemas"]["operations"] == raw_config.data["schemas"]["operations"]
