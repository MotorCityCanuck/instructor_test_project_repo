"""Structural and relationship certification rules for Raw releases."""

from __future__ import annotations

from typing import Any

from napa_pipeline.certification.config import CertificationConfig
from napa_pipeline.certification.models import (
    CertificationRuleResult,
    InventoryCertificationResult,
    SourceLoadResult,
)


def evaluate_structural_rules(
    spark: Any,
    config: CertificationConfig,
    inventory_result: InventoryCertificationResult,
) -> tuple[CertificationRuleResult, ...]:
    """Evaluate schema, key, and relationship rules for a loaded Raw release."""
    loaded_by_source = {
        source.source_name: source for source in inventory_result.loaded_sources
    }
    results: list[CertificationRuleResult] = []
    results.extend(_evaluate_schema_rules(config, loaded_by_source))
    results.extend(_evaluate_primary_key_rules(spark, config, loaded_by_source))
    results.extend(_evaluate_foreign_key_rules(spark, config, loaded_by_source))
    results.extend(_evaluate_match_shape_rules(spark, loaded_by_source))
    results.extend(_evaluate_team_identity_rule(spark, loaded_by_source))
    return tuple(results)


def _evaluate_schema_rules(
    config: CertificationConfig,
    loaded_by_source: dict[str, SourceLoadResult],
) -> list[CertificationRuleResult]:
    results: list[CertificationRuleResult] = []
    schema_contract = config.data["schema_contract"]

    for source_name, contract in schema_contract.items():
        loaded_source = loaded_by_source.get(source_name)
        if loaded_source is None:
            continue
        if loaded_source.read_status == "UNREADABLE":
            continue

        schema_by_column = {
            field["column_name"]: field["data_type"]
            for field in loaded_source.schema_fields
        }
        missing_columns = tuple(
            column_name
            for column_name in contract["required_columns"].keys()
            if column_name not in schema_by_column
        )
        results.append(
            CertificationRuleResult(
                rule_id=f"RAW_SCHEMA_REQUIRED_COLUMNS_{source_name.upper()}",
                name=f"{source_name} required columns are present",
                pillar="Schema and Structural Integrity",
                category="schema",
                status="PASS" if not missing_columns else "FAIL",
                severity="info" if not missing_columns else "blocker",
                message=(
                    f"All required columns are present for {source_name}."
                    if not missing_columns
                    else f"Missing required columns for {source_name}: {', '.join(missing_columns)}."
                ),
                affected_count=len(missing_columns),
                sample_records=missing_columns[:5],
            )
        )

        incompatible_columns = tuple(
            f"{column_name}:{schema_by_column[column_name]}"
            for column_name, allowed_types in contract["required_columns"].items()
            if column_name in schema_by_column and schema_by_column[column_name] not in allowed_types
        )
        results.append(
            CertificationRuleResult(
                rule_id=f"RAW_SCHEMA_TYPE_COMPATIBILITY_{source_name.upper()}",
                name=f"{source_name} required column types are compatible",
                pillar="Schema and Structural Integrity",
                category="schema",
                status="PASS" if not incompatible_columns else "FAIL",
                severity="info" if not incompatible_columns else "error",
                message=(
                    f"Required column types are compatible for {source_name}."
                    if not incompatible_columns
                    else f"Incompatible required column types for {source_name}: {', '.join(incompatible_columns)}."
                ),
                affected_count=len(incompatible_columns),
                sample_records=incompatible_columns[:5],
            )
        )

    return results


def _evaluate_primary_key_rules(
    spark: Any,
    config: CertificationConfig,
    loaded_by_source: dict[str, SourceLoadResult],
) -> list[CertificationRuleResult]:
    results: list[CertificationRuleResult] = []

    for source in config.sources_in_build_order:
        source_name = source["source_name"]
        loaded_source = loaded_by_source.get(source_name)
        if loaded_source is None or loaded_source.read_status != "READY" and loaded_source.read_status != "EMPTY":
            continue
        key_columns = tuple(source["key_columns"])
        duplicate_count = _run_count_query(
            spark,
            f"PK_DUPLICATES:{source_name}",
            _build_duplicate_key_query(loaded_source.temp_view_name, key_columns),
        )
        results.append(
            CertificationRuleResult(
                rule_id=f"RAW_PRIMARY_KEY_UNIQUENESS_{source_name.upper()}",
                name=f"{source_name} primary keys are unique",
                pillar="Schema and Structural Integrity",
                category="keys",
                status="PASS" if duplicate_count == 0 else "FAIL",
                severity="info" if duplicate_count == 0 else "blocker",
                message=(
                    f"Primary key uniqueness passed for {source_name}."
                    if duplicate_count == 0
                    else f"{duplicate_count} duplicate primary-key groups were found for {source_name}."
                ),
                affected_count=duplicate_count,
                expected_value=0,
            )
        )

    return results


def _evaluate_foreign_key_rules(
    spark: Any,
    config: CertificationConfig,
    loaded_by_source: dict[str, SourceLoadResult],
) -> list[CertificationRuleResult]:
    results: list[CertificationRuleResult] = []

    for relation in config.data["relationships"]:
        child = loaded_by_source.get(relation["child_source"])
        parent = loaded_by_source.get(relation["parent_source"])
        if child is None or parent is None:
            continue
        if child.read_status == "UNREADABLE" or parent.read_status == "UNREADABLE":
            continue

        orphan_count = _run_count_query(
            spark,
            f"FK_ORPHANS:{relation['rule_id']}",
            _build_foreign_key_query(
                child.temp_view_name,
                tuple(relation["child_columns"]),
                parent.temp_view_name,
                tuple(relation["parent_columns"]),
            ),
        )
        results.append(
            CertificationRuleResult(
                rule_id=str(relation["rule_id"]),
                name=(
                    f"{relation['child_source']} references "
                    f"{relation['parent_source']} successfully"
                ),
                pillar="Schema and Structural Integrity",
                category="relationships",
                status="PASS" if orphan_count == 0 else "FAIL",
                severity="info" if orphan_count == 0 else str(relation["severity"]),
                message=(
                    f"Foreign-key validation passed for {relation['child_source']} -> {relation['parent_source']}."
                    if orphan_count == 0
                    else (
                        f"{orphan_count} orphan rows were found for "
                        f"{relation['child_source']} -> {relation['parent_source']}."
                    )
                ),
                affected_count=orphan_count,
                expected_value=0,
            )
        )

    return results


def _evaluate_match_shape_rules(
    spark: Any,
    loaded_by_source: dict[str, SourceLoadResult],
) -> list[CertificationRuleResult]:
    results: list[CertificationRuleResult] = []

    if "match_teams" in loaded_by_source:
        count = _run_count_query(
            spark,
            "MATCH_SIDE_CARDINALITY",
            """
SELECT COUNT(*) AS value
FROM (
    SELECT match_id
    FROM raw_cert_match_teams
    GROUP BY match_id
    HAVING COUNT(*) <> 2
) AS invalid_matches
""".strip(),
        )
        results.append(
            CertificationRuleResult(
                rule_id="RAW_MATCH_SIDE_CARDINALITY",
                name="Each match has exactly two match-side rows",
                pillar="Schema and Structural Integrity",
                category="matches",
                status="PASS" if count == 0 else "FAIL",
                severity="info" if count == 0 else "blocker",
                message=(
                    "All matches have exactly two match-side rows."
                    if count == 0
                    else f"{count} matches do not have exactly two match-side rows."
                ),
                affected_count=count,
                expected_value=0,
            )
        )

    if "match_team_players" in loaded_by_source:
        count = _run_count_query(
            spark,
            "MATCH_PARTICIPANT_CARDINALITY",
            """
SELECT COUNT(*) AS value
FROM (
    SELECT match_team_id
    FROM raw_cert_match_team_players
    GROUP BY match_team_id
    HAVING COUNT(DISTINCT player_id) <> 2
) AS invalid_match_teams
""".strip(),
        )
        results.append(
            CertificationRuleResult(
                rule_id="RAW_MATCH_PARTICIPANT_CARDINALITY",
                name="Each match team has exactly two participating players",
                pillar="Schema and Structural Integrity",
                category="matches",
                status="PASS" if count == 0 else "FAIL",
                severity="info" if count == 0 else "blocker",
                message=(
                    "All match teams have exactly two participating players."
                    if count == 0
                    else f"{count} match teams do not have exactly two participating players."
                ),
                affected_count=count,
                expected_value=0,
            )
        )

    if {"matches", "match_teams"} <= loaded_by_source.keys():
        count = _run_count_query(
            spark,
            "MATCH_WINNER_INTEGRITY",
            """
SELECT COUNT(*) AS value
FROM raw_cert_matches AS m
LEFT JOIN raw_cert_match_teams AS mt
    ON m.id = mt.match_id
   AND m.winning_team_id = mt.team_id
WHERE m.winning_team_id IS NOT NULL
  AND mt.id IS NULL
""".strip(),
        )
        results.append(
            CertificationRuleResult(
                rule_id="RAW_MATCH_WINNER_INTEGRITY",
                name="Recorded winning team appears among match participants",
                pillar="Schema and Structural Integrity",
                category="matches",
                status="PASS" if count == 0 else "FAIL",
                severity="info" if count == 0 else "blocker",
                message=(
                    "All recorded winning teams participate in their matches."
                    if count == 0
                    else f"{count} matches reference a winning team that does not participate."
                ),
                affected_count=count,
                expected_value=0,
            )
        )

    if "match_games" in loaded_by_source:
        invalid_sequences = _run_count_query(
            spark,
            "MATCH_GAME_SEQUENCE",
            """
SELECT COUNT(*) AS value
FROM (
    SELECT match_id, game_number
    FROM raw_cert_match_games
    GROUP BY match_id, game_number
    HAVING game_number IS NULL OR game_number < 1 OR COUNT(*) > 1
) AS invalid_games
""".strip(),
        )
        results.append(
            CertificationRuleResult(
                rule_id="RAW_MATCH_GAME_SEQUENCE_INTEGRITY",
                name="Game sequence numbers are valid and unique within a match",
                pillar="Schema and Structural Integrity",
                category="matches",
                status="PASS" if invalid_sequences == 0 else "FAIL",
                severity="info" if invalid_sequences == 0 else "blocker",
                message=(
                    "Game sequence numbers are valid and unique."
                    if invalid_sequences == 0
                    else f"{invalid_sequences} invalid or duplicate game sequence values were found."
                ),
                affected_count=invalid_sequences,
                expected_value=0,
            )
        )

        invalid_scores = _run_count_query(
            spark,
            "MATCH_GAME_SCORE",
            """
SELECT COUNT(*) AS value
FROM raw_cert_match_games
WHERE team_one_score IS NULL
   OR team_two_score IS NULL
   OR team_one_score < 0
   OR team_two_score < 0
   OR team_one_score = team_two_score
   OR (
       winning_team_number = 1
       AND team_one_score <= team_two_score
   )
   OR (
       winning_team_number = 2
       AND team_two_score <= team_one_score
   )
""".strip(),
        )
        results.append(
            CertificationRuleResult(
                rule_id="RAW_MATCH_GAME_SCORE_INTEGRITY",
                name="Game scores and winning team assignments are coherent",
                pillar="Schema and Structural Integrity",
                category="matches",
                status="PASS" if invalid_scores == 0 else "FAIL",
                severity="info" if invalid_scores == 0 else "blocker",
                message=(
                    "Game score integrity passed."
                    if invalid_scores == 0
                    else f"{invalid_scores} invalid game score rows were found."
                ),
                affected_count=invalid_scores,
                expected_value=0,
            )
        )

    return results


def _evaluate_team_identity_rule(
    spark: Any,
    loaded_by_source: dict[str, SourceLoadResult],
) -> list[CertificationRuleResult]:
    if "team_memberships" not in loaded_by_source:
        return []

    invalid_pairs = _run_count_query(
        spark,
        "TEAM_IDENTITY_INVARIANTS",
        """
WITH pair_memberships AS (
    SELECT
        LEAST(CAST(m1.player_id AS STRING), CAST(m2.player_id AS STRING)) AS player_one_id,
        GREATEST(CAST(m1.player_id AS STRING), CAST(m2.player_id AS STRING)) AS player_two_id,
        m1.team_id
    FROM raw_cert_team_memberships AS m1
    INNER JOIN raw_cert_team_memberships AS m2
        ON m1.team_id = m2.team_id
       AND CAST(m1.player_id AS STRING) < CAST(m2.player_id AS STRING)
)
SELECT COUNT(*) AS value
FROM (
    SELECT player_one_id, player_two_id
    FROM pair_memberships
    GROUP BY player_one_id, player_two_id
    HAVING COUNT(DISTINCT team_id) > 1
) AS invalid_pairs
""".strip(),
    )
    return [
        CertificationRuleResult(
            rule_id="RAW_PERSISTENT_TEAM_IDENTITY_INVARIANTS",
            name="Each unordered player pair resolves to at most one persistent team id",
            pillar="Schema and Structural Integrity",
            category="teams",
            status="PASS" if invalid_pairs == 0 else "FAIL",
            severity="info" if invalid_pairs == 0 else "blocker",
            message=(
                "Persistent team identity invariants passed."
                if invalid_pairs == 0
                else f"{invalid_pairs} player pairs map to more than one persistent team id."
            ),
            affected_count=invalid_pairs,
            expected_value=0,
        )
    ]


def _build_duplicate_key_query(temp_view_name: str | None, key_columns: tuple[str, ...]) -> str:
    if not temp_view_name:
        return "SELECT 0 AS value"
    group_columns = ", ".join(key_columns)
    return f"""
SELECT COUNT(*) AS value
FROM (
    SELECT {group_columns}
    FROM {temp_view_name}
    GROUP BY {group_columns}
    HAVING COUNT(*) > 1
) AS duplicate_keys
""".strip()


def _build_foreign_key_query(
    child_view_name: str | None,
    child_columns: tuple[str, ...],
    parent_view_name: str | None,
    parent_columns: tuple[str, ...],
) -> str:
    if not child_view_name or not parent_view_name:
        return "SELECT 0 AS value"
    join_conditions = " AND ".join(
        f"c.{child_column} = p.{parent_column}"
        for child_column, parent_column in zip(child_columns, parent_columns, strict=True)
    )
    not_null_filter = " AND ".join(f"c.{child_column} IS NOT NULL" for child_column in child_columns)
    parent_null_filter = " AND ".join(f"p.{parent_column} IS NULL" for parent_column in parent_columns)
    return f"""
SELECT COUNT(*) AS value
FROM {child_view_name} AS c
LEFT JOIN {parent_view_name} AS p
    ON {join_conditions}
WHERE {not_null_filter}
  AND {parent_null_filter}
""".strip()


def _run_count_query(spark: Any, query_tag: str, sql_text: str) -> int:
    query = f"/* {query_tag} */\n{sql_text}"
    rows = spark.sql(query).collect()
    row = rows[0]
    mapping = row.asDict() if hasattr(row, "asDict") else dict(row)
    return int(mapping.get("value", 0) or 0)

