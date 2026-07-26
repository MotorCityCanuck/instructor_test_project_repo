# Data Dictionary

**Purpose:** This document points reviewers to the implemented table catalogs and column-level references for the instructor NAPA pipelines.

## Current Source Of Truth

The implemented data dictionary is split across configuration, deployed schema exports, and Gold contract documentation:

| Scope | Source |
|---|---|
| Raw file inventory and Bronze table mapping | `config/raw_to_bronze/raw_sources.yml` |
| Silver table registry, sources, build order, keys, and reject tables | `config/bronze_to_silver/silver_tables.yml` |
| Gold table registry, phases, build order, and keys | `config/silver_to_gold/gold_tables.yml` |
| Bronze/Silver column export from Databricks | `docs/napa_5k_bronze_silver_columns.csv` |
| Gold column-level contract | `docs/gold_target_schema_registry.md` |
| Cross-layer summary catalog | `docs/implemented_layer_catalog.md` |

## Layer Catalog Summary

### Raw

Raw contains thirteen delivered Parquet files under the release-specific `napa_files` Volume. See [implemented_layer_catalog.md](implemented_layer_catalog.md) for the complete file inventory.

### Bronze

Bronze contains one Delta table per Raw file and appends ingestion metadata. Bronze preserves duplicate rows, null values, source data types where possible, and invalid business values for downstream validation.

### Silver

Silver contains thirteen conformed operational tables:

```text
monthly_batches
regions
players
clubs
teams
player_registrations
player_assessment_history
club_memberships
team_memberships
matches
match_teams
match_team_players
match_games
```

Silver also publishes reject tables in the release-specific Silver reject schema and convenience views in the Silver schema.

### Gold

Gold currently materializes the Phase 3 through Phase 13 analytical tables:

```text
competition_match_sides
competition_player_matches
resolved_match_teams
player_rating_events
player_rating_history
player_current_ratings
player_performance_features
player_development_features
team_performance_features
partnership_effectiveness
entity_data_quality_confidence
match_outcome_training_set
match_outcome_predictions
match_model_metrics
player_evaluation_scorecards
national_player_rankings
team_selection_scorecards
olympic_team_candidates
olympic_team_recommendations
selection_sensitivity_results
recommendation_explanations
```

`gold_run_summary` remains a planned configured table and is not yet part of the current materialized Gold workflow.

## Column-Level Documentation

Use the Databricks schema export for Bronze and Silver:

```text
docs/napa_5k_bronze_silver_columns.csv
```

Use the Gold registry for Gold:

```text
docs/gold_target_schema_registry.md
```

## Maintenance Notes

- Update this document when the implemented table inventory changes.
- Update `implemented_layer_catalog.md` when schemas, workflows, operations tables, or release naming changes.
- Update `gold_target_schema_registry.md` when Gold table columns or contract notes change.
- Regenerate or replace `napa_5k_bronze_silver_columns.csv` after a deployed Bronze/Silver schema change.
