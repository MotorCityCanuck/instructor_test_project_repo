# Lineage

**Purpose:** This document summarizes implemented source-to-output lineage for the instructor NAPA medallion pipelines.

## End-To-End Flow

```text
Raw Parquet files
  -> Bronze source-aligned Delta tables
  -> Silver conformed operational entities, reject tables, and convenience views
  -> Gold analytical products
  -> Gold audit profiles and anomaly records
```

All release-specific business tables are isolated by schema. All durable run evidence is written to `workspace.instructor_ops`.

## Raw To Bronze

The Raw-to-Bronze workflow reads the thirteen configured files from the release-specific `napa_files` Volume and writes same-grain Bronze Delta tables.

| Raw file | Bronze table |
|---|---|
| `regions.parquet` | `regions` |
| `clubs.parquet` | `clubs` |
| `club_memberships.parquet` | `club_memberships` |
| `player_master.parquet` | `player_master` |
| `player_registrations.parquet` | `player_registrations` |
| `player_assessment_history.parquet` | `player_assessment_history` |
| `teams.parquet` | `teams` |
| `team_memberships.parquet` | `team_memberships` |
| `matches.parquet` | `matches` |
| `match_teams.parquet` | `match_teams` |
| `match_team_players.parquet` | `match_team_players` |
| `match_games.parquet` | `match_games` |
| `monthly_batches.parquet` | `monthly_batches` |

Bronze adds ingestion metadata and records row/schema reconciliation in Raw-to-Bronze operations tables.

## Bronze To Silver

The Bronze-to-Silver workflow converts source-aligned Bronze tables into conformed Silver entities.

| Silver stage | Silver tables |
|---|---|
| `reference` | `monthly_batches`, `regions` |
| `athlete` | `players`, `player_registrations`, `player_assessment_history` |
| `organization` | `clubs`, `club_memberships` |
| `partnership` | `teams`, `team_memberships` |
| `competition` | `matches`, `match_teams`, `match_team_players`, `match_games` |

Silver quality, reconciliation, schema snapshots, and diagnostic messages are stored in `b2s_*` operations tables.

## Silver To Gold

The Silver-to-Gold workflow publishes Gold outputs in phase order.

| Phase | Gold outputs |
|---:|---|
| 3 | `competition_match_sides`, `competition_player_matches` |
| 4 | `resolved_match_teams` |
| 5 | `player_rating_events`, `player_rating_history`, `player_current_ratings` |
| 6 | `player_performance_features`, `player_development_features` |
| 7 | `team_performance_features`, `partnership_effectiveness` |
| 8 | `entity_data_quality_confidence` |
| 9 | `match_outcome_training_set`, `match_outcome_predictions`, `match_model_metrics` |
| 10 | `player_evaluation_scorecards`, `national_player_rankings` |
| 11 | `team_selection_scorecards`, `olympic_team_candidates` |
| 12 | `olympic_team_recommendations` |
| 13 | `selection_sensitivity_results`, `recommendation_explanations` |

Gold source dependencies are documented at table level in [gold_target_schema_registry.md](gold_target_schema_registry.md). Gold execution evidence is stored in `pipeline_runs`, `gold_table_runs`, `gold_quality_results`, `gold_reconciliation_results`, `gold_model_runs`, `gold_model_metrics`, and `gold_recommendation_runs`.

## Gold Audit

The standalone Gold audit workflow reads published Gold tables and writes diagnostic outputs:

| Audit output | Purpose |
|---|---|
| `gold_table_profile_results` | One profile row per audited Gold table |
| `gold_column_profile_results` | One profile row per audited table-column pair |
| `gold_quality_results` | Missing table, primary-key, alignment, and empty-table anomaly records |
| `gold_reconciliation_results` | Cross-table row-balance checks |

## Reviewer Traceability

To trace a recommendation:

1. Start in `olympic_team_recommendations`.
2. Join to `recommendation_explanations` for rationale text and stability summary.
3. Join to `selection_sensitivity_results` by country, category, team, and scenario.
4. Join to `team_selection_scorecards` for component scores and eligibility reason codes.
5. Trace team evidence through `team_performance_features`, `partnership_effectiveness`, and `entity_data_quality_confidence`.
6. Trace player evidence through `player_evaluation_scorecards`, `player_performance_features`, `player_development_features`, and `player_current_ratings`.
7. Trace competition evidence back through `competition_match_sides`, `competition_player_matches`, and Silver `matches` / `match_teams` / `match_team_players` / `match_games`.

## Known Lineage Gaps

- `gold_run_summary` is configured but not currently materialized by the Gold workflow.
- The current Gold workflow uses phase harnesses that each create their own Gold pipeline run records. A future unified Gold workflow run ID would simplify run-level lineage across Phase 3 through Phase 13.
- Bronze and Silver column-level documentation is currently based on the `napa_5k` Databricks export. Regenerate that export after schema changes or after validating larger releases.
