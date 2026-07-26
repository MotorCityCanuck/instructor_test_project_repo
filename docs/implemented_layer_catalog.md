# Implemented Layer Catalog

**Purpose:** This document summarizes the implemented Raw, Bronze, Silver, Gold, and operations data model for the instructor reference pipelines in this repository. It is a documentation companion to the YAML registries and Python modules, not a replacement for source-controlled configuration.

## Runtime Pattern

The implemented pipelines are full-refresh, release-parameterized Databricks workflows. They run against one Unity Catalog catalog and release-specific medallion schemas.

| Release | Role | Raw schema | Bronze schema | Silver schema | Silver reject schema | Gold schema | Gold stage schema | Operations schema |
|---|---|---|---|---|---|---|---|---|
| `napa_5k` | Development | `workspace.instructor_5k_raw` | `workspace.instructor_5k_bronze` | `workspace.instructor_5k_silver` | `workspace.instructor_5k_silver_reject` | `workspace.instructor_5k_gold` | `workspace.instructor_5k_gold_stage` | `workspace.instructor_ops` |
| `napa_50k` | Validation | `workspace.instructor_50k_raw` | `workspace.instructor_50k_bronze` | `workspace.instructor_50k_silver` | `workspace.instructor_50k_silver_reject` | `workspace.instructor_50k_gold` | `workspace.instructor_50k_gold_stage` | `workspace.instructor_ops` |
| `napa_250k` | Production-scale case dataset | `workspace.instructor_250k_raw` | `workspace.instructor_250k_bronze` | `workspace.instructor_250k_silver` | `workspace.instructor_250k_silver_reject` | `workspace.instructor_250k_gold` | `workspace.instructor_250k_gold_stage` | `workspace.instructor_ops` |

Configured runtime defaults:

| Area | Value |
|---|---|
| Catalog | `workspace` |
| Team prefix | `instructor` |
| Processing mode | `full_refresh` |
| Time zone | `America/New_York` |
| Gold scoring scenario | `BALANCED` |
| Gold analysis date strategy | `MAX_VALID_MATCH_DATE` unless an explicit `analysis_as_of_date` is passed |

## Workflow Catalog

| Workflow resource | Purpose | Parameter names | Script/task source |
|---|---|---|---|
| `napa_raw_to_bronze` | Validate Raw inventory and publish Bronze tables | `release_type` with values `5k`, `50k`, `250k` | `config/raw_to_bronze/workflows/napa_raw_to_bronze.job.yml` |
| `napa_bronze_to_silver` | Validate Bronze sources and publish Silver tables, rejects, and convenience views | `release_name` | `config/bronze_to_silver/workflows/napa_bronze_to_silver.job.yml` |
| `napa_silver_to_gold` | Publish Gold analytical products from Phase 3 through Phase 13 | `release_name`, optional `analysis_as_of_date` | `config/silver_to_gold/workflows/napa_silver_to_gold.job.yml` |
| `napa_silver_to_gold_audit` | Profile and validate published Gold-layer contents | `release_name`, optional `analysis_as_of_date` | `config/silver_to_gold/workflows/napa_silver_to_gold_audit.job.yml` |

## Raw Layer

Raw is a file-based layer stored in a release-specific Unity Catalog Volume named `napa_files`.

| Raw file | Configured source | Business grain |
|---|---|---|
| `regions.parquet` | `regions` | One geographic region |
| `clubs.parquet` | `clubs` | One club or facility |
| `club_memberships.parquet` | `club_memberships` | One player-club membership period |
| `player_master.parquet` | `player_master` | One current or snapshot player row |
| `player_registrations.parquet` | `player_registrations` | One registration event |
| `player_assessment_history.parquet` | `player_assessment_history` | One player assessment observation |
| `teams.parquet` | `teams` | One doubles team |
| `team_memberships.parquet` | `team_memberships` | One player-team membership period |
| `matches.parquet` | `matches` | One match |
| `match_teams.parquet` | `match_teams` | One match side |
| `match_team_players.parquet` | `match_team_players` | One player on a match side |
| `match_games.parquet` | `match_games` | One game within a match |
| `monthly_batches.parquet` | `monthly_batches` | One processing or snapshot batch |

Raw files are treated as immutable source files. The pipeline validates the configured inventory and does not transform Raw content in place.

## Bronze Layer

Bronze is a source-aligned Delta representation of Raw. It preserves source rows and source business columns, then appends ingestion metadata.

| Bronze table | Source file | Configured key columns | Business grain |
|---|---|---|---|
| `regions` | `regions.parquet` | `id` | One geographic region |
| `clubs` | `clubs.parquet` | `id` | One club or facility |
| `club_memberships` | `club_memberships.parquet` | `id` | One player-club membership period |
| `player_master` | `player_master.parquet` | `player_id` | One current or snapshot player row |
| `player_registrations` | `player_registrations.parquet` | `id` | One registration event |
| `player_assessment_history` | `player_assessment_history.parquet` | `id` | One player assessment observation |
| `teams` | `teams.parquet` | `id` | One doubles team |
| `team_memberships` | `team_memberships.parquet` | `id` | One player-team membership period |
| `matches` | `matches.parquet` | `id` | One match |
| `match_teams` | `match_teams.parquet` | `id` | One match side |
| `match_team_players` | `match_team_players.parquet` | `id` | One player on a match side |
| `match_games` | `match_games.parquet` | `id` | One game within a match |
| `monthly_batches` | `monthly_batches.parquet` | `id` | One processing or snapshot batch |

Bronze metadata columns are controlled by `config/raw_to_bronze/base.yml`:

| Metadata column | Purpose |
|---|---|
| `_pipeline_run_id` | Raw-to-Bronze pipeline execution identifier |
| `_pipeline_name` | Pipeline name |
| `_pipeline_version` | Pipeline version |
| `_release_name` | Release identifier |
| `_source_file_name` | Delivered file name |
| `_source_file_path` | Unity Catalog Volume file path |
| `_source_file_size` | Source file size |
| `_source_file_modification_ts` | Source file modification timestamp |
| `_ingested_ts` | Bronze ingestion timestamp |
| `_source_record_hash` | Deterministic source-record hash |

Column-level Bronze/Silver reference for the `napa_5k` validation export is stored in `docs/napa_5k_bronze_silver_columns.csv`.

## Silver Layer

Silver publishes conformed operational entities, reject tables, operations records, and convenience views. Its table registry lives in `config/bronze_to_silver/silver_tables.yml`.

| Silver table | Bronze source | Stage | Build order | Primary key | Reject table |
|---|---|---|---:|---|---|
| `monthly_batches` | `monthly_batches` | `reference` | 10 | `batch_id` | `monthly_batches_exceptions` |
| `regions` | `regions` | `reference` | 20 | `region_id` | `regions_exceptions` |
| `players` | `player_master` | `athlete` | 30 | `player_id` | `players_exceptions` |
| `clubs` | `clubs` | `organization` | 40 | `club_id` | `clubs_exceptions` |
| `teams` | `teams` | `partnership` | 50 | `team_id` | `teams_exceptions` |
| `player_registrations` | `player_registrations` | `athlete` | 60 | `registration_id` | `player_registrations_exceptions` |
| `player_assessment_history` | `player_assessment_history` | `athlete` | 70 | `assessment_id` | `player_assessment_history_exceptions` |
| `club_memberships` | `club_memberships` | `organization` | 80 | `club_membership_id` | `club_memberships_exceptions` |
| `team_memberships` | `team_memberships` | `partnership` | 90 | `team_membership_id` | `team_memberships_exceptions` |
| `matches` | `matches` | `competition` | 100 | `match_id` | `matches_exceptions` |
| `match_teams` | `match_teams` | `competition` | 110 | `match_team_id` | `match_teams_exceptions` |
| `match_team_players` | `match_team_players` | `competition` | 120 | `match_team_player_id` | `match_team_players_exceptions` |
| `match_games` | `match_games` | `competition` | 130 | `match_game_id` | `match_games_exceptions` |

Implemented convenience views:

| View | Purpose |
|---|---|
| `vw_current_team_memberships` | Current team-membership view |
| `vw_match_results` | Match-result view |
| `vw_player_match_history` | Player match-history view |
| `vw_players_current` | Current player view |
| `vw_team_rosters` | Team roster view |

Silver metadata columns are controlled by `config/bronze_to_silver/base.yml` and include `_pipeline_run_id`, `_pipeline_version`, `_source_dataset`, `_source_table`, `_load_ts`, `_record_hash`, and `_data_quality_status`.

## Gold Layer

Gold publishes business-ready analytical outputs from the conformed Silver layer. Its implemented table registry lives in `config/silver_to_gold/gold_tables.yml`.

| Gold table | Phase | Stage | Build order | Primary key | Purpose |
|---|---:|---|---:|---|---|
| `competition_match_sides` | 3 | `foundation` | 10 | `match_id`, `team_number` | Match-side analytical foundation |
| `competition_player_matches` | 3 | `foundation` | 20 | `match_id`, `team_number`, `player_id` | Player-match analytical foundation |
| `resolved_match_teams` | 4 | `foundation` | 30 | `match_id`, `match_team_id` | Persistent team identity resolution |
| `player_rating_events` | 5 | `ratings` | 40 | `match_id`, `player_id` | Per-match analytical rating events |
| `player_rating_history` | 5 | `ratings` | 50 | `player_id`, `rating_effective_date` | Rating history snapshots |
| `player_current_ratings` | 5 | `ratings` | 60 | `player_id` | Current analytical player ratings |
| `player_performance_features` | 6 | `player_features` | 70 | `player_id`, `evidence_window` | Windowed player performance features |
| `player_development_features` | 6 | `player_features` | 80 | `player_id` | Player trend and development features |
| `team_performance_features` | 7 | `team_features` | 90 | `team_id`, `evidence_window` | Windowed team performance features |
| `partnership_effectiveness` | 7 | `team_features` | 100 | `partnership_key` | Doubles partnership effectiveness |
| `entity_data_quality_confidence` | 8 | `quality` | 110 | `entity_type`, `entity_id` | Player/team data quality confidence |
| `match_outcome_training_set` | 9 | `modeling` | 120 | `match_id` | Match-level model training rows |
| `match_outcome_predictions` | 9 | `modeling` | 130 | `match_id` | Baseline match outcome predictions |
| `match_model_metrics` | 9 | `modeling` | 140 | `model_run_id`, `split_name`, `metric_name` | Published model metrics |
| `player_evaluation_scorecards` | 10 | `scorecards` | 150 | `player_id`, `scoring_scenario` | Player evaluation scorecards |
| `national_player_rankings` | 10 | `scorecards` | 160 | `country_code`, `ranking_group`, `player_id`, `scoring_scenario` | National player ranking outputs |
| `team_selection_scorecards` | 11 | `scorecards` | 170 | `team_id`, `scoring_scenario` | Team selection scorecards |
| `olympic_team_candidates` | 11 | `recommendations` | 180 | `country_code`, `category_code`, `team_id`, `scoring_scenario` | Ranked team-candidate layer |
| `olympic_team_recommendations` | 12 | `recommendations` | 190 | `country_code`, `category_code`, `team_id`, `scoring_scenario` | Recommendation statuses and rationale |
| `selection_sensitivity_results` | 13 | `sensitivity` | 200 | `country_code`, `category_code`, `team_id`, `scenario_name` | Scenario sensitivity results |
| `recommendation_explanations` | 13 | `sensitivity` | 210 | `country_code`, `category_code`, `team_id`, `scoring_scenario` | Recommendation explanation rows |

`gold_run_summary` remains configured as a planned publication table at build order 220, but it is not part of the current materialized Phase 3 through Phase 13 workflow.

Column-level Gold contracts are maintained in `docs/gold_target_schema_registry.md`.

## Operations Schema

All pipelines write operational evidence to `workspace.instructor_ops`.

### Raw-to-Bronze operations tables

| Table | Purpose |
|---|---|
| `pipeline_runs` | Raw-to-Bronze run lifecycle |
| `table_runs` | Raw-to-Bronze table-level execution and reconciliation |
| `schema_snapshots` | Raw and Bronze schema snapshots |
| `reconciliation_results` | Raw-to-Bronze row and schema reconciliation |
| `run_messages` | Raw-to-Bronze diagnostic messages |

### Bronze-to-Silver operations tables

| Table | Purpose |
|---|---|
| `b2s_pipeline_runs` | Bronze-to-Silver run lifecycle |
| `b2s_table_runs` | Silver table execution, rejects, and publication metrics |
| `b2s_quality_results` | Silver quality-rule outcomes |
| `b2s_reconciliation_results` | Bronze-to-Silver reconciliation records |
| `b2s_schema_snapshots` | Bronze and Silver schema snapshots |
| `b2s_run_messages` | Bronze-to-Silver diagnostic messages |

### Silver-to-Gold operations tables

| Table | Purpose |
|---|---|
| `pipeline_runs` | Gold phase and audit run lifecycle records |
| `gold_table_runs` | Gold table publication and audit output metrics |
| `gold_quality_results` | Gold source-contract, table, key, alignment, and audit findings |
| `gold_reconciliation_results` | Gold row-balance and audit cross-table checks |
| `gold_model_runs` | Phase 9 model run metadata |
| `gold_model_metrics` | Phase 9 model metric records |
| `gold_recommendation_runs` | Phase 12 recommendation run metadata |
| `gold_table_profile_results` | Standalone Gold audit table profiles |
| `gold_column_profile_results` | Standalone Gold audit column profiles |

Note: Raw-to-Bronze and Silver-to-Gold both use `pipeline_runs` in the shared operations schema. Distinguish records by `pipeline_name` and, where applicable, by Gold-specific fields.

## Source Of Truth

Use this precedence when documents disagree:

1. YAML registries in `config/raw_to_bronze`, `config/bronze_to_silver`, and `config/silver_to_gold`.
2. Python implementation under `src/napa_pipeline`.
3. Deployed Databricks table schemas and the latest schema export.
4. Documentation files.

When documentation drifts from code or deployed tables, update documentation and note whether code or configuration also requires correction.
