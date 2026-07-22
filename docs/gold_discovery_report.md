# Gold Discovery Report

## Purpose

This document records Phase 0 repository and Silver contract discovery for the NAPA Silver-to-Gold build. It does not implement Gold transformations.

## Repository State

- Current branch: `main`.
- Remote: `origin https://github.com/MotorCityCanuck/instructor_test_project_repo`.
- Pre-existing staged change: `docs/NAPA_Bronze_to_Silver_Implementation_Decisions.md`.
- Pre-existing untracked inputs: `docs/napa_silver_to_gold_codex_implementation_plan_v1.md` and `docs/napa_silver_to_gold_layer_engineering_spec_v1.md`.
- No nested `AGENTS.md` files were found.

## Documents Reviewed

- `AGENTS.md`
- `README.md`
- `docs/napa_silver_to_gold_codex_implementation_plan_v1.md`
- `docs/napa_silver_to_gold_layer_engineering_spec_v1.md`
- `docs/NAPA_Bronze_to_Silver_Spec.md`
- `docs/NAPA_Bronze_to_Silver_Plan.md`
- `docs/NAPA_Bronze_to_Silver_Implementation_Decisions.md`
- Existing config, notebook, source, SQL, and test structure

## Current Pipeline Architecture

The repository contains implemented Raw-to-Bronze and Bronze-to-Silver packages under `src/napa_pipeline/`.

- Raw-to-Bronze package: `src/napa_pipeline/raw_to_bronze/`.
- Bronze-to-Silver package: `src/napa_pipeline/bronze_to_silver/`.
- Existing Databricks-style notebooks are flat files under `notebooks/`.
- Existing workflow configs are under `config/raw_to_bronze/workflows/` and `config/bronze_to_silver/workflows/`.
- Existing release configs support `napa_5k`, `napa_50k`, and `napa_250k`.
- Current Databricks bundle sync/include only references raw-to-bronze and bronze-to-silver config trees.

The Silver-to-Gold implementation plan expects a future `src/napa_pipeline/silver_to_gold/` package and `config/silver_to_gold/` configuration tree. These do not exist yet.

## Configuration Discovery

Bronze-to-Silver base configuration:

- Catalog default: `workspace`.
- Team prefix: `instructor`.
- Default release: `napa_5k`.
- Operations schema expression: `${runtime.team_prefix}_ops`.
- Processing mode: `full_refresh`.
- Publication mode: Delta overwrite with staging enabled.

Release-specific Silver schemas:

- `napa_5k`: `workspace.instructor_5k_silver`.
- `napa_50k`: `workspace.instructor_50k_silver`.
- `napa_250k`: `workspace.instructor_250k_silver`.

The Gold spec expects matching release-specific Gold schemas:

- `workspace.instructor_5k_gold`.
- `workspace.instructor_50k_gold`.
- `workspace.instructor_250k_gold`.

## Test Framework

The repository uses `pytest`. Local requirements are minimal:

- `pyyaml`
- `pytest`

Relevant test command:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

## Operations Table Discovery

The implemented Bronze-to-Silver operations module creates the following prefixed Delta tables in the configured operations schema:

- `b2s_pipeline_runs`
- `b2s_table_runs`
- `b2s_quality_results`
- `b2s_reconciliation_results`
- `b2s_schema_snapshots`
- `b2s_run_messages`

The Gold spec references unprefixed `pipeline_runs`, but the implemented Bronze-to-Silver upstream success gate should query `b2s_pipeline_runs` unless the operations naming convention is changed.

The Bronze-to-Silver success gate is compatible with the existing `b2s_pipeline_runs` fields:

- `pipeline_name`
- `release_name`
- `status`
- `pipeline_run_id`

The current Silver table metadata differs from the new Gold spec. Current accepted Silver rows emit:

- `_pipeline_run_id`
- `_pipeline_version`
- `_source_dataset`
- `_source_table`
- `_load_ts`
- `_record_hash`
- `_data_quality_status`

The Gold spec expects additional metadata names that are not currently emitted by Bronze-to-Silver accepted rows:

- `_pipeline_name`
- `_release_name`
- `_source_bronze_table`
- `_bronze_pipeline_run_id`

## Silver Contract Discovery

The implemented Silver inventory matches the default required Gold input inventory:

- `monthly_batches`
- `regions`
- `players`
- `player_registrations`
- `player_assessment_history`
- `clubs`
- `club_memberships`
- `teams`
- `team_memberships`
- `matches`
- `match_teams`
- `match_team_players`
- `match_games`

Detailed columns and known keys are documented in `docs/gold_source_contract.md`.

Databricks `DESCRIBE TABLE` output has been provided and reconciled for all required Silver tables:

- `monthly_batches`
- `regions`
- `clubs`
- `club_memberships`
- `players`
- `player_registrations`
- `player_assessment_history`
- `teams`
- `team_memberships`
- `matches`
- `match_teams`
- `match_team_players`
- `match_games`

The provided physical schemas match the locally derived Bronze-to-Silver SQL-plan contract.

## Code Value Discovery

Configured accepted domain values are:

- Country code: `USA`, `CAN`.
- Gender: `M`, `F`.
- Player status: `ACTIVE`, `INACTIVE`.
- Team status: `ACTIVE`, `INACTIVE`, `DISSOLVED`.
- Team type/category: `MENS`, `WOMENS`, `MIXED`, `OPEN`.
- Player position: `LEFT`, `RIGHT`.

Runtime-only values still need Databricks inspection:

- Actual values present in Silver `batch_type` and `batch_status`.
- Residual reject patterns after the latest Bronze-to-Silver rerun.

## Key Phase 0 Findings

- `match_teams` includes persistent `team_id` in the implemented Silver SQL plan.
- `match_team_players` includes `team_id`, `match_id`, `match_date`, and `player_id`, which supports player-match and team-match attribution.
- `players.country_code` is derived from direct player country where present, otherwise from `regions.country_code` through `players.home_region_id`.
- `team_memberships` includes `membership_start_date`, `membership_end_date`, and `current_membership_flag`, supporting as-of membership logic.
- `club_memberships` includes equivalent membership dates and flags.
- `matches` includes `winning_team_number` and `completed_flag`.
- `match_games` includes game-level scores and winning side, supporting winner validation and point metrics.
- Provided `matches` profiling shows `match_type` values: `CHALLENGE`, `CLINIC`, `LADDER`, `LEAGUE`, `RECREATIONAL`, `TOURNAMENT`.
- Provided `matches` profiling shows `competition_category` and `match_status` are null for all profiled distinct rows, and `completed_flag` is false for all profiled distinct rows.
- Provided `players` profiling confirms gender values `F` and `M`. Player `country_code` is expected to be populated from home region when direct player country is absent.
- Provided `players` profiling shows `active_flag` values null, false, and true.
- Databricks validation on July 22, 2026 confirmed a successful latest 5K Bronze-to-Silver run in `workspace.instructor_ops.b2s_pipeline_runs` with:
  - `pipeline_run_id = c08cf5ff-efdf-4e74-b12d-705e34cc2ccc`
  - `release_name = napa_5k`
  - `started_ts = 2026-07-22T11:43:48.850Z`
  - `completed_ts = 2026-07-22T11:53:57.433Z`
- Databricks row-count validation on July 22, 2026 confirmed current 5K Silver population for all required tables:
  - `monthly_batches = 12`
  - `regions = 494`
  - `clubs = 2,430`
  - `club_memberships = 5,812`
  - `players = 6,096`
  - `player_registrations = 6,096`
  - `player_assessment_history = 283,847`
  - `teams = 13,200`
  - `team_memberships = 25,881`
  - `matches = 78,074`
  - `match_teams = 156,148`
  - `match_team_players = 306,300`
  - `match_games = 117,439`
- Databricks domain validation on July 22, 2026 confirmed delivered Bronze-to-Silver mappings now handle:
  - `team_type`: `MENS_DOUBLES`, `WOMENS_DOUBLES`, `MIXED_DOUBLES`, `OPEN_DOUBLES`
  - `team_status`: `ACTIVE`, `DORMANT`, `RETIRED`
  - `player_position`: numeric values `1` and `2`, normalized to `LEFT` and `RIGHT`

## Issues and Concerns

- The new Silver-to-Gold specification references versioned file names such as `NAPA_Bronze_to_Silver_Spec_v1.1.md`, but the actual repository file is `docs/NAPA_Bronze_to_Silver_Spec.md`.
- `databricks.yml` does not include `config/silver_to_gold/**`; future Gold workflow/config files would not deploy until the bundle is updated.
- The Gold spec's required Silver metadata does not match the implemented Silver metadata exactly.
- Player `country_code` should not be null when `home_region_id` resolves to a valid region with `country_code`. Any remaining null player country values should be investigated as missing home-region data or invalid region linkage.
- `matches` appears to contain no completed matches in the profiled distinct values. This blocks match outcome, rating, team-performance, and recommendation analytics until confirmed or corrected.
- `competition_category` is null in the profiled `matches` values. This blocks category-specific Gold products unless category can be derived from `teams.team_category` or another approved source.
- No local Delta/Spark Silver tables are available in this repository. Physical schemas and profiling output were provided externally, but row counts and source correction must occur in Databricks.
- The current repo is on `main`; `AGENTS.md` and the implementation plan recommend using a feature branch for implementation.
- `README.md` describes the repository as a student scaffold with no completed pipeline logic, while the current repository already contains instructor pipeline code and the new docs specify an instructor reference implementation. This should be treated as instructor-facing work and kept clearly separate from student-facing scaffolding.

## Runtime Checks Still Required

Run these validation checks in Databricks before or during Phase 1 implementation:

```sql
SELECT
    target_table,
    status,
    accepted_row_count,
    rejected_row_count,
    published_row_count,
    error_message
FROM workspace.instructor_ops.b2s_table_runs
WHERE pipeline_run_id = 'c08cf5ff-efdf-4e74-b12d-705e34cc2ccc'
ORDER BY build_order;

SELECT 'monthly_batches' AS table_name, COUNT(*) AS row_count FROM workspace.instructor_5k_silver.monthly_batches
UNION ALL SELECT 'regions', COUNT(*) FROM workspace.instructor_5k_silver.regions
UNION ALL SELECT 'clubs', COUNT(*) FROM workspace.instructor_5k_silver.clubs
UNION ALL SELECT 'club_memberships', COUNT(*) FROM workspace.instructor_5k_silver.club_memberships
UNION ALL SELECT 'players', COUNT(*) FROM workspace.instructor_5k_silver.players
UNION ALL SELECT 'player_registrations', COUNT(*) FROM workspace.instructor_5k_silver.player_registrations
UNION ALL SELECT 'player_assessment_history', COUNT(*) FROM workspace.instructor_5k_silver.player_assessment_history
UNION ALL SELECT 'teams', COUNT(*) FROM workspace.instructor_5k_silver.teams
UNION ALL SELECT 'team_memberships', COUNT(*) FROM workspace.instructor_5k_silver.team_memberships
UNION ALL SELECT 'matches', COUNT(*) FROM workspace.instructor_5k_silver.matches
UNION ALL SELECT 'match_teams', COUNT(*) FROM workspace.instructor_5k_silver.match_teams
UNION ALL SELECT 'match_team_players', COUNT(*) FROM workspace.instructor_5k_silver.match_team_players
UNION ALL SELECT 'match_games', COUNT(*) FROM workspace.instructor_5k_silver.match_games;
```

## Instructor Review Decisions

- Decide whether Gold should accept current Silver metadata names or require Bronze-to-Silver metadata changes before Gold implementation.
- Confirm whether the Gold package should be added on a new branch before Phase 1.
- Confirm whether `workspace` is the intended instructor-test catalog for all three releases.
- Confirm corrected `players.country_code` profiling output shows no null values after rerunning Bronze-to-Silver.
- Confirm accepted `match_type` values: `CHALLENGE`, `CLINIC`, `LADDER`, `LEAGUE`, `RECREATIONAL`, `TOURNAMENT`.
- Confirm whether `competition_category` should be sourced from `matches.competition_category`, `teams.team_category`, or another approved derivation.
- Confirm whether current team membership as-of logic should use `matches.match_date` or release `analysis_as_of_date` when reconstructing historical rosters.

## Phase 0 Status

Phase 0 repository and schema discovery is complete. The upstream Bronze-to-Silver success gate is now verified for `napa_5k`, and the required team and competition-participation Silver tables are populated. Silver-to-Gold work may proceed to Phase 1, with remaining focus on metadata alignment, player-country residuals, and category-derivation decisions rather than structural source-table blockers.
