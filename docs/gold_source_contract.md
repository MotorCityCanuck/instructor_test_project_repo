# Gold Source Contract

## Purpose

This document captures the Phase 0 Silver source contract available to the Silver-to-Gold pipeline. As of July 23, 2026, the authoritative contract is the Databricks-exported schema file [docs/napa_5k_bronze_silver_columns.csv](D:/@repos/instructor_test_project_repo/docs/napa_5k_bronze_silver_columns.csv), with supporting interpretation captured in [docs/gold_schema_audit_from_databricks_csv.md](D:/@repos/instructor_test_project_repo/docs/gold_schema_audit_from_databricks_csv.md).

## Source Namespace Pattern

| Release | Silver schema |
|---|---|
| `napa_5k` | `workspace.instructor_5k_silver` |
| `napa_50k` | `workspace.instructor_50k_silver` |
| `napa_250k` | `workspace.instructor_250k_silver` |

## Common Metadata Columns

Implemented accepted Silver rows currently include these metadata fields:

| Column | Notes |
|---|---|
| `_pipeline_run_id` | Bronze-to-Silver pipeline run ID |
| `_pipeline_version` | Bronze-to-Silver pipeline version |
| `_source_dataset` | Release name, for example `napa_5k` |
| `_source_table` | Source Bronze table name |
| `_load_ts` | Silver load timestamp |
| `_record_hash` | Deterministic record hash |
| `_data_quality_status` | Current accepted rows use `ACCEPTED` |

The Silver-to-Gold specification additionally references `_pipeline_name`, `_release_name`, `_source_bronze_table`, and `_bronze_pipeline_run_id`. These are not present in the accepted Silver SQL plans reviewed locally.

## Silver Table Inventory

Physical `DESCRIBE TABLE` output has been provided for:

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

The physical schemas match the locally derived contract below.

### `monthly_batches`

Primary key: `batch_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `batch_id`
- `batch_sk`
- `batch_sequence`
- `batch_date`
- `batch_year`
- `batch_month`
- `batch_quarter`
- `batch_type`
- `batch_status`
- common metadata columns

Gold uses:

- Release as-of date from max `batch_date`.
- Batch chronology from `batch_sequence` and `batch_date`.

### `regions`

Primary key: `region_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `region_id`
- `region_sk`
- `region_name`
- `province_state`
- `country_code`
- common metadata columns

Gold uses:

- Region-to-country lookup.
- Optional reconciliation with `players.country_code`.

Known country domain from config: `USA`, `CAN`.

### `players`

Primary key: `player_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `player_id`
- `player_sk`
- `first_name`
- `last_name`
- `display_name`
- `birth_date`
- `gender`
- `dominant_hand`
- `home_region_id`
- `home_region_sk`
- `country_code`
- `active_flag`
- `age`
- `age_group`
- `rating`
- `rating_confidence`
- common metadata columns

Gold uses:

- Player identity and display fields.
- Direct country code where present.
- Region-derived country through `home_region_id` when direct player country is absent.
- Gender, hand, age group, rating, rating confidence.
- Active status via `active_flag`.

Known domains from config:

- Gender: `M`, `F`.
- Dominant hand: `LEFT`, `RIGHT`, `AMBIDEXTROUS`.

### `player_registrations`

Primary key: `registration_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `registration_id`
- `registration_sk`
- `player_id`
- `player_sk`
- `batch_id`
- `batch_sk`
- `registration_date`
- `registration_type`
- `registration_status`
- `effective_start_date`
- `effective_end_date`
- `current_registration_flag`
- `registration_duration_days`
- `registration_sequence`
- common metadata columns

Gold uses:

- Registration timing and status.
- Current registration eligibility signal, if approved by instructor.

Current-generation source semantics:

- `player_registrations` is an intake event table, not a slowly changing interval table.
- `registration_source` is the source field that exists and should be treated as provenance/type context, not as a registration lifecycle status.
- `effective_start_date` is derived from `registration_month` as the first day of that month when needed for Silver period semantics.
- `effective_end_date` is intentionally unavailable from the source/export contract and remains null by design for current active registration events.
- `registration_status` is a downstream-derived as-of field. In the current Silver logic, rows are `ACTIVE` when `effective_end_date` is null or on/after the release snapshot date; otherwise they are `INACTIVE`.
- `registration_duration_days` is a downstream-derived as-of metric representing the age of the registration record at the release snapshot date, not active-player tenure.
- If downstream interval semantics are required, derive an end date from lifecycle or status logic downstream rather than inferring it from the intake event itself.

### `player_assessment_history`

Primary key: `assessment_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `assessment_id`
- `assessment_sk`
- `player_id`
- `player_sk`
- `batch_id`
- `batch_sk`
- `assessment_date`
- `assessment_type`
- `assessment_value`
- `assessment_confidence`
- `assessor_source`
- common metadata columns

Gold uses:

- Longitudinal player assessments.
- Rating comparison and trend inputs where assessment semantics are approved.

### `clubs`

Primary key: `club_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `club_id`
- `club_sk`
- `club_name`
- `region_id`
- `region_sk`
- `country_code`
- `open_date`
- `close_date`
- common metadata columns

Gold uses:

- Club context and optional player development context.
- Club country is derived through `clubs.region_id -> regions.country_code`.

### `club_memberships`

Primary key: `club_membership_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `club_membership_id`
- `club_membership_sk`
- `player_id`
- `player_sk`
- `club_id`
- `club_sk`
- `membership_start_date`
- `membership_end_date`
- `membership_duration_days`
- `current_membership_flag`
- `membership_overlap_flag`
- common metadata columns

Gold uses:

- Club affiliation and development context.
- Membership overlap warning signal.

Current-generation export behavior:

- `membership_end_date` maps from source/export `end_date`.
- Null `membership_end_date` is expected for active or open-ended club memberships.
- Current 5K, 50K, and 250K release patterns may produce 100% null `end_date` when no club membership churn is modeled.
- If downstream analytics require a closed period, derive an as-of end date downstream without overwriting the source-derived null.
- `membership_duration_days` is a downstream-derived as-of metric: `date_diff('day', membership_start_date, coalesce(membership_end_date, snapshot_end_exclusive))`.
- In the Silver layer, `snapshot_end_exclusive` is represented by the release snapshot date resolved from `monthly_batches`.

### `teams`

Primary key: `team_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `team_id`
- `team_sk`
- `team_category`
- `country_code`
- `team_status`
- `formation_date`
- `dissolution_date`
- `active_flag`
- `team_age_days`
- common metadata columns

Gold uses:

- Persistent team identity.
- Team category.
- Team country.
- Active/dissolved team status.
- Existing-team candidate universe.

Known domains from config:

- Team category: `MENS`, `WOMENS`, `MIXED`, `OPEN`.
- Team status: `ACTIVE`, `INACTIVE`, `DISSOLVED`.

### `team_memberships`

Primary key: `team_membership_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `team_membership_id`
- `team_membership_sk`
- `team_id`
- `team_sk`
- `player_id`
- `player_sk`
- `membership_start_date`
- `membership_end_date`
- `player_role`
- `player_position`
- `membership_duration_days`
- `current_membership_flag`
- `membership_overlap_flag`
- common metadata columns

Gold uses:

- Team roster reconstruction.
- As-of joins by match date or analysis as-of date.
- Player-side and role signals where approved.
- Membership overlap warning signal.

### `matches`

Primary key: `match_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `match_id`
- `match_sk`
- `batch_id`
- `batch_sk`
- `region_id`
- `region_sk`
- `match_date`
- `match_type`
- `winning_team_id`
- `competition_category`
- `match_status`
- `winning_team_number`
- `completed_flag`
- `match_year`
- `match_month`
- common metadata columns

Gold uses:

- Match chronology.
- Match status and completion filtering.
- Persistent winner-team lineage through `winning_team_id`.
- Winner derivation through `winning_team_number`.
- Region and batch joins.

Runtime values and limitations:

- `match_type` values provided: `CHALLENGE`, `CLINIC`, `LADDER`, `LEAGUE`, `RECREATIONAL`, `TOURNAMENT`.
- Bronze `matches` does not provide `competition_category` directly; Silver derives it by renaming source `match_type`.
- Bronze `matches` does not provide `match_status`.
- Bronze `matches` does provide `winning_team_id`, and the current Silver contract preserves it as persistent winner lineage.
- `competition_category` is a required Silver contract column derived from source `match_type`.
- `match_status` exists physically in Silver but should currently be treated as a nullable placeholder unless an approved derivation is introduced.
- `winning_team_number` and `completed_flag` are derived Silver fields, not direct Bronze source fields.
- `winning_team_number` should derive only from same-match `match_teams.team_id` resolution, never from `match_teams.id`.

### `match_teams`

Primary key: `match_team_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `match_team_id`
- `match_team_sk`
- `match_id`
- `match_sk`
- `match_date`
- `team_id`
- `team_sk`
- `team_number`
- `winner_flag`
- `pre_match_team_rating`
- `post_match_team_rating`
- `rating_change`
- `side_cardinality_warning_flag`
- common metadata columns

Gold uses:

- Match-side construction.
- Persistent team attribution through `team_id`.
- Winner flag.
- Source-provided pre/post match team ratings, if used only as visible comparison inputs.
- Side cardinality warning signal.

Phase 0 answer: historical match sides do contain persistent `team_id` in the implemented Silver SQL plan.

### `match_team_players`

Primary key: `match_team_player_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `match_team_player_id`
- `match_team_player_sk`
- `match_team_id`
- `match_team_sk`
- `match_id`
- `match_sk`
- `match_date`
- `team_id`
- `team_sk`
- `player_id`
- `player_sk`
- `player_position`
- `player_rating_at_match`
- `membership_history_warning_flag`
- common metadata columns

Gold uses:

- Player participation by match side.
- Player-team attribution.
- Player rating at match, if used as an explicit source-provided comparison input.
- Membership history warning signal.

### `match_games`

Primary key: `match_game_id`.

Physical schema status: confirmed from provided Databricks `DESCRIBE TABLE` output.

Columns:

- `match_game_id`
- `match_game_sk`
- `match_id`
- `match_sk`
- `game_number`
- `team_one_score`
- `team_two_score`
- `winning_team_number`
- `target_score`
- `win_by`
- `actual_team_one_score_share`
- `score_margin`
- `total_points`
- `close_game_flag`
- `extended_game_flag`
- common metadata columns

Gold uses:

- Game-level points, margins, close-game and extended-game metrics.
- Match winner validation by side.

## Foreign Key Relationships

Known implemented joins:

- `players.home_region_id` to `regions.region_id`.
- `clubs.region_id` to `regions.region_id`.
- `player_registrations.player_id` to `players.player_id`.
- `player_registrations.batch_id` to `monthly_batches.batch_id`.
- `player_assessment_history.player_id` to `players.player_id`.
- `player_assessment_history.batch_id` to `monthly_batches.batch_id`.
- `club_memberships.player_id` to `players.player_id`.
- `club_memberships.club_id` to `clubs.club_id`.
- `team_memberships.player_id` to `players.player_id`.
- `team_memberships.team_id` to `teams.team_id`.
- `matches.batch_id` to `monthly_batches.batch_id`.
- `matches.region_id` to `regions.region_id`.
- `matches.winning_team_id` to same-match `match_teams.team_id`.
- `match_teams.match_id` to `matches.match_id`.
- `match_teams.team_id` to `teams.team_id`.
- `match_team_players.match_team_id` to `match_teams.match_team_id`.
- `match_team_players.player_id` to `players.player_id`.
- `match_games.match_id` to `matches.match_id`.

## Unsupported or Unresolved Concepts

- Actual Databricks physical data types are confirmed for all required Silver source tables.
- Databricks validation on July 22, 2026 confirmed a successful upstream `bronze_to_silver` run for `napa_5k`:
  - `pipeline_run_id = c08cf5ff-efdf-4e74-b12d-705e34cc2ccc`
  - `started_ts = 2026-07-22T11:43:48.850Z`
  - `completed_ts = 2026-07-22T11:53:57.433Z`
- Databricks validation on July 22, 2026 confirmed the current 5K Silver row counts for team and participation tables:
  - `teams = 13,200`
  - `team_memberships = 25,881`
  - `match_teams = 156,148`
  - `match_team_players = 306,300`
- Delivered Bronze values now confirmed through rerun and reject-triage:
  - `teams.team_category` should derive from Bronze `team_division` under schema `1.5`
  - legacy compatibility may still derive `teams.team_category` from division-valued Bronze `team_type` in pre-`1.5` exports
  - `teams.team_status` derives from Bronze `team_status` values including `ACTIVE`, `DORMANT`, and `RETIRED`
  - `team_memberships.player_position` and `match_team_players.player_position` may arrive as numeric values `1` and `2`, normalized to `LEFT` and `RIGHT`
- Player `country_code` is expected to be populated from direct player country or from `regions.country_code` through `home_region_id`. Null values should be treated as missing home-region data or invalid region linkage.
- Match outcome semantics should rely on the current Silver derivation of `winning_team_number` and `completed_flag`, not on any assumed Bronze `match_status`.
- Silver now preserves `matches.winning_team_id` as the persistent winner-team lineage field alongside derived `winning_team_number`.
- `region_type` is requested by Phase 0, but the implemented `regions` Silver plan does not expose a `region_type` column.
- Player status is exposed as `active_flag`, not `player_status`.
- Team identity class and team category are separate source concepts in schema `1.5`.
- Gold should treat `teams.team_category` as the analytical category field and avoid reading source `team_type` as a division field.
- Gold-required metadata names are not fully aligned with current accepted Silver metadata.
- Exact Olympic eligibility rules remain out of scope until instructor configuration is approved.

## Approved Fallbacks Proposed for Instructor Review

- Use `players.country_code` as the Gold player country source after Bronze-to-Silver is rerun. Silver is responsible for deriving it from home region when direct country is absent.
- Use `teams.team_category` as the Gold team category field.
- Use `players.active_flag` and `teams.active_flag` as status booleans, while preserving `teams.team_status` where categorical status is needed.
- Use `match_teams.team_id` for persistent historical team attribution.
- Use `team_memberships` date ranges for as-of roster reconstruction.
- When Bronze schema `1.5` is the source contract, derive `teams.team_category` from `team_division`, not `team_type`.

These fallbacks should be approved before Phase 1 implementation.
