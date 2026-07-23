## Purpose

This document records the Silver-to-Gold schema audit performed on July 23, 2026 against the Databricks-exported file [napa_5k_bronze_silver_columns.csv](D:/@repos/instructor_test_project_repo/docs/napa_5k_bronze_silver_columns.csv). The CSV is the authoritative Bronze and Silver contract for this repository until replaced by a newer export.

## Authoritative Source

Use the Databricks export, not the earlier inferred contract, as the source of truth for Bronze and Silver field names and data types.

Authoritative file:

- [docs/napa_5k_bronze_silver_columns.csv](D:/@repos/instructor_test_project_repo/docs/napa_5k_bronze_silver_columns.csv)

Audited schemas:

- `workspace.instructor_5k_bronze`
- `workspace.instructor_5k_silver`

## Confirmed Bronze Competition Contract

### `workspace.instructor_5k_bronze.matches`

Present columns:

- `id`
- `match_date`
- `region_id`
- `match_type`
- `court_type`
- `match_format`
- `winning_team_id`
- `total_points_played`
- `batch_id`

Not present:

- `competition_category`
- `match_status`
- `winning_team_number`

### `workspace.instructor_5k_bronze.match_teams`

Present columns:

- `id`
- `match_id`
- `team_number`
- `team_id`
- `team_score`
- `average_team_rating`

Not present:

- `side_number`

### `workspace.instructor_5k_bronze.match_team_players`

Present columns include:

- `id`
- `match_team_id`
- `player_id`
- `player_position`
- `player_rating_at_match`

Note:

- `player_position` is `bigint` in Bronze, not a string side label.

### `workspace.instructor_5k_bronze.team_memberships`

Present columns include:

- `id`
- `team_id`
- `player_id`
- `player_position`
- `joined_date`
- `left_date`

Notes:

- `player_position` is `bigint` in Bronze.
- Membership dates are `joined_date` and `left_date` in Bronze, not Silver-style names.

## Confirmed Silver Competition Contract

### `workspace.instructor_5k_silver.matches`

Present columns:

- `match_id`
- `match_sk`
- `batch_id`
- `batch_sk`
- `region_id`
- `region_sk`
- `match_date`
- `match_type`
- `competition_category`
- `match_status`
- `winning_team_number`
- `completed_flag`
- `match_year`
- `match_month`

Important interpretation:

- `competition_category` and `match_status` exist physically in Silver, but the Bronze contract does not provide source fields for them.
- Until approved derivation logic is added, these columns should be treated as currently nullable analytical placeholders, not trusted source facts.
- `winning_team_number` is a derived Silver field.
- `completed_flag` is a derived Silver field.

### `workspace.instructor_5k_silver.match_teams`

Present columns:

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

Important interpretation:

- The deployed Silver schema uses `pre_match_team_rating` and `post_match_team_rating`.
- Earlier Gold planning language that referenced `average_team_rating_at_match` or `opponent_average_team_rating_at_match` does not match the current Silver contract.

### `workspace.instructor_5k_silver.match_team_players`

Present columns:

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

### `workspace.instructor_5k_silver.team_memberships`

Present columns:

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

## Gold Assumption Audit

### Safe assumptions for Gold now

- `matches.match_date` is available and should drive chronology.
- `matches.winning_team_number` is the current winner-side field after Bronze-to-Silver derivation.
- `matches.completed_flag` is a Silver-derived field and can be used only with the understanding that it depends on winner derivation quality.
- `match_teams.team_number` is the authoritative side number field.
- `match_teams.team_id` is the persistent team identifier when populated.
- `match_team_players.player_position` and `team_memberships.player_position` are already normalized in Silver.
- `teams.team_category` is the current team category field for team-based Gold logic.

### Unsafe assumptions that should be avoided

- Do not assume Bronze `matches` has `competition_category`.
- Do not assume Bronze `matches` has `match_status`.
- Do not assume Bronze `matches` has `winning_team_number`.
- Do not assume Bronze `match_teams` has `side_number`.
- Do not assume Silver `matches.competition_category` is populated from source.
- Do not assume Silver `matches.match_status` is populated from source.
- Do not assume Gold feature logic can rely on `average_team_rating_at_match`; the deployed Silver names are `pre_match_team_rating` and `post_match_team_rating`.

## Phase Impact

### Phase 3 competition foundation

Implications:

- Competition chronology is supported by `matches.match_date`.
- Winner-side logic should use `matches.winning_team_number`.
- Any filtering based on `match_status` should be treated as untrusted until explicit derivation logic exists.
- Any category-specific branching should not depend on `matches.competition_category` without an approved derivation.

### Phase 4 persistent team resolution

Implications:

- The Phase 4 implementation is structurally aligned with the deployed Silver schema.
- Persistent-team resolution should continue to use:
  - `match_teams.team_id`
  - `match_teams.team_number`
  - `match_team_players.player_id`
  - `team_memberships.membership_start_date`
  - `team_memberships.membership_end_date`

### Later Gold phases

Before implementing category-specific features, scorecards, or recommendations, first decide:

1. whether `competition_category` should be derived from `teams.team_category`;
2. whether a match-level category should be inferred only when both sides agree;
3. whether `match_status` should remain null, or whether a derived status vocabulary should be introduced.

## Recommended Guardrails

1. Treat the CSV export as the contract gate for future Gold work.
2. When Gold code references a Silver field, verify it exists in the export before implementing.
3. Prefer fields already materialized in Silver over re-deriving from assumed Bronze columns.
4. For any planned derivation that fills currently nullable Silver fields, document it explicitly before relying on it downstream.
