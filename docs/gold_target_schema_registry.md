## Purpose

This document is the working schema registry for Gold-layer tables. It should be updated as each Gold phase is implemented so the target Gold contract stays explicit and reviewable.

This is not a speculative full solution. It is a registry of implemented or approved Gold table contracts.

## Usage Rules

For each Gold table:

1. Add the table before implementation begins.
2. Mark its status:
   - `planned`
   - `in_progress`
   - `implemented`
   - `validated`
3. Record source dependencies.
4. Record the exact Gold columns that are implemented or approved.
5. Note any unresolved derivations or instructor decisions.

Do not mark a table `validated` until it has been run and checked in Databricks.

## Gold Table Inventory

### `competition_match_sides`

- Status: `planned`
- Phase: `3`
- Intended purpose: normalized match-side competition fact table for downstream player and team analytics
- Expected Silver dependencies:
  - `matches`
  - `match_teams`
- Current contract notes:
  - Must use deployed Silver fields, not spec-only Bronze assumptions
  - Must treat `matches.winning_team_number` as Silver-derived
  - Must not assume `matches.competition_category` is populated
- Gold columns:
  - pending phase design finalization
- Unresolved items:
  - whether a match-level category derivation is needed in this table

### `competition_player_matches`

- Status: `planned`
- Phase: `3`
- Intended purpose: player-level competition participation fact table
- Expected Silver dependencies:
  - `matches`
  - `match_teams`
  - `match_team_players`
  - `players`
- Current contract notes:
  - `player_position` is normalized in Silver
  - `membership_history_warning_flag` is available in Silver
- Gold columns:
  - pending phase design finalization
- Unresolved items:
  - whether the table should carry nullable category/status placeholders from `matches`

### `resolved_match_teams`

- Status: `in_progress`
- Phase: `4`
- Intended purpose: resolve historical match sides to persistent team identities
- Expected Silver dependencies:
  - `matches`
  - `match_teams`
  - `match_team_players`
  - `team_memberships`
  - `teams`
- Current implemented columns:
  - `match_id`
  - `match_team_id`
  - `team_number`
  - `match_date`
  - `player_one_id`
  - `player_two_id`
  - `canonical_player_pair_key`
  - `resolved_team_id`
  - `team_resolution_method`
  - `team_resolution_status`
  - `team_resolution_confidence`
  - `candidate_attribution_allowed_flag`
- Current contract notes:
  - Uses `match_teams.team_number`, not Bronze `side_number`
  - Uses `match_teams.team_id` when present
  - Uses membership date windows from Silver
- Unresolved items:
  - observed resolution rate needs further evaluation before downstream team products rely on it heavily

### `team_match_ratings`

- Status: `planned`
- Phase: `5`
- Intended purpose: team-level chronological rating table
- Expected Silver / Gold dependencies:
  - `matches`
  - `match_teams`
  - `resolved_match_teams`
- Current contract notes:
  - Should use deployed Silver rating field names:
    - `pre_match_team_rating`
    - `post_match_team_rating`
  - Must not assume spec-only names such as `average_team_rating_at_match`
- Gold columns:
  - pending phase design finalization
- Unresolved items:
  - whether source-provided pre/post ratings are used as baseline, audit signal, or not at all

### `player_match_features`

- Status: `planned`
- Phase: `6+`
- Intended purpose: player-level feature table for modeling and evaluation
- Expected Silver / Gold dependencies:
  - `competition_player_matches`
  - `players`
  - `player_assessment_history`
  - `player_registrations`
- Gold columns:
  - pending
- Unresolved items:
  - exact feature set should be documented only when approved for implementation

### `team_performance_features`

- Status: `planned`
- Phase: `7+`
- Intended purpose: team-level feature table
- Expected Silver / Gold dependencies:
  - `resolved_match_teams`
  - `team_match_ratings`
  - `match_games`
  - `teams`
- Gold columns:
  - pending
- Unresolved items:
  - depends on acceptable confidence in persistent team resolution

### `recommendation_scorecards`

- Status: `planned`
- Phase: `10+`
- Intended purpose: roster recommendation evidence and explainability outputs
- Expected Silver / Gold dependencies:
  - later Gold analytical products
- Gold columns:
  - pending
- Unresolved items:
  - should not be finalized before upstream category/status derivation decisions are made

## Change Log Expectations

Whenever this registry changes materially:

1. update the relevant phase status;
2. update the implemented columns for affected tables;
3. note any newly approved derivations;
4. keep the registry aligned with Databricks-validated behavior.
