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

- Status: `implemented`
- Phase: `3`
- Intended purpose: normalized match-side competition fact table for downstream player and team analytics
- Expected Silver dependencies:
  - `matches`
  - `match_teams`
  - `match_team_players`
  - `match_games`
  - `regions`
  - `monthly_batches`
- Current contract notes:
  - Must use deployed Silver fields, not spec-only Bronze assumptions
  - Must treat `matches.winning_team_number` as Silver-derived
  - `side_score` and `opponent_score` are currently implemented as match-side game wins because Silver does not expose a separate side-level match score
  - Future matches after `analysis_as_of_date`, invalid winners, duplicate sides, and overlapping cross-side players are excluded from Phase 3 output
- Current implemented columns:
  - `match_id`
  - `match_team_id`
  - `match_date`
  - `batch_id`
  - `batch_sequence`
  - `batch_date`
  - `region_id`
  - `match_country_code`
  - `match_type`
  - `competition_category`
  - `team_number`
  - `opponent_team_number`
  - `team_id`
  - `opponent_team_id`
  - `winning_team_id`
  - `winning_team_number`
  - `completed_flag`
  - `side_score`
  - `opponent_score`
  - `won_flag`
  - `lost_flag`
  - `games_won`
  - `games_lost`
  - `game_differential`
  - `points_for`
  - `points_against`
  - `point_differential`
  - `point_share`
  - `close_game_count`
  - `deciding_game_flag`
  - `pre_match_team_rating`
  - `opponent_pre_match_team_rating`
  - `player_one_id`
  - `player_two_id`
  - `canonical_player_pair_key`
  - `side_cardinality_warning_flag`
  - `membership_history_warning_flag`
- Unresolved items:
  - whether a separate side-level match score should supersede the current game-win proxy if a better source is approved later

### `competition_player_matches`

- Status: `implemented`
- Phase: `3`
- Intended purpose: player-level competition participation fact table
- Expected Silver dependencies:
  - `competition_match_sides`
  - `match_team_players`
- Current contract notes:
  - `player_position` is normalized in Silver
  - `membership_history_warning_flag` is available in Silver
- Current implemented columns:
  - `match_id`
  - `match_team_id`
  - `match_date`
  - `batch_id`
  - `batch_sequence`
  - `batch_date`
  - `region_id`
  - `match_country_code`
  - `match_type`
  - `competition_category`
  - `team_number`
  - `opponent_team_number`
  - `team_id`
  - `opponent_team_id`
  - `player_id`
  - `player_position`
  - `partner_player_id`
  - `canonical_player_pair_key`
  - `won_flag`
  - `lost_flag`
  - `side_score`
  - `opponent_score`
  - `games_won`
  - `games_lost`
  - `game_differential`
  - `points_for`
  - `points_against`
  - `point_differential`
  - `point_share`
  - `close_game_count`
  - `deciding_game_flag`
  - `pre_match_player_rating`
  - `pre_match_partner_rating`
  - `pre_match_team_rating`
  - `pre_match_opponent_team_rating`
  - `membership_history_warning_flag`
- Unresolved items:
  - later phases may add expectation-based and longitudinal player-match features, but Phase 3 intentionally stops at sourced participation context

### `resolved_match_teams`

- Status: `implemented`
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

### `player_rating_events`

- Status: `implemented`
- Phase: `5`
- Intended purpose: deterministic chronological analytical rating-event table at the player-match grain
- Expected Silver / Gold dependencies:
  - `competition_player_matches`
- Current contract notes:
  - analytical ratings currently initialize at the configured default rating rather than from source player ratings
  - event ordering is deterministic by `match_date`, `batch_sequence`, `batch_id`, and `match_id`
  - each player on a match side receives the same team delta
  - the current implementation is a Python-built reference engine intended for validation and instructor review; large-release scalability still needs a distributed implementation path
- Current implemented columns:
  - `match_id`
  - `match_date`
  - `batch_id`
  - `batch_sequence`
  - `batch_date`
  - `team_number`
  - `player_id`
  - `partner_player_id`
  - `opponent_player_one_id`
  - `opponent_player_two_id`
  - `source_pre_match_player_rating`
  - `pre_match_rating`
  - `team_pre_match_rating`
  - `opponent_team_pre_match_rating`
  - `expected_win_probability`
  - `actual_result`
  - `won_flag`
  - `lost_flag`
  - `k_factor`
  - `margin_multiplier`
  - `rating_delta`
  - `post_match_rating`
  - `prior_match_count`
  - `post_match_count`
  - `wins_to_date`
  - `losses_to_date`
  - `event_sequence`
- Unresolved items:
  - whether future releases should initialize from registration-era priors or another instructor-approved cold-start strategy
  - whether the large-release implementation should migrate from Python driver logic to distributed chronological batch processing

### `player_rating_history`

- Status: `implemented`
- Phase: `5`
- Intended purpose: end-of-day analytical rating snapshots per player
- Expected Silver / Gold dependencies:
  - `players`
  - `player_rating_events`
- Current contract notes:
  - current history rows are one per `player_id` and `rating_effective_date`, using the latest event on that date when a player has multiple same-day matches
- Current implemented columns:
  - `player_id`
  - `rating_effective_date`
  - `latest_match_id`
  - `latest_event_sequence`
  - `batch_id`
  - `batch_sequence`
  - `batch_date`
  - `analytical_rating_value`
  - `rating_change_from_prior`
  - `rated_match_count`
  - `wins_to_date`
  - `losses_to_date`
  - `last_rated_match_date`
  - `rating_reliability_score`
  - `rating_evidence_band`
  - `rating_uncertainty_proxy`
  - `is_current_flag`
- Unresolved items:
  - whether monthly snapshot rows should eventually supplement or replace the current end-of-day history grain

### `player_current_ratings`

- Status: `implemented`
- Phase: `5`
- Intended purpose: latest analytical rating row for every player in the Silver player universe
- Expected Silver / Gold dependencies:
  - `players`
  - `player_rating_history`
- Current contract notes:
  - players without any analytical rating events currently remain in the table with the configured default analytical rating and zero rated matches
- Current implemented columns:
  - `player_id`
  - `display_name`
  - `country_code`
  - `active_flag`
  - `source_rating_value`
  - `source_confidence_score`
  - `analytical_rating_value`
  - `analytical_rating_rank_overall`
  - `rating_difference_from_source`
  - `rating_reliability_score`
  - `rating_evidence_band`
  - `rating_uncertainty_proxy`
  - `rated_match_count`
  - `wins_to_date`
  - `losses_to_date`
  - `last_rated_match_date`
  - `current_rating_effective_date`
- Unresolved items:
  - the source-versus-analytical comparison is currently a direct numeric difference and may need a scale-aware alternative if instructor review confirms source and analytical ratings intentionally use different scales

### `player_match_features`

- Status: `implemented`
- Phase: `6`
- Intended purpose: player-level performance feature table for modeling and evaluation
- Expected Silver / Gold dependencies:
  - `competition_player_matches`
  - `player_current_ratings`
  - `players`
  - current contract is implemented as `player_performance_features`
- Current contract notes:
  - current implementation publishes one row per `player_id` and evidence window
  - four windows are currently emitted: `career`, `trailing_365`, `trailing_180`, `trailing_90`
  - zero-evidence rows remain visible so downstream scorecards can distinguish `NONE` from `LIMITED`
- Current implemented columns:
  - `player_id`
  - `evidence_window`
  - `analysis_as_of_date`
  - `display_name`
  - `country_code`
  - `active_flag`
  - `analytical_rating_value`
  - `rated_match_count_current`
  - `match_count`
  - `win_count`
  - `loss_count`
  - `win_pct`
  - `game_win_pct`
  - `avg_point_share`
  - `avg_point_differential`
  - `avg_expected_win_probability`
  - `performance_above_expectation`
  - `avg_opponent_analytical_rating`
  - `strength_of_schedule`
  - `upset_win_pct`
  - `favorite_loss_pct`
  - `recency_weighted_win_pct`
  - `distinct_partner_count`
  - `primary_partner_match_pct`
  - `performance_with_multiple_partners_flag`
  - `partner_adjusted_performance`
  - `point_share_stddev`
  - `point_differential_stddev`
  - `worst_quartile_point_share`
  - `consistency_score`
  - `consistency_evidence_status`
  - `feature_evidence_status`
- Unresolved items:
  - player performance currently uses the match-side opponent team rating as the strength-of-schedule proxy; later phases may replace this with richer opponent rollups
  - additional windowed features can be added without changing the published grain

### `team_performance_features`

- Status: `planned`
- Phase: `7+`
- Intended purpose: team-level feature table
- Expected Silver / Gold dependencies:
  - `resolved_match_teams`
  - `match_games`
  - `teams`
- Gold columns:
  - pending
- Unresolved items:
  - depends on acceptable confidence in persistent team resolution

### `player_development_features`

- Status: `implemented`
- Phase: `6`
- Intended purpose: as-of-date player trend and future-potential feature table
- Expected Silver / Gold dependencies:
  - `player_rating_history`
  - `player_assessment_history`
  - `player_registrations`
  - `players`
- Current contract notes:
  - one row is published per `player_id` as of `analysis_as_of_date`
  - trend features currently use deterministic linear slopes over the trailing configured trend window
- Current implemented columns:
  - `player_id`
  - `analysis_as_of_date`
  - `display_name`
  - `country_code`
  - `active_flag`
  - `latest_analytical_rating_value`
  - `latest_assessment_value`
  - `latest_assessment_confidence`
  - `rating_change_90`
  - `rating_change_180`
  - `rating_change_total`
  - `rating_slope_per_30_days`
  - `assessment_change_180`
  - `assessment_slope_per_30_days`
  - `confidence_change_180`
  - `rated_match_count`
  - `experience_growth_180`
  - `days_since_registration`
  - `current_registration_flag`
  - `development_momentum_score`
  - `feature_evidence_status`
- Unresolved items:
  - development momentum is currently a transparent bounded weighted composite rather than an instructor-approved final scorecard component
  - volatility-related development measures remain unimplemented because no separate Silver volatility field is currently available

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
