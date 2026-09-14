# NAPA Gold Context Adjustment Enhancements
## Codex Implementation Specification

### Purpose

Enhance the **Silver-to-Gold pipeline** so that Olympic team-selection scoring incorporates three additional contextual performance adjustments:

1. **Regional strength / rating bias**
2. **Age-related current-performance adjustment**
3. **Recent-workload / fatigue adjustment**

These adjustments must be implemented as **transparent, explainable Gold features**. They must not alter Silver business entities or overwrite the existing base performance measures.

The intended scoring sequence is:

```text
raw_team_selection_score
    -> context_adjustment_factor
    -> context_adjusted_team_score
    -> confidence_factor
    -> confidence_adjusted_team_score
    -> existing risk penalty logic
    -> final_team_selection_score
```

The existing `raw_team_selection_score` calculation, existing component weights, eligibility rules, confidence logic, and risk-penalty logic should remain unchanged except that the confidence adjustment must now operate on `context_adjusted_team_score` rather than directly on `raw_team_selection_score`.

---

# 1. Important Implementation Constraints

## 1.1 Inspect the current repository first

Before editing code:

- Identify the current Silver-to-Gold pipeline entry point.
- Identify where the following Gold objects are currently built:
  - `competition_match_sides`
  - `competition_player_matches`
  - `team_performance_features`
  - `team_selection_scorecards`
- Identify the current configuration mechanism for:
  - dataset name / release
  - catalog
  - Silver schema
  - Gold schema
  - analysis-as-of date
- Identify existing unit/integration tests for Gold scoring.
- Preserve the existing architectural style rather than introducing an unrelated framework.

Do not duplicate logic that already exists in reusable helpers.

## 1.2 Do not hard-code the 250K environment

The current calibration work used tables such as:

```text
instructor_250k_silver.players
instructor_250k_gold.competition_match_sides
instructor_250k_gold.competition_player_matches
instructor_250k_gold.team_selection_scorecards
```

These names are examples only.

The implementation must continue to work for the **5K, 50K, and 250K releases through configuration changes only**. Do not hard-code:

- `250k`
- `instructor_250k_silver`
- `instructor_250k_gold`
- catalog names
- schema names
- filesystem paths
- `2025-12-31`

Use the pipeline's existing configuration and `analysis_as_of_date`.

## 1.3 No Silver-layer changes

Do not change the business meaning of existing Silver tables.

The required source fields already exist in Silver and Gold.

Relevant Silver fields include:

### `silver.players`

```text
player_id
birth_date
gender
home_region_id
country_code
active_flag
age
rating
rating_confidence
```

### `silver.teams`

```text
team_id
team_category
country_code
team_status
active_flag
formation_date
dissolution_date
```

### `silver.team_memberships`

```text
team_id
player_id
player_position
current_membership_flag
membership_overlap_flag
```

Relevant existing Gold fields include:

### `gold.competition_match_sides`

```text
match_id
match_date
region_id
match_country_code
competition_category
team_id
opponent_team_id
completed_flag
won_flag
points_for
points_against
pre_match_team_rating
opponent_pre_match_team_rating
player_one_id
player_two_id
side_cardinality_warning_flag
membership_history_warning_flag
```

### `gold.competition_player_matches`

```text
match_id
match_date
team_id
player_id
partner_player_id
won_flag
points_for
points_against
pre_match_player_rating
pre_match_partner_rating
pre_match_team_rating
pre_match_opponent_team_rating
```

### `gold.team_selection_scorecards`

```text
team_id
scoring_scenario
analysis_as_of_date
team_category
country_code
player_one_id
player_two_id
eligible_team_flag
partnership_score
player_strength_score
prediction_score
confidence_component_score
combined_team_confidence
raw_team_selection_score
confidence_factor
confidence_adjusted_team_score
risk_penalty_score
final_team_selection_score
top_strengths
top_risks
ranking_rationale
```

---

# 2. Architectural Approach

Create a new Gold analytical feature product named:

```text
team_context_adjustment_features
```

Preferred grain:

```text
one row per team_id + analysis_as_of_date
```

This table should contain the detailed evidence and component factors used to derive the team-level context adjustment.

Then join the required summary factors into:

```text
team_selection_scorecards
```

This separation is intentional:

- `team_context_adjustment_features` explains **how the contextual factors were derived**.
- `team_selection_scorecards` uses the resulting team-level factors in the final selection score.

If the current repository has an established naming convention that makes a slightly different table name more appropriate, preserve that convention, but document the choice.

---

# 3. Configuration Parameters

Use the repository's existing configuration mechanism. The following values are the calibrated defaults.

```yaml
context_adjustments:
  enabled: true

  regional:
    lookback_days: 365
    min_effective_observations: 100.0
    shrinkage_k: 1000.0
    residual_multiplier: 2.0
    factor_floor: 0.98
    factor_ceiling: 1.02

  age:
    neutral_through_age: 35
    quadratic_coefficient: 0.00006
    max_penalty: 0.06

  fatigue:
    lookback_days: 10
    decay_days: 3.0
    points_scale: 100.0
    threshold: 0.75
    penalty_rate: 0.03
    max_penalty: 0.05

  composite:
    factor_floor: 0.92
    factor_ceiling: 1.02
```

If the existing configuration format is not YAML, implement these settings using the existing pattern rather than adding YAML solely for this feature.

Do not scatter numeric constants throughout SQL/Python code.

---

# 4. Regional Strength / Rating-Bias Adjustment

## 4.1 Intent

Estimate whether players from a home region systematically perform above or below what their existing team ratings predict.

This is a **rating-context correction**, not a replacement for observed performance.

The adjustment must:

- use pre-match ratings;
- use only completed historical matches available as of the analysis date;
- be centered within country;
- shrink low-volume regional effects toward neutral;
- remain small relative to actual team performance.

## 4.2 Expected win probability

For each completed match side with both pre-match ratings available:

```text
expected_win_probability =
    1 /
    (
        1 +
        10 ^ (
            (
                opponent_pre_match_team_rating
                - pre_match_team_rating
            ) / 400
        )
    )
```

Define:

```text
actual_result =
    1.0 if won_flag = true
    0.0 otherwise
```

Then:

```text
rating_residual =
    actual_result - expected_win_probability
```

## 4.3 Attribute the team-side observation to players' home regions

Each match-side observation represents two players.

Attribute one-half of the observation to each player's current `home_region_id`:

```text
player_region_observation_weight = 0.5
```

Therefore:

- if both players are from the same region, the region receives one effective observation;
- if they are from different regions, each region receives 0.5 effective observations.

Join `competition_match_sides.player_one_id` and `player_two_id` to `silver.players`.

Exclude rows where:

- `completed_flag != true`;
- either pre-match team rating is null;
- player ID cannot resolve to a Silver player;
- `home_region_id` is null;
- `side_cardinality_warning_flag = true`.

Use matches within the configured regional lookback period ending on `analysis_as_of_date`.

Do not use future matches.

## 4.4 Regional mean residual

For each:

```text
country_code + home_region_id
```

calculate:

```text
regional_effective_observations =
    SUM(observation_weight)
```

```text
regional_mean_residual =
    SUM(rating_residual * observation_weight)
    / SUM(observation_weight)
```

Also calculate the country-level weighted mean residual using the same observation population:

```text
country_mean_residual =
    SUM(rating_residual * observation_weight)
    / SUM(observation_weight)
```

Center the regional effect:

```text
centered_regional_residual =
    regional_mean_residual
    - country_mean_residual
```

## 4.5 Reliability shrinkage

For a region with effective observation count `N`:

```text
regional_reliability =
    N / (N + 1000.0)
```

Then:

```text
shrunk_regional_residual =
    centered_regional_residual
    * regional_reliability
```

## 4.6 Player regional factor

If:

```text
regional_effective_observations < 100
```

then:

```text
player_regional_strength_factor = 1.0
```

Otherwise:

```text
player_regional_strength_factor =
    1.0
    + clip(
        2.0 * shrunk_regional_residual,
        -0.02,
        +0.02
      )
```

Equivalent bounds:

```text
0.98 <= player_regional_strength_factor <= 1.02
```

A missing or unsupported regional estimate must be **neutral**, not punitive:

```text
factor = 1.0
```

## 4.7 Team regional factor

Calculate the factor independently for each player and combine using the geometric mean:

```text
team_regional_strength_factor =
    sqrt(
        player_one_regional_strength_factor
        * player_two_regional_strength_factor
    )
```

Do not calculate the regional factor from average team geography.

---

# 5. Age Performance Adjustment

## 5.1 Intent

Apply a modest contextual adjustment for current competitive performance at older ages.

This factor must **not** be used as the age logic for future-development candidate selection. Development-potential age logic is a different analytical problem.

Do not penalize younger players in the Olympic-readiness score.

## 5.2 Player age factor

Use the existing Silver `players.age` field.

For each player:

```text
age_excess =
    max(age - 35, 0)
```

```text
age_penalty =
    min(
        0.06,
        0.00006 * age_excess^2
    )
```

```text
player_age_factor =
    1.0 - age_penalty
```

Expected behavior:

| Age | Approx. factor | Approx. effect |
|---:|---:|---:|
| 30 | 1.0000 | 0.00% |
| 35 | 1.0000 | 0.00% |
| 40 | 0.9985 | -0.15% |
| 45 | 0.9940 | -0.60% |
| 50 | 0.9865 | -1.35% |
| 55 | 0.9760 | -2.40% |
| 60 | 0.9625 | -3.75% |
| 65 | 0.9460 | -5.40% |
| 67+ | >= 0.9400 | maximum -6.00% |

If age is null:

```text
player_age_factor = 1.0
```

and record the evidence as incomplete/partial.

## 5.3 Team age factor

Calculate each player independently and use the geometric mean:

```text
team_age_factor =
    sqrt(
        player_one_age_factor
        * player_two_age_factor
    )
```

Do **not** calculate the factor from average team age.

This is important because the observed team population contains substantial partner age differences.

---

# 6. Fatigue / Recent Workload Adjustment

## 6.1 Intent

Represent temporary competitive workload immediately preceding the analysis date.

This is a recent-workload signal, not a long-term durability score.

Use `gold.competition_player_matches`.

## 6.2 Lookback window

For each player, use matches satisfying:

```text
match_date < analysis_as_of_date
```

and:

```text
match_date >= analysis_as_of_date - 10 days
```

The analysis date itself is excluded because the data does not contain match timestamps, so same-day ordering cannot be established reliably.

Do not use future matches.

## 6.3 Per-match workload

For each player-match record:

```text
total_points =
    coalesce(points_for, 0)
    + coalesce(points_against, 0)
```

```text
match_load =
    1.0
    + total_points / 100.0
```

```text
days_ago =
    datediff(
        analysis_as_of_date,
        match_date
    )
```

Apply exponential recency decay:

```text
weighted_match_load =
    match_load
    * exp(-days_ago / 3.0)
```

Player fatigue load:

```text
weighted_fatigue_load =
    SUM(weighted_match_load)
```

Also retain:

```text
matches_last_10_days
```

for explainability.

Players with no matches in the lookback period receive:

```text
weighted_fatigue_load = 0.0
matches_last_10_days = 0
```

## 6.4 Player fatigue factor

No penalty is applied through a weighted fatigue load of 0.75.

```text
fatigue_penalty =
    min(
        0.05,
        0.03 * max(
            weighted_fatigue_load - 0.75,
            0
        )
    )
```

```text
player_fatigue_factor =
    1.0 - fatigue_penalty
```

Expected behavior:

| Weighted fatigue load | Factor |
|---:|---:|
| 0.50 | 1.0000 |
| 0.75 | 1.0000 |
| 1.00 | 0.9925 |
| 1.25 | 0.9850 |
| 1.50 | 0.9775 |
| 2.00 | 0.9625 |
| 2.42+ | 0.9500 floor |

## 6.5 Team fatigue factor

Calculate each player's fatigue independently and combine using the geometric mean:

```text
team_fatigue_factor =
    sqrt(
        player_one_fatigue_factor
        * player_two_fatigue_factor
    )
```

Do not country-normalize fatigue.

A real difference in recent competitive workload should remain visible even if U.S. and Canadian players have different workload distributions.

---

# 7. Composite Context Adjustment

Calculate:

```text
unbounded_context_adjustment_factor =
    team_regional_strength_factor
    * team_age_factor
    * team_fatigue_factor
```

Apply final safety bounds:

```text
context_adjustment_factor =
    clip(
        unbounded_context_adjustment_factor,
        0.92,
        1.02
    )
```

Therefore:

```text
0.92 <= context_adjustment_factor <= 1.02
```

The adjustment is intentionally asymmetric:

- maximum positive contextual adjustment = +2%;
- maximum combined negative contextual adjustment = -8%.

This prevents contextual features from overwhelming actual performance evidence.

---

# 8. New Gold Table

Create:

```text
gold.team_context_adjustment_features
```

with at least the following fields.

## 8.1 Identity / grain

```text
team_id
analysis_as_of_date
player_one_id
player_two_id
```

Use the same stable player ordering already used by `team_selection_scorecards`.

## 8.2 Regional evidence

```text
player_one_home_region_id
player_two_home_region_id

player_one_regional_effective_observations
player_two_regional_effective_observations

player_one_regional_mean_residual
player_two_regional_mean_residual

player_one_country_mean_residual
player_two_country_mean_residual

player_one_regional_reliability
player_two_regional_reliability

player_one_regional_strength_factor
player_two_regional_strength_factor

team_regional_strength_factor
```

## 8.3 Age evidence

```text
player_one_age
player_two_age

player_one_age_factor
player_two_age_factor

team_age_factor
```

## 8.4 Fatigue evidence

```text
player_one_matches_last_10_days
player_two_matches_last_10_days

player_one_fatigue_load
player_two_fatigue_load

player_one_fatigue_factor
player_two_fatigue_factor

team_fatigue_factor
```

## 8.5 Composite fields

```text
unbounded_context_adjustment_factor
context_adjustment_factor
context_evidence_status
```

Recommended `context_evidence_status` values:

```text
COMPLETE
PARTIAL
```

Use `PARTIAL` if one or more contextual inputs are missing and a neutral fallback was required.

Do not make missing context data automatically make a team ineligible.

---

# 9. Changes to `team_selection_scorecards`

Add these columns to the Gold scorecard:

```text
team_regional_strength_factor
team_age_factor
team_fatigue_factor
context_adjustment_factor
context_adjusted_team_score
context_evidence_status
```

Optionally include the player-level factors if doing so is consistent with the existing scorecard design, but the detailed evidence must remain available in `team_context_adjustment_features`.

## 9.1 Preserve raw score

Do not change:

```text
raw_team_selection_score
```

Its existing calculation must remain exactly as it is today.

## 9.2 Add context-adjusted score

Calculate:

```text
context_adjusted_team_score =
    raw_team_selection_score
    * context_adjustment_factor
```

## 9.3 Change confidence-adjusted score

The current confidence adjustment must now operate on the context-adjusted score:

```text
confidence_adjusted_team_score =
    context_adjusted_team_score
    * confidence_factor
```

Do not change the existing calculation of:

```text
confidence_factor
```

## 9.4 Preserve existing risk logic

After recalculating `confidence_adjusted_team_score`, apply the existing risk-penalty logic exactly as before.

Do not independently redesign:

```text
risk_penalty_score
final_team_selection_score
```

The only intended upstream change is the addition of the contextual adjustment before confidence and risk.

---

# 10. Explainability

The adjustment must remain visible and auditable.

At minimum, an analyst must be able to answer:

- What was the team's original raw selection score?
- What was each contextual factor?
- What was the combined context adjustment?
- Which player or regional evidence caused the adjustment?
- What was the context-adjusted score?
- What confidence factor was subsequently applied?
- What risk penalty was applied?
- What final score resulted?

Do not collapse the three factors into a single unexplained derived number.

If the existing scorecard contains a structured rationale-building mechanism, extend it so that material context adjustments can be explained without replacing the existing rationale.

A useful threshold for inclusion in narrative text is:

```text
abs(context_adjustment_factor - 1.0) >= 0.01
```

Do not generate misleading narrative for immaterial adjustments.

---

# 11. Null and Edge-Case Handling

Use neutral defaults rather than arbitrary penalties when evidence is missing.

Required behavior:

```text
missing/unsupported regional evidence -> regional factor = 1.0
missing age                         -> age factor = 1.0
no recent matches                  -> fatigue load = 0.0
no recent matches                  -> fatigue factor = 1.0
```

Also:

- guard against division by zero;
- guard against null ratings;
- do not take square roots of negative values;
- factors must remain within documented bounds;
- duplicate player-match records must not inflate fatigue;
- one player should contribute no more than one fatigue observation per match;
- future-dated matches must never enter regional or fatigue calculations;
- ineligible teams may retain context feature rows if useful, but existing eligibility logic controls whether they are selectable.

If the same `player_id + match_id` appears more than once in `competition_player_matches`, deduplicate to one logical player-match before fatigue aggregation and document the rule used.

---

# 12. Calibration Basis

The formulas above were calibrated against the current 250K production dataset.

Do not hard-code these observations into the implementation; they are provided only to explain the coefficient choices.

## 12.1 Analysis date

Current scorecards use:

```text
analysis_as_of_date = 2025-12-31
```

The code must use the runtime/configured date instead.

## 12.2 Age distribution

Observed active-player age distribution is very similar across country and gender:

```text
mean age: approximately 43
median: approximately 41
75th percentile: approximately 54-55
90th percentile: approximately 67
95th percentile: approximately 73
99th percentile: approximately 82-83
```

Eligible team average age is also approximately 43, with median team age near 42.

Mean partner age difference is approximately 18 years, supporting player-level rather than average-team-age adjustment.

## 12.3 Regional residual distribution

Current region counts:

```text
Canada: 156 regions
USA:    417 regions
```

For regions with at least 50 matches, approximate regional residual distribution:

```text
Canada:
  p10 ≈ -0.0119
  p50 ≈  0.0002
  p90 ≈  0.0091

USA:
  p10 ≈ -0.0033
  p50 ≈  0.0000
  p90 ≈  0.0034
```

The regional effect is therefore intentionally small and reliability-shrunk.

## 12.4 Fatigue distribution

Observed mean weighted fatigue load:

```text
Canada women: ~0.344
Canada men:   ~0.345
USA women:    ~0.493
USA men:      ~0.499
```

The 0.75 threshold was chosen so that ordinary recent activity receives no penalty and the adjustment is concentrated in the heavier-workload tail.

---

# 13. Required Tests

Add automated tests following the repository's current test conventions.

At minimum test the following.

## 13.1 Regional factor tests

1. Region with fewer than 100 effective observations -> factor exactly 1.0.
2. Region with positive centered residual and sufficient evidence -> factor > 1.0.
3. Region with negative centered residual and sufficient evidence -> factor < 1.0.
4. Factor never below 0.98.
5. Factor never above 1.02.
6. Increasing evidence should reduce shrinkage toward 1.0.
7. Country mean centering is applied.
8. Missing region -> factor 1.0.
9. Future matches are excluded.

## 13.2 Age factor tests

Verify approximately:

```text
age 35 -> 1.0000
age 40 -> 0.9985
age 45 -> 0.9940
age 50 -> 0.9865
age 55 -> 0.9760
age 60 -> 0.9625
age 65 -> 0.9460
age 80 -> 0.9400
```

Also:

- age below 35 -> 1.0;
- null age -> 1.0;
- factor never below 0.94;
- factor never above 1.0.

## 13.3 Fatigue factor tests

Verify approximately:

```text
load 0.00 -> 1.0000
load 0.50 -> 1.0000
load 0.75 -> 1.0000
load 1.00 -> 0.9925
load 1.25 -> 0.9850
load 1.50 -> 0.9775
load 2.00 -> 0.9625
load 3.00 -> 0.9500
```

Also:

- no matches -> load 0.0 and factor 1.0;
- future matches excluded;
- duplicate player-match rows do not double-count;
- factor never below 0.95;
- factor never above 1.0.

## 13.4 Team aggregation tests

Verify geometric mean behavior:

```text
team_factor = sqrt(player_1_factor * player_2_factor)
```

for regional, age, and fatigue components.

## 13.5 Composite tests

Verify:

```text
context_adjustment_factor =
    clip(
        regional_factor
        * age_factor
        * fatigue_factor,
        0.92,
        1.02
    )
```

and:

```text
0.92 <= context_adjustment_factor <= 1.02
```

## 13.6 Scorecard integration tests

Verify:

```text
raw_team_selection_score
```

is unchanged from the prior implementation for identical input data.

Verify:

```text
context_adjusted_team_score
=
raw_team_selection_score
* context_adjustment_factor
```

Verify:

```text
confidence_adjusted_team_score
=
context_adjusted_team_score
* confidence_factor
```

Verify that the existing risk penalty is then applied using the pre-existing logic.

---

# 14. Production Validation

After implementation, run the pipeline on the available release used for development and produce a validation summary.

At minimum report:

## 14.1 Factor distributions

For each country and team category:

```text
min
p05
p25
median
p75
p95
max
mean
```

for:

```text
team_regional_strength_factor
team_age_factor
team_fatigue_factor
context_adjustment_factor
```

## 14.2 Score impact

For each country and team category report:

```text
mean(raw_team_selection_score)
mean(context_adjusted_team_score)
mean(final_team_selection_score)
```

and:

```text
mean absolute context score change
maximum positive context score change
maximum negative context score change
```

## 14.3 Ranking movement

Compare rankings before and after the new contextual adjustment.

Report counts of teams that move:

```text
0 positions
1-4 positions
5-9 positions
10-24 positions
25+ positions
```

Also identify the top 20 largest upward and downward movements by:

```text
country_code
team_category
```

This validation is required to ensure that the new contextual factors create useful variation without overwhelming the existing performance model.

## 14.4 Country-level fatigue check

Because the current production distribution shows somewhat greater recent workload among U.S. players than Canadian players, explicitly report:

```text
mean team_fatigue_factor by country
median team_fatigue_factor by country
p10 team_fatigue_factor by country
```

Do not normalize away the difference automatically.

Flag it for review only if the resulting country-level scoring effect appears materially larger than intended.

---

# 15. Documentation Updates

Update the appropriate project documentation to describe:

- purpose of the new Gold contextual features;
- lineage from Silver/Gold source fields;
- formulas;
- configuration parameters;
- null handling;
- factor bounds;
- scorecard integration;
- known limitations.

If the repository contains a Gold data dictionary, add the new table and fields.

If lineage documentation exists, update it to show:

```text
Silver players
        \
Gold competition_match_sides
Gold competition_player_matches
        |
        v
Gold team_context_adjustment_features
        |
        v
Gold team_selection_scorecards
```

Do not rewrite unrelated documentation.

---

# 16. Completion Criteria

The change is complete when all of the following are true:

- [ ] Existing Silver outputs are unchanged.
- [ ] Existing `raw_team_selection_score` calculation is unchanged.
- [ ] Regional strength factor is implemented and reliability-shrunk.
- [ ] Age factor is implemented at the player level.
- [ ] Fatigue load and factor are implemented at the player level.
- [ ] Team factors use geometric means.
- [ ] Composite context factor is bounded to `[0.92, 1.02]`.
- [ ] `team_context_adjustment_features` is produced.
- [ ] Required summary fields are added to `team_selection_scorecards`.
- [ ] Confidence adjustment operates on `context_adjusted_team_score`.
- [ ] Existing risk-penalty logic is preserved.
- [ ] Missing contextual evidence produces neutral factors rather than automatic penalties.
- [ ] Logic is configuration-driven and works across 5K/50K/250K releases.
- [ ] Automated tests pass.
- [ ] Pipeline execution succeeds.
- [ ] Distribution and ranking-impact validation is produced.
- [ ] Relevant data dictionary / lineage documentation is updated.

---

# 17. Final Codex Deliverable

After making the change, provide a concise implementation report containing:

1. Files changed.
2. New Gold table(s) and fields.
3. Configuration additions.
4. Formula implementation summary.
5. Tests added or modified.
6. Test results.
7. Pipeline execution result.
8. Validation distribution summary.
9. Ranking-impact summary.
10. Any assumptions, limitations, or issues requiring instructor review.

Do not silently modify formulas or coefficients from this specification. If implementation constraints require a material change, stop and explain the issue before changing the analytical design.
