# NAPA Gold Team Confidence Simplification
## Codex Implementation Prompt / Specification

### Objective

Simplify the Gold-layer Olympic team-confidence model so that team confidence is based on **direct team evidence, data quality, and team-resolution certainty**, while player-level confidence remains available as a diagnostic but no longer determines team evidence sufficiency or contributes to the combined team-confidence score.

This change is intentionally narrow. Do **not** redesign unrelated Gold scoring, player scoring, Olympic ranking logic, or the synthetic data generator.

---

# 1. Background and Confirmed Findings

The current implementation combines four confidence components:

```text
team_feature_confidence_raw       30%
player_confidence_raw             25%
data_quality_confidence_raw       25%
team_resolution_confidence_raw    20%
```

Two problems have been confirmed.

## 1.1 Mixed confidence units

The first three components are on a `0–100` scale.

`team_resolution_confidence_raw` is on a `0–1` scale:

```text
direct resolved team ID   1.0
unique active pair        0.9
unique historical pair    0.6
unresolved                0.0
```

The existing weighted calculation incorrectly uses the raw `0–1` value directly with the `0–100` components.

This must be fixed by normalizing team-resolution confidence to `0–100` before weighting.

---

## 1.2 Player confidence is not useful as a team-confidence driver in this dataset

The source Silver field:

```text
players.rating_confidence
```

is `0.1` for 100% of players in the current production dataset.

The Gold rating-reliability model correctly interprets this as `10%`, which causes the player rating-reliability score to have an effective ceiling of approximately `63`.

Observed player evidence distribution:

```text
MODERATE   ~94.78%
LOW        ~5.04%
VERY_LOW   ~0.17%
HIGH       0%
VERY_HIGH  0%
```

Therefore player-level evidence bands currently provide almost no useful discrimination at the high end.

This also causes the existing team evidence classifier to make `SUFFICIENT` effectively unreachable because it requires both players to be above `MODERATE`.

The source-data issue should be documented separately, but this change must **not** attempt to repair or reinterpret source `rating_confidence`.

---

# 2. Design Decision

For this use case, team confidence should answer:

> How confident are we in the analytical evidence supporting this specific team/partnership?

That is better represented by:

1. direct team-performance evidence;
2. supporting data quality;
3. certainty that match and player records resolve to the correct team.

Player-level rating confidence remains useful as a diagnostic for individual-athlete analysis but should not dominate or block team-level evidence sufficiency.

---

# 3. Required Team Evidence Classification Change

Locate the current team evidence classification logic in:

```text
src/napa_pipeline/silver_to_gold/team_selection.py
```

The current combined classification uses four inputs:

```text
team-performance feature status
partnership-effectiveness status
player one evidence_band
player two evidence_band
```

Change the team evidence classification so that **only team and partnership evidence determine team evidence sufficiency**.

## 3.1 Revised classification

### NONE

Set:

```text
evidence_sufficiency_status = NONE
```

when any of the following is true:

```text
no team-performance row exists
OR no partnership-effectiveness row exists
OR team-performance feature status is NULL
OR partnership feature status is NULL
OR team-performance feature status = NONE
OR partnership feature status = NONE
```

### LIMITED

Set:

```text
evidence_sufficiency_status = LIMITED
```

when the team is not `NONE` and either:

```text
team-performance feature status = LIMITED
OR partnership feature status = LIMITED
```

### SUFFICIENT

Set:

```text
evidence_sufficiency_status = SUFFICIENT
```

when:

```text
team-performance feature status = SUFFICIENT
AND partnership feature status = SUFFICIENT
```

Player evidence bands must **not** affect this classification.

---

# 4. Preserve Player Evidence as Diagnostic Information

Do not remove existing player evidence fields.

Retain:

```text
player_one_evidence_band
player_two_evidence_band
player_confidence_raw
```

and any existing player confidence or player evidence columns already present in the scorecard.

If useful and consistent with the current schema, add a diagnostic field such as:

```text
player_evidence_limitation_flag
```

with semantics such as:

```text
true if either player evidence band is:
VERY_LOW
LOW
MODERATE
CRITICAL
```

This flag is optional.

It must **not** affect:

```text
eligible_team_flag
evidence_sufficiency_status
combined_team_confidence
```

unless another existing business rule independently requires it.

Do not remove player evidence from the data model because it remains useful for explainability.

---

# 5. Simplify Combined Team Confidence

Remove `player_confidence_raw` from the weighted team-confidence calculation.

The revised combined team confidence should use:

```text
team_feature_confidence
data_quality_confidence
team_resolution_confidence
```

Player confidence should remain available as a diagnostic field only.

---

# 6. Team-Resolution Normalization

Preserve the existing raw source field:

```text
team_resolution_confidence_raw
```

on its current `0–1` scale for backward compatibility and traceability.

Add or retain a normalized field:

```text
team_resolution_confidence
```

or, if repository naming conventions prefer:

```text
team_resolution_confidence_normalized
```

calculated as:

```text
team_resolution_confidence =
    clamp(
        team_resolution_confidence_raw * 100.0,
        0.0,
        100.0
    )
```

Expected mappings:

```text
1.0 -> 100
0.9 -> 90
0.6 -> 60
0.0 -> 0
```

The weighted combined score must use the normalized `0–100` value.

Do not overwrite the raw `0–1` field.

---

# 7. Revised Confidence Weights

Do not invent an unrelated new weighting scheme.

Preserve the relative proportions of the three retained components from the current model.

Current retained weights are:

```text
team feature     0.30
data quality     0.25
team resolution  0.20
```

Their total is:

```text
0.75
```

Normalize them to sum to `1.0`.

Use proposed defaults:

```text
team_feature_weight      = 0.30 / 0.75 = 0.400000
data_quality_weight      = 0.25 / 0.75 = 0.333333
team_resolution_weight   = 0.20 / 0.75 = 0.266667
```

These are not new arbitrary weights; they preserve the relative importance of the three existing retained components after removing player confidence.

If weights are currently hard-coded, move them into the repository's existing configuration mechanism.

Do not change the relative weighting unless explicitly approved by the instructor/business owner.

---

# 8. Required Check Before Finalizing the Combined Confidence Formula

Before implementing the final weighting, inspect the calculation of:

```text
data_quality_confidence_raw
```

Determine whether it already includes a material team-resolution component or team-resolution coverage component.

If it does, report the overlap before making a larger design change.

Preferred behavior:

- data-quality confidence may include general referential integrity / coverage checks;
- the explicit `team_resolution_confidence` component should represent the quality of resolving this specific team;
- avoid materially double-counting the exact same resolution signal twice.

Do not redesign the entire data-quality model.

If overlap is minor or represents different concepts, proceed with the three-component model and document the distinction.

If the exact same team-resolution score is included inside `data_quality_confidence_raw`, stop and report the duplication before finalizing weights.

---

# 9. Null-Aware Weight Re-Normalization

Use explicit null-aware weighting.

Do not automatically treat a missing component as zero unless the business meaning explicitly defines missing as failure.

For available components:

```text
combined_team_confidence =
    weighted numerator
    /
    available weight
```

For the proposed default weights:

```text
team feature     0.400000
data quality     0.333333
team resolution  0.266667
```

Example:

```text
team_feature_confidence = 80
data_quality_confidence = NULL
team_resolution_confidence = 100
```

Then:

```text
numerator =
    80 * 0.40
  + 100 * 0.266667
```

and:

```text
available_weight =
    0.40 + 0.266667
```

Final confidence is:

```text
numerator / available_weight
```

If all three components are null:

```text
combined_team_confidence = NULL
```

Persist or expose:

```text
confidence_available_weight
```

and preferably:

```text
confidence_component_count
```

for auditability.

---

# 10. Component Bounds

All component values used in the weighted calculation must be on a `0–100` scale and defensively bounded.

Use:

```text
0 <= team_feature_confidence <= 100
0 <= data_quality_confidence <= 100
0 <= team_resolution_confidence <= 100
```

Final:

```text
0 <= combined_team_confidence <= 100
```

when at least one component is available.

---

# 11. Team Feature Confidence

Do not redesign team-feature confidence as part of this change unless required to make the current code operate correctly.

Preserve the existing team-feature confidence logic for now.

The earlier match-volume saturation issue may be recalibrated separately after the team evidence classification has been corrected and the resulting candidate distribution is re-examined.

Do not introduce the previously proposed 50-match logarithmic curve as part of this change unless it already exists in the branch being modified.

This implementation should stay focused.

---

# 12. Eligibility Behavior

Preserve the current structural eligibility concept.

`LIMITED` evidence may remain:

```text
eligible_team_flag = true
eligibility_status = REVIEW_REQUIRED
```

and `NONE` may remain ineligible according to the existing rules.

Do not change the Olympic final-selection fallback logic in this change unless required for consistency.

After the new team evidence classification is implemented, `SUFFICIENT` should become reachable for teams whose:

```text
team-performance evidence = SUFFICIENT
AND
partnership evidence = SUFFICIENT
```

Player evidence must no longer prevent that outcome.

---

# 13. Fix the Existing VERY_LOW Classification Defect

The current team classifier only explicitly treats:

```text
LOW
MODERATE
```

as player evidence limitations, allowing `VERY_LOW` to fall through.

Because player evidence will no longer determine team evidence sufficiency, this specific defect should no longer affect `evidence_sufficiency_status`.

However, if player evidence is used anywhere else for diagnostic/risk classification, ensure the ordering is logically correct:

```text
VERY_LOW
LOW
MODERATE
HIGH
VERY_HIGH
```

Do not allow `VERY_LOW` to behave as if it were stronger than `MODERATE`.

Add a regression test if the value remains in any classification logic.

---

# 14. Source Rating Confidence

Do not change:

```text
silver.players.rating_confidence
```

or its Bronze-to-Silver mapping in this change.

The current production source value:

```text
0.1 for 100% of players
```

should be documented as a source-data limitation.

Do not silently reinterpret `0.1` as `10`, `0.9`, or any other value.

Do not alter the synthetic-data generator unless specifically asked in a separate change.

---

# 15. Recommended SQL Calculation Pattern

Use repository conventions, but the combined score should be equivalent to:

```sql
-- Normalize/bound components in an upstream CTE first.

CASE
    WHEN confidence_available_weight > 0.0
    THEN LEAST(
        100.0,
        GREATEST(
            0.0,
            confidence_weighted_numerator
            / confidence_available_weight
        )
    )
    ELSE NULL
END AS combined_team_confidence
```

Where:

```sql
confidence_weighted_numerator =
      CASE
          WHEN team_feature_confidence IS NOT NULL
          THEN team_feature_confidence * 0.400000
          ELSE 0.0
      END
    + CASE
          WHEN data_quality_confidence IS NOT NULL
          THEN data_quality_confidence * 0.333333
          ELSE 0.0
      END
    + CASE
          WHEN team_resolution_confidence IS NOT NULL
          THEN team_resolution_confidence * 0.266667
          ELSE 0.0
      END
```

and:

```sql
confidence_available_weight =
      CASE
          WHEN team_feature_confidence IS NOT NULL
          THEN 0.400000
          ELSE 0.0
      END
    + CASE
          WHEN data_quality_confidence IS NOT NULL
          THEN 0.333333
          ELSE 0.0
      END
    + CASE
          WHEN team_resolution_confidence IS NOT NULL
          THEN 0.266667
          ELSE 0.0
      END
```

Use configured weights rather than literals if the repository already supports configuration-driven scoring.

---

# 16. Recommended PySpark Calculation Pattern

Use repository style and reusable helper expressions.

Conceptually:

```python
components = [
    ("team_feature_confidence", team_feature_weight),
    ("data_quality_confidence", data_quality_weight),
    ("team_resolution_confidence", team_resolution_weight),
]
```

Build:

```text
weighted numerator
available weight
component count
```

from only non-null components.

Then:

```text
combined_team_confidence =
    numerator / available_weight
```

with defensive clipping to `0–100`.

Do not include:

```text
player_confidence_raw
```

in the numerator or denominator.

---

# 17. Required Tests

Follow the existing repository's pytest/PySpark conventions.

## 17.1 Team evidence classification

Create synthetic cases proving:

### Case A

```text
team feature = SUFFICIENT
partnership = SUFFICIENT
player 1 = MODERATE
player 2 = MODERATE
```

Expected:

```text
evidence_sufficiency_status = SUFFICIENT
```

### Case B

```text
team feature = LIMITED
partnership = SUFFICIENT
```

Expected:

```text
LIMITED
```

### Case C

```text
team feature = SUFFICIENT
partnership = LIMITED
```

Expected:

```text
LIMITED
```

### Case D

```text
team feature = NONE
partnership = SUFFICIENT
```

Expected:

```text
NONE
```

### Case E

```text
team feature = SUFFICIENT
partnership = NULL
```

Expected:

```text
NONE
```

Player evidence bands must not alter these results.

---

## 17.2 Player evidence no longer controls team evidence

Using identical team and partnership statuses:

```text
SUFFICIENT / SUFFICIENT
```

test player combinations:

```text
MODERATE / MODERATE
LOW / HIGH
VERY_LOW / VERY_HIGH
HIGH / HIGH
```

Expected for all:

```text
team evidence = SUFFICIENT
```

The player fields remain present as diagnostics.

---

## 17.3 Resolution normalization

Verify:

```text
1.0 -> 100
0.9 -> 90
0.6 -> 60
0.0 -> 0
```

Also verify output is bounded to `0–100`.

---

## 17.4 Player confidence removed from combined score

Create two otherwise identical synthetic team rows:

Row A:

```text
player_confidence_raw = 10
```

Row B:

```text
player_confidence_raw = 100
```

All three retained team-confidence components are identical.

Expected:

```text
combined_team_confidence(A)
=
combined_team_confidence(B)
```

This is an important regression test.

---

## 17.5 Combined confidence weights

With:

```text
team_feature_confidence = 100
data_quality_confidence = 100
team_resolution_confidence = 100
```

Expected:

```text
combined_team_confidence = 100
```

With:

```text
team_feature_confidence = 80
data_quality_confidence = 90
team_resolution_confidence = 60
```

Expected using normalized proposed weights:

```text
80 * 0.40
+ 90 * 0.333333
+ 60 * 0.266667
```

Use floating-point tolerance.

---

## 17.6 Null reweighting

Example:

```text
team_feature_confidence = 80
data_quality_confidence = NULL
team_resolution_confidence = 100
```

Expected:

```text
available_weight = 0.40 + 0.266667
combined confidence =
    (80*.40 + 100*.266667)
    / available_weight
```

Do not treat the missing data-quality component as zero.

---

## 17.7 All-null components

Expected:

```text
confidence_available_weight = 0
combined_team_confidence = NULL
```

---

## 17.8 Bound tests

Verify every non-null final score remains:

```text
0 <= combined_team_confidence <= 100
```

even if upstream test inputs are outside expected ranges.

---

# 18. Post-Implementation Validation

Run the Gold pipeline and report the following.

## 18.1 Evidence-status distribution

Return:

```text
evidence_sufficiency_status
team_count
percentage
```

Expected result:

- `SUFFICIENT` should now exist if there are teams with both team and partnership statuses `SUFFICIENT`;
- `LIMITED` should still exist for low-evidence teams;
- `NONE` should remain for teams lacking valid team/partnership evidence.

Do not force target percentages.

---

## 18.2 Cross-tab

Report:

```text
team_feature_evidence_status
partnership_feature_evidence_status
final evidence_sufficiency_status
team_count
```

Verify that:

```text
SUFFICIENT + SUFFICIENT -> SUFFICIENT
```

regardless of player evidence bands.

---

## 18.3 Combined-confidence distribution

Return:

```text
count
null count
min
p05
p25
median
mean
p75
p95
max
count(distinct combined_team_confidence)
```

The score must no longer be distorted by the player-confidence source limitation.

---

## 18.4 Resolution check

Confirm mappings:

```text
raw 1.0 -> normalized 100
raw 0.9 -> normalized 90
raw 0.6 -> normalized 60
raw 0.0 -> normalized 0
```

---

# 19. Backward Compatibility

Preserve existing raw fields wherever possible:

```text
player_confidence_raw
team_resolution_confidence_raw
player_one_evidence_band
player_two_evidence_band
```

Do not drop them.

Changing:

```text
combined_team_confidence
evidence_sufficiency_status
```

is intentional and should be documented as a semantic correction.

Update any Gold data dictionary or repository documentation that describes:

- team evidence sufficiency;
- player evidence influence;
- combined confidence;
- confidence-component units.

---

# 20. Files Likely Involved

Inspect the repository before editing. Based on the current implementation, likely files include:

```text
src/napa_pipeline/silver_to_gold/team_selection.py
src/napa_pipeline/silver_to_gold/olympic_team_selection.py
src/napa_pipeline/silver_to_gold/scorecards.py
src/napa_pipeline/silver_to_gold/quality_confidence.py
config/silver_to_gold/*.yml
tests/
docs/
```

Do not modify unrelated files merely because they were inspected.

---

# 21. Completion Criteria

The change is complete when:

- [ ] Player evidence bands no longer determine team `evidence_sufficiency_status`.
- [ ] `SUFFICIENT` is reachable when team and partnership evidence are both sufficient.
- [ ] Player confidence remains available as a diagnostic field.
- [ ] `player_confidence_raw` is removed from the combined team-confidence calculation.
- [ ] `team_resolution_confidence_raw` remains on `0–1`.
- [ ] A normalized team-resolution field is used on `0–100`.
- [ ] Combined confidence uses team feature, data quality, and team resolution only.
- [ ] Relative weighting of retained components is preserved at approximately `40% / 33.33% / 26.67%`.
- [ ] Null components are explicitly reweighted.
- [ ] Final combined confidence is bounded `0–100`.
- [ ] The data-quality calculation is checked for material duplication of team-resolution confidence.
- [ ] The source `rating_confidence = 0.1` issue is documented but not altered.
- [ ] Unit tests pass.
- [ ] Gold pipeline runs successfully.
- [ ] Post-run validation confirms `SUFFICIENT` teams now exist when supported by team/partnership evidence.
- [ ] Relevant documentation is updated.

---

# 22. Final Codex Report

After implementation, report:

1. Files changed.
2. Exact changes to team evidence classification.
3. Exact changes to combined team confidence.
4. Whether data-quality confidence duplicated team-resolution confidence.
5. New or retained configuration values.
6. Tests added or updated.
7. Test results.
8. Before/after evidence-status distribution.
9. Before/after combined-confidence distribution.
10. Any assumptions or remaining issues requiring instructor review.

Do not modify the source player rating-confidence data or synthetic-data generator as part of this change.
Do not redesign unrelated Gold scoring.
Do not make final Olympic roster decisions.
