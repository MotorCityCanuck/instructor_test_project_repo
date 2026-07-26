## Purpose

This document is the working draft model card for the Gold Phase 9 match-outcome prediction build.

It describes the current instructor reference implementation only. It is not a claim that the methodology is final or mandatory for student teams.

## Current Model Scope

- Primary published model: `analytical_rating_probability`
- Algorithm: deterministic analytical-rating baseline
- Deployment form: pre-match probability generated from Gold `competition_match_sides` team ratings
- Current challenger status: not implemented in the repository reference build yet

## Prediction Unit

- One row per historical match
- Canonical Team A assignment is deterministic:
  - Team A = `team_number = 1`
  - Team B = `team_number = 2`

## Inputs

The current training-set and prediction build uses only Gold fields available before the match:

- `team_a_pre_match_team_rating`
- `team_b_pre_match_team_rating`
- `team_rating_difference`
- `recent_form_difference`
- `adjusted_win_rate_difference`
- `strength_of_schedule_difference`
- `partnership_continuity_difference`
- `consistency_score_difference`
- `rating_reliability_difference`

The published baseline prediction itself currently uses:

- `rating_expected_probability`

computed from the pre-match analytical team ratings.

## Time Controls

- Historical matches after `analysis_as_of_date` are excluded.
- The current repository configuration supports chronological `train` and `validation` splits only.
- A separate `test` split is still a planned extension because the deployed `models.yml` contract currently enforces `train_fraction + validation_fraction = 1.0`.

## Output Tables

- `match_outcome_training_set`
- `match_outcome_predictions`
- `match_model_metrics`

## Published Metrics

The current baseline implementation publishes per-split metrics for:

- `accuracy`
- `precision`
- `recall`
- `f1`
- `roc_auc`
- `log_loss`
- `brier_score`
- `calibration_gap_band_00_20`
- `calibration_gap_band_20_40`
- `calibration_gap_band_40_60`
- `calibration_gap_band_60_80`
- `calibration_gap_band_80_100`

## Limitations

- The current repository implementation is baseline-first and does not yet publish a logistic-regression challenger.
- Calibration is currently summarized as probability-band gap metrics rather than a richer reliability report artifact.
- Team A is deterministic rather than symmetry-expanded; the current build favors transparent historical scoring over data augmentation.
- The baseline probability uses the analytical-rating logistic transform only and does not yet consume the richer feature differences in a fitted challenger model.

## Validation Expectations

Before Phase 9 is considered Databricks-validated for a release:

1. The harness must publish all three Gold tables.
2. Reconciliation must pass for training, prediction, and metric outputs.
3. The model run must be logged in `gold_model_runs`.
4. Metric rows must be logged in both:
   - Gold `match_model_metrics`
   - operations `gold_model_metrics`

