# Team Identity Pipeline Refactor Implementation Plan

## Purpose

This document recommends pipeline changes needed to accommodate the NAPA student export team identity contract described in `docs/team_identity_student_export_refactor_handoff.md`.

The plan is for later implementation. It intentionally avoids completing student-facing analytical decisions or changing roster-selection methodology.

## Source Contract Change

Student export schema version `1.5` changes team identity semantics:

- `teams.team_type` is now an identity class: `competitive` or `ad_hoc`.
- `teams.team_division` is now the doubles category: `mens_doubles`, `womens_doubles`, `mixed_doubles`, or `open_doubles`.
- `match_teams.team_id` is a required persistent team identifier, projected from internal `source_team_id`.
- `matches.winning_team_id` references persistent `teams.id`, not `match_teams.id`.
- Every non-null `matches.winning_team_id` must match one of the two `match_teams.team_id` values for the same match.
- Incremental exports may include contextual `teams` and `team_memberships` rows because match facts reference those teams.

## Current Repo Assessment

Raw-to-Bronze is mostly schema-preserving and should not need transformation changes. It should still update inventory expectations and validation messages for `teams.team_division`, identity-class `teams.team_type`, and non-null `match_teams.team_id`.

Bronze-to-Silver has the highest required change surface:

- `build_teams` currently derives Silver `team_category` from `team_category`, `category`, or `team_type`.
- SQL `build_teams` currently uses the same fallback order.
- Test fixtures still use division-valued `team_type`.
- `build_match_teams` allows `team_id` to be missing or unresolved because `team_sk` is nullable.
- Winner derivation currently accepts `winning_team_id` matching either `match_teams.id`/`match_team_id` or `match_teams.team_id`, which preserves legacy compatibility but is weaker than the schema `1.5` contract.
- Silver `matches` currently keeps `winning_team_number` and drops the persistent `winning_team_id`.

Silver-to-Gold is partially insulated because it already consumes Silver `teams.team_category` and `match_teams.team_id`, but it should be made explicit that:

- analytical category comes from `team_division` via Silver `team_category`;
- identity class comes from `teams.team_type` via a new Silver field such as `team_identity_type`;
- Olympic candidate logic should not treat `ad_hoc` teams as structured competitive teams unless the instructor explicitly approves that rule.

## Recommended Target Silver Contract

Keep existing Silver names where they support downstream stability, but preserve the new source meanings:

| Silver table | Recommended field | Source field | Purpose |
|---|---|---|---|
| `teams` | `team_identity_type` | `teams.team_type` | Identity class: `COMPETITIVE` or `AD_HOC`. |
| `teams` | `team_category` | `teams.team_division` | Analytical doubles category: `MENS`, `WOMENS`, `MIXED`, `OPEN`. |
| `teams` | `team_status` | `teams.team_status` | Lifecycle status, independent of identity type. |
| `match_teams` | `team_id` | `match_teams.team_id` | Required persistent team identifier. |
| `matches` | `winning_team_id` | `matches.winning_team_id` | Persistent winning `team_id`, nullable for incomplete matches. |
| `matches` | `winning_team_number` | derived from same-match `match_teams.team_id` | Side number convenience field for existing downstream logic. |

Do not overload `team_category` with `competitive` or `ad_hoc`. Do not overload `team_identity_type` with doubles divisions.

## Implementation Plan

### Phase 1: Contracts and Configuration

- Update `config/bronze_to_silver/domains.yml` to split `team_type` into two explicit domains: `team_identity_type` and `team_division`.
- Preserve a legacy domain mapping for old division-valued `team_type` only as a compatibility fallback, not as the preferred schema `1.5` path.
- Add `OPEN` to all Gold-facing team category documentation where it is currently omitted.
- Update `config/bronze_to_silver/silver_tables.yml` comments or table metadata if used by documentation generation to state that `teams.team_category` is sourced from `team_division`.

### Phase 2: Bronze-to-Silver Team Builder

- Update `src/napa_pipeline/bronze_to_silver/organization.py` so `_build_team_candidate` reads `team_division` before legacy `team_category`, `category`, or old division-valued `team_type`.
- Add `team_identity_type` to accepted Silver `teams` rows, normalized from source `team_type`.
- Reject invalid identity values when `team_type` is present and not `competitive` or `ad_hoc`.
- Keep legacy division-valued `team_type` compatibility only when `team_division` is absent and `team_type` matches the old division domain.
- Update record hash inputs to include both `team_identity_type` and `team_category`.
- Apply the same logic to `src/napa_pipeline/bronze_to_silver/organization_sql.py`.

### Phase 3: Bronze-to-Silver Match Builder

- Update `src/napa_pipeline/bronze_to_silver/competition.py` so `match_teams.team_id` is required and must resolve to accepted Silver `teams.team_id`.
- Add a specific reject rule for missing or unresolved `match_teams.team_id`; do not allow silent null `team_sk` for schema `1.5` data.
- Preserve `matches.winning_team_id` in the Silver `matches` table.
- Derive `matches.winning_team_number` by matching `matches.winning_team_id` only to same-match `match_teams.team_id` under the default schema `1.5` contract.
- If legacy winner matching to `match_teams.id` is still needed, put it behind an explicit configuration flag such as `compatibility.allow_legacy_match_team_winner_ids`, defaulting to `false`.
- Apply equivalent changes to `src/napa_pipeline/bronze_to_silver/competition_sql.py` and the SQL column-wrapper logic in `src/napa_pipeline/bronze_to_silver/execute.py`.

### Phase 4: Cross-Table Quality Controls

- Add a Silver cross-table validation that every accepted `match_teams.team_id` resolves to `teams.team_id`.
- Add a Silver cross-table validation that every non-null `matches.winning_team_id` resolves to one of that match's `match_teams.team_id` values.
- Keep existing `winning_team_number`, `winner_flag`, and game-winner consistency checks because they remain useful downstream convenience validations.
- Add a team-pair identity validation that each accepted persistent team has exactly two distinct team members where the source contract requires doubles teams.
- Add a duplicate-pair warning or error that detects multiple active team IDs for the same unordered player pair in the same release. Treat severity as instructor-configurable because historical data and incremental context rows may require nuance.

### Phase 5: Silver-to-Gold Adjustments

- Update `src/napa_pipeline/silver_to_gold/team_resolution.py` to treat direct `match_teams.team_id` as the normal schema `1.5` path, with pair-resolution fallbacks documented as migration or legacy handling.
- Include `team_identity_type` in Gold candidate filters once it is available from Silver.
- For Olympic candidate and recommendation outputs, exclude `AD_HOC` teams from structured competitive candidate pools unless explicitly approved by the instructor.
- Continue deriving player/match results from `winning_team_number` where convenient, but retain `winning_team_id` lineage where Gold outputs need persistent team traceability.
- Update Gold source-contract and target-schema docs to state that `team_category` derives from `team_division`, not source `team_type`.

### Phase 6: Tests

- Update Bronze-to-Silver organization tests to use schema `1.5` fixtures with both `team_type: competitive` and `team_division: mixed_doubles`.
- Add a regression test that `team_type: competitive` does not become `team_category`.
- Add a regression test that `team_division: womens_doubles` maps to `team_category: WOMENS`.
- Add a regression test that `team_identity_type` is preserved as `COMPETITIVE` or `AD_HOC`.
- Update match tests so `winning_team_id` resolves only through `match_teams.team_id` by default.
- Replace or explicitly mark the existing test that derives winners from `match_teams.id` as legacy-compatibility behavior.
- Add tests rejecting missing `match_teams.team_id` and unresolved `match_teams.team_id`.
- Add tests for same-match winner validation.
- Update SQL-plan tests to assert `team_division` is used before `team_type` for category derivation.

### Phase 7: Documentation and Notebooks

- Update `docs/dataset_specification.md` to identify student export schema `1.5` and the new `teams`, `match_teams`, and `matches` relationship contract.
- Update `docs/NAPA_Bronze_to_Silver_Spec.md` to split team identity class from team category and to include `OPEN`.
- Update `docs/gold_source_contract.md`, `docs/gold_discovery_report.md`, and `docs/gold_schema_audit_from_databricks_csv.md` after Databricks rerun evidence is available; the current July 22, 2026 notes describe the old source shape.
- Update `docs/napa_silver_to_gold_layer_engineering_spec_v1.md` references that say category may come from `teams.team_type`.
- Update `docs/data_quality_rules.md` with explicit team identity, team division, persistent match team ID, and persistent winner checks.
- Reclassify `notebooks/07_r2b_diagnose_match_team_identity_loss.py` as a temporary migration diagnostic or retire it after schema `1.5` validation passes.

## Validation Plan

Run local unit tests after code changes:

```powershell
python -m pytest tests/test_bronze_to_silver_organization.py tests/test_bronze_to_silver_organization_sql.py
python -m pytest tests/test_bronze_to_silver_competition.py tests/test_bronze_to_silver_competition_sql.py
python -m pytest tests/test_bronze_to_silver_cross_table.py
python -m pytest tests/test_silver_to_gold_team_resolution.py
python -m pytest tests/test_raw_to_bronze_to_silver_contract.py
```

Run Databricks validation after local tests:

- Raw inventory validation for all three release sizes.
- Raw-to-Bronze publish for `napa_5k`.
- Bronze-to-Silver publish for `napa_5k`.
- Cross-table validation for `napa_5k`.
- Silver-to-Gold Phase 4 persistent-team resolution harness.
- Repeat for `napa_50k` and `napa_250k` once `napa_5k` passes.

## Open Decisions

- Whether to keep legacy `winning_team_id -> match_teams.id` support behind a compatibility flag or remove it entirely.
- Whether Silver should expose `team_division` directly in addition to normalized `team_category`.
- Whether `ad_hoc` teams should ever be eligible for recommendation outputs. Default recommendation: no, unless instructor approves.
- Whether duplicate unordered player pairs should be an error or warning in Silver when incremental contextual rows are present.
- Whether existing Gold target tables should use `team_id` or `resolved_team_id` as their public identifier once direct resolution is complete.

## Risks

- Leaving `team_type` fallback unchanged will misclassify schema `1.5` teams because `competitive` and `ad_hoc` are identity classes, not doubles divisions.
- Leaving `match_teams.team_id` nullable allows the original missing-team problem to propagate into Gold.
- Keeping legacy winner joins enabled by default can hide data that still uses pre-refactor winner semantics.
- Excluding `team_identity_type` from Silver makes it difficult for Gold to separate structured competitive teams from ad hoc match pairs.
- Updating code without refreshing docs will create conflicting instructions for students and downstream agents.
