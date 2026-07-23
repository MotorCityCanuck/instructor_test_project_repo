## Purpose

This document defines the contract-first workflow for Silver-to-Gold development in this repository. Its purpose is to prevent future mismatches between repository assumptions and the actual Databricks Bronze and Silver schemas.

## Core Rule

Gold implementation must follow the deployed Databricks contract, not memory, not earlier profiling notes, and not specification prose alone.

The current authoritative Bronze and Silver contract is:

- [docs/napa_5k_bronze_silver_columns.csv](D:/@repos/instructor_test_project_repo/docs/napa_5k_bronze_silver_columns.csv)

The current interpretation of that contract is:

- [docs/gold_schema_audit_from_databricks_csv.md](D:/@repos/instructor_test_project_repo/docs/gold_schema_audit_from_databricks_csv.md)

## Required Workflow Before Each New Gold Phase

Before implementing any new Gold phase:

1. Identify every Silver source table the phase will use.
2. Verify every referenced column exists in the Databricks export.
3. Record whether each field is:
   - source-native
   - Silver-derived
   - nullable placeholder
   - approved derivation required
4. Confirm whether any expected field names differ from spec wording.
5. Update the Gold schema registry before writing transformation code.

Do not start implementation for a phase until those checks are complete.

## Required Workflow After Any Bronze-to-Silver Change

After any Bronze-to-Silver change that affects schema or semantics:

1. Rerun the Bronze-to-Silver pipeline in Databricks.
2. Export the updated Bronze and Silver schema again.
3. Replace or supersede the schema CSV in `docs/`.
4. Update:
   - [docs/gold_schema_audit_from_databricks_csv.md](D:/@repos/instructor_test_project_repo/docs/gold_schema_audit_from_databricks_csv.md)
   - [docs/gold_source_contract.md](D:/@repos/instructor_test_project_repo/docs/gold_source_contract.md)
   - [docs/gold_discovery_report.md](D:/@repos/instructor_test_project_repo/docs/gold_discovery_report.md)
5. Recheck any Gold phase that depends on changed fields.

## Field Classification Standard

Every Silver field used by Gold should be classified explicitly.

### `source_native`

The field is carried directly from Bronze source semantics into Silver and can be treated as a stable business fact, subject to normal null handling.

Examples:

- `matches.match_date`
- `match_teams.team_number`
- `team_memberships.membership_start_date`

### `silver_derived`

The field is physically present in Silver but is computed during Bronze-to-Silver and should be treated as dependent on transform correctness.

Examples:

- `matches.winning_team_number`
- `matches.completed_flag`
- `match_teams.winner_flag`

### `nullable_placeholder`

The field exists physically in Silver but does not currently have trustworthy source population.

Examples as of July 23, 2026:

- `matches.competition_category`
- `matches.match_status`

### `approved_derivation_required`

The field is needed by Gold logic, but the source contract does not provide it directly and there is not yet an approved derivation.

Examples:

- match-level competition category, if needed for category-specific Gold products
- any future match-level status vocabulary

## Development Guardrails

### 1. No spec-only coding

Do not implement a Gold transform from the engineering spec alone. Always verify the deployed Bronze/Silver field names first.

### 2. No silent field substitution

Do not silently substitute one field for another because the names sound similar.

Examples of substitutions that require explicit review:

- `winning_team_id` versus `team_id`
- `team_number` versus `side_number`
- `team_category` versus `competition_category`
- `pre_match_team_rating` versus `average_team_rating_at_match`

### 3. Prefer deployed Silver names

If the spec and deployed schema differ, use the deployed schema in implementation and document the mismatch.

### 4. Fail early in harnesses

Validation harnesses should print or validate the exact source fields they rely on and fail clearly when a required field is missing.

### 5. Treat semantics separately from physical existence

A field existing in the table is not enough. Gold code should only trust fields whose semantics are confirmed.

## Gold Schema Registry Requirement

The target Gold schema should be documented as it is built out, not reconstructed after the fact.

Use:

- [docs/gold_target_schema_registry.md](D:/@repos/instructor_test_project_repo/docs/gold_target_schema_registry.md)

Update that document:

1. before implementing a new Gold table;
2. after finalizing the table columns;
3. whenever a Gold table contract changes.

## Minimum Phase Checklist

Before declaring a Gold phase ready:

- [ ] Source Silver tables are listed.
- [ ] Required source columns are verified in the Databricks export.
- [ ] Field classifications are recorded.
- [ ] Any derivation gaps are documented.
- [ ] Gold target table columns are added to the registry.
- [ ] Local tests cover the field assumptions that matter for the phase.
- [ ] Databricks validation confirms the phase against the deployed contract.

## Current Known High-Risk Areas

As of Thursday, July 23, 2026, these areas require explicit caution:

- `matches.competition_category`
- `matches.match_status`
- winner-side semantics sourced from Bronze `matches.winning_team_id`
- any Gold logic that assumes both `match_teams` sides have persistent `team_id`
- any Gold logic that assumes specification-only rating field names rather than deployed Silver field names

## Recommended Operating Habit

For every new Gold phase, start by answering this sentence in writing:

`This phase depends on these deployed Silver columns, and each one is trusted for these reasons.`

If that sentence cannot be written clearly, the phase is not ready to implement.
