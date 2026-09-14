# NAPA Olympic Team Selection Module
## Codex Implementation Specification

**Project:** DSB6000 NAPA Olympic Analytics Platform  
**Purpose:** Instructor/reference Databricks module for Olympic team selection  
**Target Platform:** Databricks Free Edition / Delta Lake / existing NAPA medallion architecture  
**Primary Consumer:** Codex, acting as the engineering implementation assistant  

---

## 1. Objective

Build a new Databricks module that selects Olympic doubles teams for the NAPA case study using **existing Gold-layer analytical data products only**.

The module is intended to mimic the team submissions produced by student consulting groups. It must select the highest-ranked valid NAPA teams for:

- United States — Men's Doubles
- United States — Women's Doubles
- United States — Mixed Doubles
- Canada — Men's Doubles
- Canada — Women's Doubles
- Canada — Mixed Doubles

The module must be configurable for the number of **team sets** to select.

A **team set** means one team in each of the six country/division combinations above. Therefore:

- `team_set_count = 1` produces 6 selected teams.
- `team_set_count = 4` produces 24 selected teams.
- In general, expected output row count = `team_set_count * 6`.

Each selected team must already exist in the NAPA data model. The selection module must **never create, recombine, synthesize, or infer a new doubles pairing from individual players**.

---

## 2. Architectural Role

This module belongs **after Gold-layer analytics have been built**.

Its responsibility is:

> **Gold analytical products -> eligibility validation -> ranked team selection -> Gold selection output**

The module is **not** responsible for rebuilding Bronze or Silver data, recomputing player history from raw match data, or generating new partnership combinations.

Codex must inspect and reuse the Gold table definitions and Gold scoring logic already present in the repository. Do not invent replacement schemas, scoring formulas, or table names when equivalent governed objects already exist.

### Required architectural principles

1. Use only existing Gold-layer data products as analytical inputs.
2. Reuse the repository's existing catalog/schema configuration patterns.
3. Do not hard-code dataset names, catalogs, schemas, table paths, or workspace-specific locations.
4. Support all three NAPA dataset scales through configuration/parameters only.
5. Persist the final result as a managed Gold Delta table.
6. Make execution repeatable and deterministic for a fixed source dataset and Gold state.
7. Keep selection logic separate from Gold feature engineering/scoring logic.

---

## 3. Functional Requirements

### 3.1 Required selections

For every execution, select teams for exactly these six groups:

| Country | Division |
|---|---|
| USA | Men's Doubles |
| USA | Women's Doubles |
| USA | Mixed Doubles |
| Canada | Men's Doubles |
| Canada | Women's Doubles |
| Canada | Mixed Doubles |

For each group, select exactly `team_set_count` teams.

If `team_set_count = 4`, the module must return:

- USA Men's ranks 1-4
- USA Women's ranks 1-4
- USA Mixed ranks 1-4
- Canada Men's ranks 1-4
- Canada Women's ranks 1-4
- Canada Mixed ranks 1-4

The rank within each country/division becomes the **selection set number**.

Example:

- Selection Set 1 = highest-ranked team in all six groups.
- Selection Set 2 = second-highest-ranked team in all six groups.
- Selection Set 3 = third-highest-ranked team in all six groups.
- Selection Set 4 = fourth-highest-ranked team in all six groups.

This structure is intended to emulate multiple complete student-group submissions.

---

## 4. Team Identity and No-Ad-Hoc-Team Rule

### 4.1 Existing teams only

Every selected team must be an existing team already represented in the governed NAPA data.

The implementation must not:

- pair high-ranked players together to create a new team;
- substitute one player into an existing team;
- create a synthetic team identifier;
- generate a new Men's, Women's, or Mixed pairing;
- infer a partnership that is absent from the existing team records.

### 4.2 Team number

Selections are to be identified by **team number**.

Codex must inspect the repository's current Gold and underlying team definitions to identify the canonical field that represents the team number/identifier used by the NAPA implementation.

Rules:

1. Use the existing canonical identifier.
2. Do not invent a new numbering system.
3. If the repository uses a field such as `team_id` internally and that field is the canonical tournament/team number, preserve it and expose it in the final output under a clear field name such as `team_number` only if the repository's semantics support that alias.
4. Maintain traceability back to the source Gold record.

---

## 5. Ranking and Selection Logic

### 5.1 Core rule

For each country/division group, select the **highest-ranked eligible teams** from the existing Gold analytical products.

### 5.2 Required bias considerations

The ranking used for selection must reflect the NAPA analytical treatment of:

- regional bias / regional competitive strength;
- age-related bias or adjustment;
- fatigue-related bias or adjustment.

Codex must inspect the existing repository to determine how these effects are represented in the Gold layer.

Preferred order of use:

1. **Use an existing final Gold selection rank or adjusted selection score** if the repository already provides one that incorporates regional, age, and fatigue considerations.
2. If the Gold layer exposes the required components but not a final adjusted rank, use the **existing documented Gold scoring logic and weights from the repository** to calculate the final ordering within this module.
3. **Do not invent new weights, formulas, thresholds, or bias corrections.**
4. If the repository does not contain sufficient Gold-layer information or documented scoring logic to account for regional, age, and fatigue effects, fail with a clear implementation/configuration error rather than silently making assumptions.

### 5.3 Selection order

Within each country/division:

1. Filter to valid eligible teams.
2. Apply the repository-defined Gold selection/ranking methodology.
3. Sort from strongest/highest-ranked to weakest/lower-ranked.
4. Assign `selection_rank` beginning at 1.
5. Retain rows where `selection_rank <= team_set_count`.
6. Set `selection_set_number = selection_rank`.

### 5.4 Tie handling

The output must be deterministic.

If two or more teams have identical final selection scores/ranks, use deterministic tie-breaking fields already available in the Gold data or repository scoring logic.

If no repository tie-break rule exists, use a stable fallback in this order where available:

1. higher Gold confidence measure;
2. greater supporting match evidence / match count;
3. canonical team number ascending.

Document any fallback tie-break rule in code comments and the module README/documentation.

---

## 6. Eligibility Rules

Codex must reuse eligibility/status definitions already present in the Gold products and repository.

At minimum, a selected team must:

- exist as a valid NAPA team;
- belong to the requested country;
- belong to the requested Men's, Women's, or Mixed division/category;
- satisfy the repository's active/eligible team status rules;
- have a valid canonical team number;
- contain valid membership according to the governed data used to create the Gold product.

Do not independently recreate lower-layer membership validation unless necessary to confirm a Gold-record contract already defined in the repository.

### Player overlap

Do **not** add new constraints that prohibit a player from appearing on multiple pre-existing teams unless such a restriction already exists in the NAPA repository's selection rules.

This module ranks and selects valid stored teams. It does not optimize a player roster by recombining athletes or enforcing newly invented cross-team exclusivity rules.

---

## 7. Databricks Parameters

The Databricks job/notebook entry point must expose at least these two parameters.

### 7.1 `dataset_source`

Required.

Supported logical values:

- `napa_5k`
- `napa_50k`
- `napa_250k`

If the repository already uses a different canonical parameter name such as `release_name`, reuse the existing naming convention rather than creating duplicate configuration concepts.

The parameter must determine which NAPA dataset/release configuration is active without requiring code changes.

### 7.2 `team_set_count`

Required positive integer.

Examples:

- `1` -> 6 output rows
- `4` -> 24 output rows
- `10` -> 60 output rows

Validation:

- must be an integer;
- must be >= 1;
- must not exceed the number of eligible ranked teams available in any required country/division group.

### 7.3 Parameter implementation

Use the repository's existing Databricks parameter/configuration convention.

Acceptable patterns include:

- Databricks notebook widgets;
- Databricks job parameters;
- existing YAML configuration plus job parameter overrides;
- existing Python argument/config helpers in the repo.

Do not introduce an incompatible configuration framework if the repository already has one.

---

## 8. Gold Input Contract

Codex must **discover the Gold input contract from the existing repository**.

Do not assume that example names in assignment documentation are the exact implemented names.

Search the repository for Gold tables, SQL definitions, notebooks, configuration, data dictionaries, and scoring modules that represent concepts such as:

- team selection scorecard;
- team rankings;
- country/division rankings;
- tournament candidates;
- adjusted selection score;
- regional adjustment;
- age adjustment;
- fatigue adjustment;
- confidence / risk / evidence measures.

### Required Gold input capabilities

The chosen Gold input must provide, directly or through existing Gold-layer joins:

- canonical team number/identifier;
- country;
- division/category;
- final ranking score or sufficient Gold components to derive it using repository-defined logic;
- regional bias/adjustment information;
- age bias/adjustment information;
- fatigue bias/adjustment information;
- eligibility/status information;
- supporting confidence/evidence fields where available.

If multiple Gold products are needed, joins must remain entirely within Gold unless the repository explicitly defines a governed Gold view that resolves lower-layer lineage for the module.

---

## 9. Gold Output Table

Create a new managed Gold Delta table for the final selections.

### 9.1 Table naming

Use the repository's existing Gold naming conventions.

Preferred logical name if no equivalent already exists:

`olympic_team_selections`

Example fully qualified name only if consistent with existing repository configuration:

`<catalog>.<gold_schema>.olympic_team_selections`

Do not hard-code the catalog or Gold schema.

### 9.2 Output grain

**One row per selected country/division/team.**

For `team_set_count = N`, output must contain exactly `N * 6` rows.

### 9.3 Required columns

Use repository naming conventions, but the output must contain equivalent information to the following:

| Column | Purpose |
|---|---|
| `dataset_source` | Active NAPA release, such as `napa_5k`, `napa_50k`, or `napa_250k` |
| `selection_run_id` | Unique identifier for the module execution |
| `selection_timestamp` | UTC timestamp when the output was produced |
| `selection_set_number` | 1..N; corresponds to rank within country/division |
| `selection_rank` | Rank within the country/division; normally same as set number |
| `country_code` | USA/US or CAN/CA according to repository convention |
| `division` | Men's, Women's, or Mixed according to repository convention |
| `team_number` | Canonical NAPA team number/identifier |
| `source_gold_rank` | Gold ranking value/rank used for selection, when available |
| `selection_score` | Final Gold score used for ordering |
| `regional_adjustment` | Gold regional bias/strength contribution, when available |
| `age_adjustment` | Gold age-related contribution, when available |
| `fatigue_adjustment` | Gold fatigue-related contribution, when available |
| `confidence_metric` | Gold confidence/risk measure, when available |
| `supporting_match_count` | Supporting evidence count, when available |
| `selection_reason` | Concise machine-generated explanation based on the Gold fields used |

Additional lineage/audit fields are encouraged where already supported by the repository.

### 9.4 Selection reason

Generate a concise deterministic explanation from the Gold data, for example:

> Ranked #1 among USA Mixed teams after applying the repository's Gold selection methodology, including regional, age, and fatigue adjustments.

Do not use an LLM call at runtime to generate selection reasons.

---

## 10. Write Behavior and Idempotency

Use Delta table write behavior consistent with the existing repository.

The result must be reproducible and auditable.

Recommended behavior:

1. Generate a unique `selection_run_id` per execution.
2. Preserve `dataset_source` and execution timestamp.
3. Either:
   - append each run and provide a current/latest view, or
   - overwrite only the active dataset partition/run according to the repository's established Gold pattern.
4. Do not silently mix 5K, 50K, and 250K selection results without source metadata.

Prefer the repository's existing Gold-history strategy if one exists.

---

## 11. Validation and Failure Behavior

The module must fail loudly and clearly when required conditions are not satisfied.

### 11.1 Required pre-selection validations

Validate that:

- `dataset_source` is recognized;
- `team_set_count` is a positive integer;
- required Gold input tables/views exist;
- required ranking/adjustment fields or repository scoring logic are available;
- all six required country/division groups are present;
- canonical team numbers are not null for eligible candidates;
- the available eligible count in every group is >= `team_set_count`.

### 11.2 Insufficient-team error

If any group contains fewer eligible teams than requested, stop the run.

The error must identify:

- dataset source;
- country;
- division;
- requested team count;
- available eligible team count.

Example:

`Selection failed: napa_5k / Canada / Mixed requested 4 teams but only 3 eligible ranked teams are available.`

This condition is not expected in normal NAPA data, but the module must still validate it.

### 11.3 Post-selection validations

After selection, verify:

- exact row count = `team_set_count * 6`;
- exactly `team_set_count` rows exist for each country/division;
- each group contains selection ranks 1..N with no gaps;
- no duplicate team number exists within the same country/division selection result;
- all selected records have valid non-null team numbers;
- all selected teams are traceable to the Gold input;
- output divisions and countries contain only the expected six combinations.

Raise an exception if any validation fails.

---

## 12. Logging and Execution Summary

At the end of a successful run, log a concise summary that includes:

- dataset source;
- team set count;
- Gold source table(s)/view(s) used;
- output table;
- total selected row count;
- count by country/division;
- highest and lowest selected score/rank by group;
- selection run ID.

Also display a compact result ordered by:

1. `selection_set_number`
2. country
3. division

This should make the four-set case easy to inspect as four complete simulated student submissions.

---

## 13. Suggested Module Structure

Codex must first inspect the repository and follow its existing organization. Do not restructure the repository unnecessarily.

If no equivalent structure exists, a reasonable implementation is:

```text
src/
  <existing_package>/
    olympic_team_selection.py

notebooks/
  <next_sequence>_select_olympic_teams.py

tests/
  test_olympic_team_selection.py
```

### Suggested responsibilities

#### `olympic_team_selection.py`

Reusable functions for:

- loading/configuring Gold inputs;
- validating input contract;
- applying existing Gold ranking/scoring rules;
- ranking teams by country/division;
- selecting top N teams;
- generating deterministic selection rationale;
- validating output;
- writing the Gold Delta table.

#### Databricks notebook/job entry point

Responsible for:

- reading job parameters;
- loading repository configuration;
- invoking the reusable selection module;
- showing the execution summary;
- surfacing failures clearly in Databricks Jobs.

Keep business logic out of notebook cells where practical.

---

## 14. Testing Requirements

Create automated tests consistent with the repository's current test approach.

At minimum include tests for:

### Parameter tests

- valid `dataset_source` values;
- invalid dataset source;
- `team_set_count = 1`;
- `team_set_count = 4`;
- zero team count;
- negative team count;
- non-integer team count.

### Ranking tests

- highest score/rank is selected first;
- regional/age/fatigue-adjusted ranking is respected;
- deterministic tie handling;
- country filters are respected;
- division filters are respected.

### Integrity tests

- output uses existing team numbers only;
- no synthetic/ad-hoc team is produced;
- expected six country/division groups are present;
- exactly N rows per group;
- exactly `6 * N` rows overall;
- duplicate team number within a country/division is rejected or eliminated according to the governed Gold input contract;
- missing team number causes validation failure.

### Failure tests

- missing Gold table/view;
- missing required bias-adjusted score/component;
- insufficient eligible teams in any group;
- unexpected/missing country/division category;
- malformed Gold input.

---

## 15. Acceptance Criteria

The implementation is complete when all of the following are true:

1. The module runs in Databricks using the existing NAPA repository and configuration framework.
2. The user can specify the NAPA source dataset using a Databricks parameter.
3. The user can specify the desired number of team sets using a Databricks parameter.
4. No code changes are required to move between 5K, 50K, and 250K datasets.
5. For `team_set_count = 4`, exactly 24 teams are selected.
6. The 24 rows represent 4 ranked teams for each of the six country/division combinations.
7. Every selected team is an existing governed NAPA team.
8. No players are recombined into new pairings.
9. Selection uses the repository's highest-ranked Gold teams after regional, age, and fatigue bias considerations.
10. No new ranking weights or bias formulas are invented by Codex.
11. The result is persisted as a managed Gold Delta table.
12. Output includes selection set/rank, country, division, team number, score/rank evidence, relevant bias-adjustment fields, and run metadata.
13. The module validates that enough eligible teams exist before writing output.
14. The module fails with a clear error rather than returning incomplete selections.
15. Automated tests cover parameters, ranking, validity, row counts, and failure conditions.
16. Code follows existing repository patterns and does not duplicate functionality already present elsewhere in the repo.

---

## 16. Codex Implementation Instructions

Before writing code, Codex must perform repository discovery.

### Step 1 — Inspect existing architecture

Identify:

- repository package/module structure;
- Databricks notebook/job patterns;
- configuration loaders;
- dataset source/release parameters;
- catalog and schema configuration;
- existing Gold table definitions;
- existing scoring/ranking logic;
- existing data quality/validation helpers;
- current unit/integration test conventions.

### Step 2 — Identify the canonical Gold selection source

Locate the Gold product or combination of Gold products that contain the team-level ranking used for Olympic/tournament selection.

Confirm that the selected Gold source includes or supports:

- team number;
- country;
- division;
- final rank/score;
- regional adjustment;
- age adjustment;
- fatigue adjustment;
- eligibility/status.

### Step 3 — Confirm scoring ownership

Determine whether regional, age, and fatigue effects are already embedded in a final Gold score/rank.

- If yes, use that score/rank directly.
- If no, find the repository's documented Gold scoring logic and reuse it.
- If the required logic does not exist, stop and report the missing design dependency. Do not invent a formula.

### Step 4 — Implement selection logic

Implement the smallest reusable module that:

- loads governed Gold inputs;
- validates them;
- ranks eligible teams within country/division;
- selects top N;
- assigns selection set number;
- writes the Gold output;
- validates final counts/integrity.

### Step 5 — Add Databricks entry point

Expose the required parameters:

- dataset source/release;
- team set count.

Reuse current repository conventions for parameter names and configuration resolution.

### Step 6 — Add tests

Implement tests before considering the feature complete.

### Step 7 — Document the feature

Update the appropriate repository documentation with:

- purpose;
- parameters;
- Gold input(s);
- Gold output table;
- selection logic;
- failure conditions;
- sample invocation;
- sample expected output shape.

---

## 17. Example Invocation

The exact command must follow the repository's existing Databricks job pattern.

Logical example:

```text
--dataset-source napa_250k
--team-set-count 4
```

If the repository already uses `release_name`, use:

```text
--release-name napa_250k
--team-set-count 4
```

Expected result:

```text
24 selected rows
= 4 team sets
x 6 country/division combinations
```

---

## 18. Example Logical Output

For `team_set_count = 4`:

| Set | Country | Division | Selected Team |
|---:|---|---|---|
| 1 | USA | Men's | highest-ranked eligible USA Men's team |
| 1 | USA | Women's | highest-ranked eligible USA Women's team |
| 1 | USA | Mixed | highest-ranked eligible USA Mixed team |
| 1 | Canada | Men's | highest-ranked eligible Canada Men's team |
| 1 | Canada | Women's | highest-ranked eligible Canada Women's team |
| 1 | Canada | Mixed | highest-ranked eligible Canada Mixed team |
| 2 | USA | Men's | second-ranked eligible USA Men's team |
| ... | ... | ... | ... |
| 4 | Canada | Mixed | fourth-ranked eligible Canada Mixed team |

The actual output must contain the canonical team numbers and supporting Gold analytical evidence.

---

## 19. Non-Goals

This module must **not**:

- build Bronze or Silver tables;
- generate new players or teams;
- construct ad-hoc pairings;
- optimize pairings by recombining individuals;
- create new regional, age, or fatigue formulas;
- independently rescore raw matches when an approved Gold score already exists;
- run the Monte Carlo tournament simulation;
- choose tournament winners;
- modify the synthetic data generator;
- replace existing Gold products that already perform ranking/scoring.

Its role is intentionally narrow: **convert governed Gold team rankings into configurable, reproducible Olympic team-set selections that mimic student submissions.**

---

## 20. Final Design Principle

Keep the module simple and auditable.

The analytical intelligence should remain in the Gold-layer data products. This module should make the final selection process transparent:

> **Use the existing Gold ranking, validate eligibility, take the top N teams for each required country/division, assign them to selection sets, and persist the result.**

