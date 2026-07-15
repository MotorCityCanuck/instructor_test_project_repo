# NAPA Bronze to Silver Pipeline Construction Plan

**Purpose:** This document converts `docs/NAPA_Bronze_to_Silver_Spec.md` into a staged construction plan for building the Databricks Bronze-to-Silver pipeline. It is intended for instructor reference implementation planning and does not implement analytical Gold-layer decisions.

**Source specification:** `docs/NAPA_Bronze_to_Silver_Spec.md`  
**Target platform:** Databricks Free Edition, Unity Catalog, Delta Lake, PySpark  
**Pipeline mode:** Configuration-driven full refresh  
**Primary output:** Validated Silver Delta tables, rejects, operational metrics, and approved Silver convenience views

---

## 1. Planning Boundary

The Silver pipeline will standardize, validate, reconcile, and publish trusted business entities from Bronze Delta tables. It will not calculate player rankings, chemistry, fatigue, roster recommendations, model features, dashboards, explainability outputs, or other Gold-layer products.

The plan assumes the Bronze layer already contains one Delta table per delivered Parquet source file. The plan also assumes the existing repo is a scaffold and will need new configuration files, reusable PySpark modules, Databricks workflow notebooks, tests, and operational table definitions before the Silver implementation is complete.

---

## 2. Target Construction Sequence

Build the pipeline in staged increments so each stage leaves the repo runnable, reviewable, and testable.

| Stage | Focus | Primary Outcome |
|---|---|---|
| 0 | Repository alignment | Confirm naming, paths, branches, and implementation boundaries |
| 1 | Configuration model | YAML-driven release, source, target, domain, and rule configuration |
| 2 | Operations foundation | Pipeline run, table run, quality, reconciliation, schema snapshot, and message tables |
| 3 | Shared transformation framework | Reusable read, validation, standardization, reject, publish, and reconciliation functions |
| 4 | Reference Silver tables | `monthly_batches`, `regions` |
| 5 | Athlete Silver tables | `players`, `player_registrations`, `player_assessment_history` |
| 6 | Organization and partnership Silver tables | `clubs`, `teams`, `club_memberships`, `team_memberships` |
| 7 | Competition Silver tables | `matches`, `match_teams`, `match_team_players`, `match_games` |
| 8 | Cross-table validation and views | Relationship checks and approved operational views |
| 9 | Databricks pipeline workflow | One reusable Databricks pipeline/workflow with a parameterized task graph |
| 10 | Release acceptance | Validate `napa_5k`, `napa_50k`, and `napa_250k` runs |

---

## 3. Stage 0 - Repository Alignment

**Objective:** Establish the construction baseline before adding pipeline code.

**Tasks:**
- Confirm the current Git branch, remote, and working-tree status.
- Confirm whether the implementation is instructor-only, student-facing, or both.
- Preserve existing scaffold files unless replacing them is explicitly part of the pipeline build.
- Decide whether to keep the current `src/napa_pipeline` package name or introduce the spec's suggested `src/pipeline` layout.
- Decide the final namespace convention for release-specific schemas, such as `workspace.napa_5k_bronze` and `workspace.napa_5k_silver`.
- Identify current notebook dependencies, especially `_shared_pipeline_config.py`, and decide whether to keep or replace them with the YAML configuration loader.

**Exit criteria:**
- A short architecture decision note exists in the implementation branch or PR description.
- The chosen package, config, notebook, and schema naming conventions are documented.

---

## 4. Stage 1 - Configuration Model

**Objective:** Build the YAML configuration system required by the spec.

**Files to add or revise:**
- `config/base.yml`
- `config/environments/napa_5k.yml`
- `config/environments/napa_50k.yml`
- `config/environments/napa_250k.yml`
- `config/sources.yml`
- `config/silver_tables.yml`
- `config/domains.yml`
- `config/quality_rules.yml`
- `config/logging.yml`
- `src/napa_pipeline/config.py`
- `tests/test_config.py`

**Tasks:**
- Implement configuration loading in this order: `base.yml`, selected environment override, shared registries, rule files.
- Implement deep merge, placeholder resolution, supported-release validation, and configuration hash generation.
- Validate required keys, duplicate build-order values, undefined source references, unknown transform names, unsupported rule types, unresolved placeholders, and unsupported processing modes.
- Keep business rules and transformations shared across releases; release files should contain only release-specific paths, schemas, scale expectations, and performance settings.
- Include configuration entries for all thirteen Bronze sources and all thirteen Silver tables.

**Exit criteria:**
- `python -m pytest tests/test_config.py` passes.
- A resolved config can be produced for `napa_5k`, `napa_50k`, and `napa_250k`.
- The resolved config identifies catalog, schemas, sources, targets, build stages, rules, and publication settings without notebook hard-coding.

---

## 5. Stage 2 - Operations Foundation

**Objective:** Create the operational data model that records pipeline behavior and makes failures auditable.

**Operational tables:**
- `pipeline_runs`
- `table_runs`
- `quality_results`
- `reconciliation_results`
- `schema_snapshots`
- `run_messages`

**Tasks:**
- Define schemas for all operational tables in reusable code or SQL DDL.
- Implement run-start, run-complete, run-failed, table-start, table-complete, and table-failed writers.
- Implement schema snapshot capture for Bronze and Silver tables.
- Implement structured message logging with severity, table, rule, and run context.
- Ensure operational writes do not hide deterministic data failures.

**Exit criteria:**
- A pipeline run can be started and failed intentionally with records written to operations tables.
- Table-level and rule-level records include `pipeline_run_id`, release, table name, status, timestamps, and failure details.

---

## 6. Stage 3 - Shared Transformation Framework

**Objective:** Build reusable PySpark functions that every Silver table uses.

**Recommended modules:**
- `src/napa_pipeline/io.py`
- `src/napa_pipeline/metadata.py`
- `src/napa_pipeline/transforms.py`
- `src/napa_pipeline/validation.py`
- `src/napa_pipeline/quality.py`
- `src/napa_pipeline/reconciliation.py`
- `src/napa_pipeline/orchestration.py`

**Reusable capabilities:**
- Read configured Bronze tables by three-part name.
- Validate source contracts before transformation.
- Standardize column names to `snake_case`.
- Cast configured target types without schema inference.
- Normalize configured domains such as gender, country, and handedness.
- Standardize strings, numeric values, dates, timestamps, booleans, and null handling.
- Generate deterministic SHA-256 `_record_hash` values from configured business columns.
- Resolve exact duplicates and duplicate business keys according to configured policy.
- Split accepted and rejected records with reason codes and severity.
- Add standard metadata columns.
- Publish through staging and replace target Silver tables only after validation succeeds.
- Reconcile Bronze input, accepted Silver output, and reject counts.

**Exit criteria:**
- Reusable unit tests cover standardization, casting, domain normalization, hashing, duplicate ranking, reject creation, and reconciliation arithmetic.
- A synthetic in-memory table can pass through the framework without table-specific code.

---

## 7. Stage 4 - Reference Tables

**Objective:** Build the parent reference tables required by later Silver entities.

**Targets:**
- `monthly_batches`
- `regions`

**Tasks:**
- Implement table-specific mapping, type casting, deduplication, validation, reject handling, metadata, and publication.
- Confirm `monthly_batches` establishes release/batch context and supports downstream reconciliation.
- Confirm `regions` standardizes geographic reference values and rejects missing or unusable primary keys.

**Exit criteria:**
- Both tables publish to the Silver schema.
- Reject tables are written even when no records are rejected.
- Reconciliation and table-run records are written.

---

## 8. Stage 5 - Athlete Tables

**Objective:** Build standardized athlete-centered Silver entities.

**Targets:**
- `players`
- `player_registrations`
- `player_assessment_history`

**Tasks:**
- Build `players` before dependent athlete history tables.
- Normalize player identifiers, country, gender, handedness, dates, ratings, status, and profile fields according to configuration.
- Validate required player identity fields, duplicate player keys, invalid domains, invalid numeric ranges, and relevant date consistency.
- Validate child records against accepted Silver `players`.
- Preserve Gold boundary by avoiding ranking, future-potential decisions, roster logic, or model features.

**Exit criteria:**
- Athlete tables publish with standard metadata and deterministic hashes.
- Orphan registration and assessment records are rejected or flagged according to configured severity.
- Unit and integration tests cover valid records, missing keys, invalid domains, duplicate business keys, and invalid dates.

---

## 9. Stage 6 - Organization and Partnership Tables

**Objective:** Build cleaned organization, team, and membership entities.

**Targets:**
- `clubs`
- `teams`
- `club_memberships`
- `team_memberships`

**Tasks:**
- Build `clubs` and `teams` before membership tables.
- Validate club-region relationships after `regions` has published.
- Validate membership references to accepted Silver players, clubs, and teams.
- Standardize membership start/end dates and active/current indicators.
- Validate membership date ordering and active-member consistency according to configured rules.

**Exit criteria:**
- Organization and partnership tables publish with rejects, reconciliation, and operational metrics.
- Cross-table dependencies are enforced through configuration and workflow ordering.

---

## 10. Stage 7 - Competition Tables

**Objective:** Build match, side, player-participation, and game-level Silver entities.

**Targets:**
- `matches`
- `match_teams`
- `match_team_players`
- `match_games`

**Tasks:**
- Build `matches` before `match_teams`, `match_team_players`, and `match_games`.
- Validate match references to batch, region, and other configured parent entities.
- Validate each match has the configured number of sides.
- Validate each match side has the configured number of players.
- Validate participating players resolve to accepted Silver `players`.
- Validate game winners, score fields, margins, and match winner consistency.
- Keep analytical feature engineering and prediction logic out of Silver.

**Exit criteria:**
- Competition tables publish in dependency order.
- Cardinality, winner, score, and relationship validations are recorded in `quality_results`.
- Reconciliation records balance for each table.

---

## 11. Stage 8 - Cross-Table Validation and Views

**Objective:** Run validation rules that require completed Silver tables and publish approved convenience views.

**Cross-table validations:**
- Team membership composition.
- Match side cardinality.
- Player cardinality by match side.
- Player appears on only one side of a match.
- Game winner agrees with game scores.
- Match winner belongs to participating teams.
- Membership-at-match date validity.
- Region, club, player, team, match, and batch referential integrity.

**Views:**
- `vw_team_rosters`
- `vw_match_results`
- `vw_player_match_history`

**Exit criteria:**
- Cross-table validation writes rule results with severity, counts, and run context.
- Views are created only after required base Silver tables succeed.
- View definitions use explicit column lists.

---

## 12. Stage 9 - Databricks Pipeline Workflow

**Objective:** Wire the implementation into one reusable Databricks pipeline workflow rather than a manually executed notebook sequence.

The Silver implementation should be deployed as a Databricks job/workflow-style pipeline with explicit tasks, parameters, dependencies, retries, and run history. Notebooks remain thin task entry points, while reusable Python modules and YAML configuration hold the implementation logic.

**Workflow parameter:**
- `release_name`, with allowed values `napa_5k`, `napa_50k`, `napa_250k`

**Optional parameters:**
- `team_prefix`
- `config_root`
- `pipeline_version`

**Pipeline task graph:**
- `01_resolve_configuration`
- `02_validate_environment`
- `03_validate_bronze`
- `04_build_reference`
- `05_build_athlete`
- `06_build_organization_and_partnership`
- `07_build_competition`
- `08_run_cross_table_validation`
- `09_publish_convenience_views`
- `10_reconcile_and_publish_summary`

**Databricks construction tasks:**
- Create one reusable Databricks pipeline/workflow definition for Bronze-to-Silver.
- Configure `release_name` as the primary job parameter and pass it to all tasks that need resolved configuration.
- Use task dependencies to enforce the spec's build order instead of relying on manual notebook execution.
- Use task values, run parameters, or a persisted operations record to pass `pipeline_run_id`, resolved config location, and configuration hash between tasks.
- Assign deterministic task names that match the spec and operations records.
- Configure retries only for transient infrastructure failures, not deterministic quality or source-contract failures.
- Ensure the final summary task records success or failure status in operations tables.
- Store the pipeline/workflow definition in source control if the Databricks environment supports export as YAML, JSON, or another declarative format.

**Exit criteria:**
- One workflow can run all three releases by changing only `release_name`.
- Failed parent tasks prevent dependent tasks.
- The summary task records final state on successful runs and captures diagnostics on failed runs where workflow settings allow.
- The Databricks run page shows the complete Bronze-to-Silver task graph, task dependencies, parameters, retries, and run history.
- The repo contains enough workflow definition or runbook detail to recreate the Databricks pipeline in a clean workspace.

---

## 13. Stage 10 - Release Acceptance

**Objective:** Prove the pipeline works consistently across the three release sizes.

**5K acceptance:**
- All thirteen Bronze sources are detected.
- All configured Silver tables are created.
- Quality and reconciliation records are produced.
- Rerun produces identical business rows and `_record_hash` values.

**50K acceptance:**
- Only release configuration changes are required.
- Silver schemas and rule definitions remain consistent with 5K.
- Performance metrics are recorded.

**250K acceptance:**
- Only release configuration changes are required.
- Full-refresh execution completes.
- No driver-side collection of full datasets is used.
- All target tables and approved views query successfully.
- Operations tables contain complete execution evidence.

**Exit criteria:**
- Acceptance evidence is recorded in operations tables and summarized in run documentation.
- Any release-specific deviations are documented as configuration differences, not code forks.

---

## 14. Testing Plan

**Unit tests:**
- YAML loading and deep merge.
- Placeholder substitution.
- Configuration validation.
- String, numeric, date, timestamp, boolean, and null standardization.
- Domain normalization.
- Safe casting and invalid-cast handling.
- Deterministic hashing.
- Duplicate ranking and duplicate resolution.
- Quality rule result aggregation.
- Reconciliation arithmetic.

**Integration tests:**
- For each Silver builder, create temporary Bronze inputs with valid and invalid records.
- Verify accepted records, rejected records, reason codes, metadata columns, and deterministic rerun behavior.
- Cover missing primary keys, exact duplicates, conflicting duplicate keys, invalid domains, orphan foreign keys, invalid dates, and invalid numeric ranges where applicable.

**Cross-table tests:**
- Missing region reference.
- Missing player reference.
- Invalid team active-member count.
- Invalid match side count.
- Invalid match-side player count.
- Player on both sides of a match.
- Game winner conflicts with scores.
- Match winner conflicts with participating teams.
- Batch reconciliation failure.

---

## 15. Documentation Updates

Update documentation as implementation proceeds:

- `docs/architecture.md`: final pipeline structure and Databricks pipeline workflow design.
- `docs/medallion_design.md`: Raw, Bronze, Silver, Gold responsibilities and boundaries.
- `docs/data_dictionary.md`: final Silver table and column definitions.
- `docs/data_quality_rules.md`: implemented rule catalog and severity levels.
- `docs/lineage.md`: Bronze-to-Silver source-to-target lineage.
- `docs/runbook.md`: single-release and all-release execution steps.
- `docs/data_quality_report.md`: run evidence and quality findings.

---

## 16. Key Risks and Decisions

| Risk or Decision | Required Handling |
|---|---|
| Current repo is lighter than the spec layout | Add staged config and modules without broad unrelated reorganization |
| Existing notebook widget config may conflict with YAML config | Choose one runtime configuration authority for Silver implementation |
| Bronze schema may differ from representative fields in the spec | Validate physical Bronze schema before implementing table mappings |
| UUID Parquet compatibility may affect Bronze inputs | Prefer upstream UUID-as-string export and keep defensive Bronze handling documented |
| Full-refresh overwrite can destroy prior target state | Publish through staging and replace only after validation succeeds |
| Data-quality rules may be treated as business decisions | Keep thresholds and severities in approved configuration |
| Cross-release scaling may expose driver-side anti-patterns | Avoid pandas, full `collect()`, and driver-side full-table processing |

---

## 17. Immediate Next Implementation Steps

1. Create the Silver configuration file set and resolve the naming convention for release schemas.
2. Define the Databricks pipeline/workflow task graph and decide how its definition will be stored or recreated from source control.
3. Extend `src/napa_pipeline/config.py` into the full YAML loader and validator described by the spec.
4. Add operations table DDL or schema definitions and write run/table logging helpers.
5. Build the reusable table processing framework using synthetic tests before implementing specific tables.
6. Implement `monthly_batches` and `regions` as the first end-to-end Silver tables.
7. Use the reference-table implementation to confirm the framework before adding athlete, organization, partnership, and competition tables.
