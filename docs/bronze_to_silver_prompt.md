# Student AI Build Prompt: NAPA Bronze-to-Silver Pipeline

**Purpose:** Give an AI coding assistant this complete construction contract to build and test a configuration-driven NAPA Bronze-to-Silver pipeline. It specifies the student pipeline architecture, data contracts, quality rules, Databricks bundle, and acceptance evidence. Students remain responsible for reviewing the generated implementation, explaining decisions, and executing their own validation. No completed pipeline or unpublished repository is required.

**Dataset basis:** NAPA student dataset schema 1.6. The source-field contract, relationships, rules, and required outputs needed for construction are included below. The selected release's actual Parquet schema and colocated manifest are the authoritative delivered-data evidence. No separate publication or private implementation artifact is needed to execute this construction contract.

**Important distinction:** The requirements below define the student construction contract. Follow its specified decisions; if a genuine ambiguity remains, document it and request team direction before implementing that affected behavior. Keep choices deterministic and tested. Do not silently alter accepted data, date semantics, publication, or failure policy.

---

## Begin prompt to copy into your AI assistant

### Task, boundaries, and working agreement

Build a reusable, configuration-driven **Bronze-to-Silver** pipeline for the NAPA case study. Implement the functional contract specified here, write meaningful tests, and package the workflow as a deployable Databricks Asset Bundle. Produce traceable validation and documented dispositions for the quality requirements included in this prompt. Do not seek or copy an existing completed implementation.

Work in stages. At each stage, explain the files changed, assumptions, tests executed, and remaining issues. Inspect before editing. Preserve existing work. Do not introduce a new framework merely for preference. Do not claim that local tests prove a Spark SQL job or bundle has run successfully.

Include typed and standardized entities, deterministic key handling, duplicate-key rejection, parent-key validation, reject tables, quality evidence, reconciliation, operational auditing, convenience views, and full-refresh execution. Read the selected profile's freshly refreshed Bronze managed Delta tables; do not rebuild Bronze or modify Raw files as part of Silver. Each run is a full refresh for the selected profile: after successful configuration, environment, and Bronze-source preflight validation, remove only the specified Silver tables, Silver-reject tables, and convenience views before rebuilding them. Do not append prior business rows. If a technical failure interrupts the run, preserve all available outputs and evidence until a subsequent run completes its preflight checks and performs the next targeted reset. Existing Raw files and their colocated manifest remain available for release-quality evidence when needed.

Exclude Gold feature engineering, model selection/training, chemistry scores, confidence models, roster selection, business conclusions, hidden simulation parameters, streaming, Auto Loader, CDC, and incremental upserts into Silver. Do not invent athletes, teams, source columns, measurements, numeric acceptance thresholds, or successful run results. Existing `ad_hoc` teams are real source entities, not permission to manufacture new teams.

Before running writes, confirm the team's catalog, schema prefix, release, workspace, and permissions. Use only the team's namespace. Do not commit datasets, Delta storage, credentials, tokens, workspace exports, or personal configuration.

### Stage 0 — Inspect, establish contracts, and record decisions

1. Read repository instructions, `README.md`, and relevant documentation, requirements, configuration, source modules, notebooks, and tests that are actually present. Consult the Raw-to-Bronze prompt if supplied; the necessary Silver input contracts are also stated here. Run `git status --short`, `git branch --show-current`, and `git remote -v`; report unrelated changes and preserve them. A clean student template may not yet contain pipeline code.
2. Inspect the student repository structure and identify the source, transformation, workflow, and test entry points to create. Implement this contract without assuming an existing finished pipeline, private helper library, or unpublished test suite.
3. Read the actual release manifest and inspect the physical Bronze schemas and their Raw schema evidence. Use Appendix A for the published source-field contract. Do not confuse schema 1.6's `team_type` identity with `team_division`, or `matches.winning_team_id` with `match_teams.id`.
4. Produce a source-to-target contract and quality coverage matrix. For each rule record its section or source field, table and columns, implementation location, severity, disposition, and test. Explicitly distinguish original source defects from orphan records caused by rejection of an upstream parent.
5. Review the implementation-risk register with the team. Follow the requirements already decided in this prompt; identify genuine remaining ambiguities before changing affected behavior, and continue unrelated work while they are resolved. Keep any approved decisions in a project log.

Deliverable: an implementation plan, contract matrix, configuration inventory, and test plan, with no invented source data or analytical decisions.

### Stage 1 — Configuration and release isolation

Use one implementation for these five independent, complete historical dataset profiles. They are dataset profiles, not successive monthly increments:

| Dataset profile / release name | Role | Data condition | Expected player scale | Final decision dataset | Schema suffix |
|---|---|---|---:|---|---|
| `napa_5k_clean` | development | clean | 5,000 | false | `5k_clean` |
| `napa_5k_tainted` | development | tainted | 5,000 | false | `5k_tainted` |
| `napa_50k_clean` | validation | clean | 50,000 | false | `50k_clean` |
| `napa_50k_tainted` | validation | tainted | 50,000 | false | `50k_tainted` |
| `napa_250k_production` | production-scale case data | production | 250,000 | true | `250k` |

Use these profiles for development, Silver validation, and production-scale analysis respectively. Use configuration changes, not forks of transformation code. These scale values are labels/expectations, not a reason to invent exact row-count assertions for every file. The colocated manifest supplies actual counts. Do not permit a clean or tainted selector for the 250K production dataset.

Use team-owned configuration for the catalog, prefix, pipeline name, version, project, and `full_refresh` processing mode. Externalize these values. Resolve fully qualified tables as `<catalog>.<schema>.<table>`:

- Bronze: `<team_prefix>_<scale>_<condition>_bronze` for 5K/50K clean and tainted profiles; `<team_prefix>_250k_bronze` for 250K production.
- Silver: `<team_prefix>_<scale>_<condition>_silver` for 5K/50K clean and tainted profiles; `<team_prefix>_250k_silver` for 250K production.
- Rejects: `<team_prefix>_<scale>_<condition>_silver_reject` for 5K/50K clean and tainted profiles; `<team_prefix>_250k_silver_reject` for 250K production.
- Shared operations: `<team_prefix>_ops`.

Use the following configuration layout, or document an equivalent:

```text
config/bronze_to_silver/
  base.yml
  environments/napa_5k_clean.yml
  environments/napa_5k_tainted.yml
  environments/napa_50k_clean.yml
  environments/napa_50k_tainted.yml
  environments/napa_250k_production.yml
  sources.yml
  silver_tables.yml
  domains.yml
  quality_rules.yml
  logging.yml
  workflows/napa_bronze_to_silver.job.yml
src/<project_package>/bronze_to_silver/
notebooks/11_b2s_resolve_configuration.py ... 20_b2s_finalize_pipeline_summary.py
tests/
```

Load and recursively merge, in order: base, selected environment, sources, Silver tables, domains, quality rules, logging. Resolve `${path.to.value}` placeholders, including nested references. Validate required sections, supported dataset profiles (`napa_5k_clean`, `napa_5k_tainted`, `napa_50k_clean`, `napa_50k_tainted`, `napa_250k_production`), matching `release.dataset_profile`/`release.release_name`, valid `release.data_condition`, `full_refresh`, source table/file/natural-key definitions, unique integer build orders, valid stages/transforms, nonempty primary keys, and rule references. Reject clean/tainted conditions for 250K and production for 5K/50K. Hash the resolved configuration using SHA-256 of UTF-8 `yaml.safe_dump(..., sort_keys=True)` for reproducible run identification. Test invalid placeholders and missing files clearly.

Do not place a fixed snapshot date, fixed included-batch list, or generator-specific release boundary in configuration. Resolve `snapshot_month`, `snapshot_end_exclusive`, and included batch metadata from the validated manifest for the selected profile, then publish the derived temporal context to downstream tasks.

Preserve these configuration values and distinguish declarative settings from actual runtime support:

| Section | Required configuration |
|---|---|
| runtime | `catalog`, `team_prefix`, `default_release: napa_5k_clean`, `timezone: America/New_York` |
| execution | `fail_fast: true`, `publish_only_after_validation: true`, `stop_dependents_on_failure: true`, `unexpected_column_policy: warn`, `missing_required_column_policy: fail`, `duplicate_primary_key_policy: reject_all`, `create_convenience_views: true`, `reset_selected_targets_before_build: true`, `cleanup_silver_on_failure: false` |
| publication | `format: delta`, `mode: overwrite`, `overwrite_schema: true`, `use_staging: false`; publish directly with full-refresh table replacement into designated Silver and Silver-reject schemas; Bronze schema reset is performed upstream by Raw-to-Bronze |
| metadata | seven `add_*` settings for run ID, version, source dataset, source table, load timestamp, record hash, quality status; all true |
| thresholds | `close_game_margin: 2`, `score_share_tolerance: 0.0001`, `warning_quality_deduction: 5`, `expected_match_team_count: 2`, `expected_match_team_player_count: 2`, `duplicate_active_team_pair_severity: WARNING` |
| age groups | `UNDER_18` 0–17; `AGE_18_34` 18–34; `AGE_35_49` 35–49; `AGE_50_64` 50–64; `AGE_65_PLUS` 65+ |
| performance | shuffle partitions 16/64/`auto`; broadcast row thresholds 100000/250000/500000 for 5k/50k/250k |
| paths | `workflow_script_root: notebooks` |

Do not infer that a setting is enforced merely because it exists. In particular, SQL publication is direct replacement, not staged publication; SQL metadata is always appended; the YAML quality registry is not a generic rule-execution engine; performance/timezone settings do not by themselves demonstrate session configuration. `warning_quality_deduction` does not implement a Silver quality score.

Declare a CRITICAL `not_null` primary-key rule for each target, referring to source `id` except `players`, which uses `player_id`. Configuration validation may allow `not_null`, `unique`, `allowed_values`, `foreign_key`, and `date_not_after` rule types, but arbitrary configuration entries must not be treated as executed checks. Keep rule declarations, executed checks, and audit rule IDs traceable rather than treating configuration as a complete quality specification.

Normalize configured string domains after trimming and uppercasing:

| Domain | Accepted standardized values and aliases |
|---|---|
| country | `USA`, `CAN`; `US`, `UNITED STATES`, `UNITED STATES OF AMERICA` → `USA`; `CA`, `CANADA` → `CAN` |
| gender | `M`, `F`; `MALE` → `M`, `FEMALE` → `F` |
| dominant hand | `LEFT`, `RIGHT`, `AMBIDEXTROUS`; `L` → `LEFT`, `R` → `RIGHT`, `BOTH` → `AMBIDEXTROUS` |
| player status | configured `ACTIVE`, `INACTIVE`; the derived active flag also recognizes the boolean-like strings specified below |
| team status | `ACTIVE`, `INACTIVE`, `DISSOLVED`; `DORMANT` → `INACTIVE`, `RETIRED` → `DISSOLVED` |
| team identity | `COMPETITIVE`, `AD_HOC`; `AD HOC` → `AD_HOC` |
| team division/category | `MENS`, `WOMENS`, `MIXED`, `OPEN`; corresponding `*_DOUBLES` values map to these categories |
| player position | `LEFT`, `RIGHT`; string `1` → `LEFT`, string `2` → `RIGHT` |

Keep legacy division-valued `team_type` support separate from the schema 1.6 identity interpretation. Do not change Bronze country values or physical types when standardizing Silver.

### Stage 2 — Environment, run context, and operations tables

Validate catalog access and resolve the selected profile's Bronze, Silver, Silver-reject, and operations schemas. The selected profile's Bronze schema is created and refreshed by Raw-to-Bronze; Silver must validate that it is reading the expected fully qualified Bronze schema and must not drop, recreate, or repopulate it. Create designated Silver and Silver-reject schemas with `CREATE SCHEMA IF NOT EXISTS` when necessary and authorized. Never drop entire schemas, delete unexpected/unrelated objects, or clean up published outputs in response to failure. The **only** permitted automatic output deletion is the explicit, profile-scoped pre-build reset specified in Stage 3. The operations schema and its audit evidence remain available. Do not create catalogs, grant permissions, upload files, or create a Raw Volume as part of this pipeline. Creation of an empty Bronze schema does not satisfy the source-table prerequisites.

Create a UUID pipeline run ID once and pass it to every task. Carry pipeline name/version, release, mode, configuration hash, and operations schema in the run context. Use UTC-aware operational timestamps, controlled clocks in tests, and structured messages that do not expose secrets or full sensitive records.

Carry validated release temporal context in the run context: `snapshot_month`, `snapshot_end_exclusive`, and `snapshot_as_of_date`, where:

```text
snapshot_as_of_date = DATE_SUB(TO_DATE(snapshot_end_exclusive), 1)
```

Use three temporal reference categories consistently. The snapshot date represents the end of the selected release and is used for current-state calculations such as player age, age group, current registration/membership status, and team age. The historical event date is the actual date of a match, game, assessment, or other event and is used for event-time age, membership validity, participation, and date comparisons. The batch date identifies a historical batch period and is used for rating/assessment update periods, batch context, and included-period validation. Do not substitute one category for another without documenting the requirement.

Create six managed Delta operations tables with explicit schemas. Preserve these column contracts; use STRING for identifiers/labels unless noted, BIGINT for counts, TIMESTAMP for timestamps, DOUBLE for duration/failure percentage, INT for build order/ordinal, BOOLEAN for nullability, and ARRAY<STRING> for sampled keys:

```text
b2s_pipeline_runs:
  pipeline_run_id, pipeline_name, pipeline_version, release_name,
  processing_mode, configuration_hash, workflow_run_id, status,
  started_ts, completed_ts, duration_seconds, triggered_by,
  error_class, error_message
b2s_table_runs:
  pipeline_run_id, release_name, source_table, target_table, build_stage,
  build_order, status, started_ts, completed_ts, duration_seconds,
  source_row_count, exact_duplicate_count, duplicate_key_rejected_count,
  accepted_row_count, rejected_row_count, warning_count,
  published_row_count, error_message
b2s_quality_results:
  pipeline_run_id, release_name, target_table, rule_id, rule_type, severity,
  evaluated_row_count, failed_row_count, failure_pct, threshold_value,
  status, sample_business_keys, evaluated_ts
b2s_reconciliation_results:
  pipeline_run_id, release_name, source_table, target_table,
  bronze_row_count, exact_duplicate_count, duplicate_key_rejected_count,
  rejected_row_count, accepted_row_count, reconciliation_difference,
  status, evaluated_ts
b2s_schema_snapshots:
  pipeline_run_id, release_name, layer_name, table_name, column_name,
  data_type, nullable, ordinal_position, schema_hash, captured_ts
b2s_run_messages:
  pipeline_run_id, release_name, target_table, message_level,
  message_code, message_text, created_ts
```

Define exact DDL/nullability for the operations tables in the project and keep it explicit. Required context keys/status/timestamps must not be silently null. Normalize appended records to the operations table schema: fill absent nullable fields with null, discard extra record fields, reject absent required values, and make empty appends a no-op. Do not rely on inference from empty/all-null records.

Initialize the operations tables and append a `VALIDATING` run record only if the run ID is not already recorded. Table execution appends a `RUNNING` record followed by a `SUCCEEDED` or `FAILED` terminal record; these are separate audit rows. Publish schema snapshots with 1-based column ordinals and SHA-256 of the ordered `name:type:nullable` strings joined with `|`. This operational schema hash is not automatically the manifest's schema-hash algorithm.

Preserve message codes `CONFIG_LOADED`, `ENVIRONMENT_VALIDATED`, `BRONZE_CONTRACT_VALIDATED`, `SILVER_BUILD_STARTED`, `SILVER_BUILD_COMPLETED`, `QUALITY_RULE_FAILED`, `RECONCILIATION_COMPLETED`, `PIPELINE_SUCCEEDED`, and `PIPELINE_FAILED`. Only the pipeline-runs table has a `pipeline_name` column; filter other audit tables by run ID/release, not a nonexistent column.

### Stage 3 — Bronze and release-package readiness

All 13 Bronze tables for the selected dataset profile must exist and be readable, with no profile mix-up or unexpected source-table substitution. Validate source schemas, the manifest, physical files, and lineage according to the Raw-to-Bronze contract; table existence alone does not prove that the correct release was imported. The Raw-to-Bronze workflow is responsible for destructive Bronze schema refresh and manifest/file validation; Silver may reuse that run-scoped evidence.

Required release-package coverage (the complete requirements are listed here):

1. Identify the complete dataset profile and its clean/tainted or production condition. Each profile contains 13 Parquet files plus required `manifest.json` in the same directory as those files, not a monthly delta that needs earlier packages. Preserve package provenance and the selected profile (`napa_5k_clean`, `napa_5k_tainted`, `napa_50k_clean`, `napa_50k_tainted`, or `napa_250k_production`). Do not use hidden injection manifests or generator internals.
2. Validate manifest `release_name`, `student_dataset_schema_version`, `included_batch_months`, `included_batch_sequences`, `snapshot_month`, `snapshot_end_exclusive`, `parquet_compression`, `build_timestamp`, `validation_status`, and `validation_summary`. Require `snapshot_month` to be a valid calendar month and `snapshot_end_exclusive` to be the first day following that month. Compute `snapshot_as_of_date` as the day before the exclusive boundary; do not hard-code example dates or use wall-clock time, pipeline execution time, manifest build time, or `MAX(monthly_batches.batch_month)` as a substitute.

For example, a manifest containing:

```json
{
  "snapshot_month": "2025-12-01",
  "snapshot_end_exclusive": "2026-01-01"
}
```

represents a release whose snapshot as-of date is 2025-12-31 and whose included history ends before 2026-01-01. This is an example of the calculation, not a hard-coded release value.
3. Reconcile file-level evidence from `output_files` and the manifest's `row_counts`, `ordered_columns`, `schema_hashes`, and `file_checksums`. Verify file inventory, actual counts, ordered columns, physical schema, checksums, and matching release identity. The Raw-file checksum is SHA-256 of the delivered Parquet file bytes. The Raw schema hash is SHA-256 over UTF-8 text formed by joining each source column's ordered `column_name:data_type:nullable` with `|`; use Spark `simpleString()` for `data_type` and Python `True`/`False`/`None` spelling for nullability. Do not substitute the Silver operational audit hash or guess a different canonicalization.
4. Reuse verified Raw-to-Bronze manifest/readiness evidence when available, with an explicit provenance link to this run and selected dataset profile. Otherwise read the colocated package metadata/files read-only using a configurable manifest/provenance location. Do not require a new clean/tainted field inside the existing manifest; the selected profile and isolated Raw/Bronze schema path establish the dataset condition. Preserve the distinction between the application-selected profile and the manifest's native `release_name`; a documented mapping may relate them without requiring identical strings. Do not assume Bronze audit tables contain snapshot fields unless their actual schemas demonstrate that they do. Missing, malformed, or contradictory release metadata must cause technical validation failure; do not invent a substitute snapshot date.
5. Validate the physical contract in Appendix A before aliases or casts can hide drift. Bronze metadata columns are additional technical columns, not unexpected source fields. Count reconciliation against the manifest applies to Raw/Bronze source rows, not deduplicated/filtered Silver rows.
6. Validate full historical coverage and snapshot boundaries as described in Stage 9. Do not infer completeness from one maximum batch date or the presence of a table. Events and timestamps must be strictly before `snapshot_end_exclusive`; an event exactly at that exclusive boundary is outside the release. For timestamp values, compare to the exclusive boundary rather than midnight on the final snapshot day.

Both clean and tainted variants are legitimate engineering inputs. Distinguish a corrupted/misidentified package from intentional, manifest-consistent business defects. Report, quarantine, or flag business defects according to the explicit policies below; do not assume every quality finding invalidates the whole release.

**Pre-build target reset (required after successful preflight):** As the last step of `validate_bronze_sources`, and only after all required configuration, environment, source, manifest, and Bronze-readiness checks have succeeded, resolve and verify the selected profile's fully qualified target names. Drop the five documented convenience views using `DROP VIEW IF EXISTS`, followed by the 13 designated accepted Silver tables and 13 designated Silver exception tables using `DROP TABLE IF EXISTS`. Use the exact target registry and naming contracts in this prompt; never discover targets by dropping every object in a schema. If a target cannot be safely resolved, belongs to another profile, has an unexpected object type, or the reset fails, stop and report a technical failure. Do not start table construction after an incomplete reset. On a first run, absent targets are normal. This reset clears the outputs from any preceding successful or failed run but does **not** delete Bronze, Raw, operations tables, run histories, or unrelated objects. If any preflight check fails, do not reset anything. If reset or later construction fails, preserve whatever objects and operational evidence remain; do not execute failure cleanup or rollback. A new run repeats preflight validation before it can clear those outputs.

### Stage 4 — Shared transformation, deduplication, and publication framework

Use distributed Spark SQL/DataFrame operations for the actual workload. Keep notebooks/scripts thin and reusable logic in project source modules. Avoid collecting entire Bronze datasets to the driver. Pure-Python toy-data helpers may support tests, but must not replace scalable execution or be assumed to have SQL parity without testing.

Governing data-quality policy:

- Record-level data-quality defects are expected processing conditions. Log them, preserve the source occurrence in the appropriate exception table, and exclude the invalid occurrence from the accepted Silver table. A positive data-quality finding count, including CRITICAL or ERROR findings, does not by itself fail pipeline execution.
- Technical failures—including missing or unreadable Bronze tables, namespace or dataset-selection errors, unrecoverable schema/configuration incompatibilities, failed Delta writes, runtime exceptions, incomplete publication, failed required tasks, or unexplained reconciliation differences—must fail the pipeline.
- Never synthesize corrections. Do not generate replacement keys, invent dates/ratings/scores, replace invalid numerics with zero/averages/estimates, choose conflicting source values arbitrarily, or fill invalid required fields through undocumented inference. Preserve original values in Bronze and expose them through exception evidence.
- Permitted standardization is deterministic and documented: trim whitespace, map known domain aliases, convert valid date/numeric representations, derive legitimate calculated fields from accepted values, and generate technical surrogate keys from valid natural keys. Standardization must not conceal an explicitly invalid supplied value; for example, do not replace an invalid country with a country inferred from region.
- A successful execution status means the required pipeline work completed and outputs were published. It does not certify that the source is defect-free or suitable for every Gold analysis. Keep execution status, data-quality summaries, and downstream eligibility distinct.

Governing attribute-preservation and naming policy:

- Every business attribute published by a Bronze source table must have a documented corresponding attribute in the accepted Silver target table. Silver is attribute-complete, not row-complete: records that fail validation remain excluded from accepted Silver and are preserved through the established exception tables.
- Do not silently discard optional, unused, currently unreferenced, redundant, or analytically unexplored business attributes. Preserve separately published source attributes even when their values are expected to agree. Do not merge distinct source attributes into one Silver column if information would be lost.
- Bronze remains the authoritative unchanged source of original values. Silver may standardize values and types or apply a documented canonical rename, but every source-to-Silver mapping must remain traceable. Add derived and data-quality attributes without removing unrelated source information.
- Classify columns by origin and use these conventions across all 13 accepted Silver tables: source business attributes retain their original or documented canonical business name; standardized source attributes use a meaningful business name without a special prefix; derived business attributes use the `derived_` prefix; data-quality indicators use the `dq_` prefix; surrogate keys retain the `_sk` suffix; operational metadata uses a leading underscore.
- Do not add `derived_` to a standardized version of an existing source attribute. Do not classify a column solely from its name when source provenance establishes otherwise. For example, `player_assessment_history.derived_from_matches` is a source attribute and must remain source-origin.
- Preserve source business values and numerical meaning. Document pass-through, rename, normalization, cast, calculation, and validation behavior; avoid unnecessary precision loss. Do not introduce imputation, interpolation, inferred repairs, or synthetic corrections.

For every source table, maintain a source-to-Silver attribute mapping with Bronze source table/column, Silver target table/column, origin classification (`source`, `standardized`, `derived`, `quality`, `surrogate`, or `metadata`), resulting data type, transformation, business description, and relevant assumptions. The mapping must enumerate every published Bronze business attribute and every new Silver-derived or quality attribute. Use the existing project documentation and configuration conventions; do not introduce a separate database or external cataloging service.

Apply the naming convention as follows:

| Attribute category | Convention | Example |
|---|---|---|
| Source business attribute | Original or documented canonical business name | `rating`, `birth_date`, `volatility_score` |
| Standardized source attribute | Meaningful business name without a special prefix | `country_code`, `gender`, `team_status` |
| Derived business attribute | `derived_` prefix | `derived_age_at_snapshot`, `derived_score_margin` |
| Data-quality indicator | `dq_` prefix | `dq_membership_overlap_flag` |
| Surrogate key | `_sk` suffix | `player_sk`, `team_sk` |
| Operational metadata | Leading underscore | `_pipeline_run_id`, `_load_ts` |

For example, rename `age` to `derived_age_at_snapshot` when it is snapshot-based, use `derived_age_at_match` for event-time age, rename `score_margin` to `derived_score_margin`, `membership_duration_days` to `derived_membership_duration_days`, `membership_overlap_flag` to `dq_membership_overlap_flag`, and `side_cardinality_warning_flag` to `dq_side_cardinality_warning_flag`. Do not rename a source attribute solely because its name contains `age`, `status`, `derived`, or `flag`; classify it from provenance and meaning.

Safe conversion policy:

- All potentially malformed numeric, date, timestamp, integer, boolean, and categorical conversions must use non-throwing built-in logic supported by the target Databricks runtime, such as Spark SQL `TRY_CAST` or an equivalent defensive expression. Validate the chosen expressions in the actual serverless environment. Do not use Python UDFs or introduce a general conversion framework for this requirement.
- Evaluate conversion success before applying business rules. A valid non-null source value converts and proceeds to normal validation; a legitimate null in an optional field remains null; a null in a required field receives a missing-required-value rejection; and a non-null malformed value receives an invalid-type or invalid-format rejection. A null produced by failed conversion must never be treated as a legitimate source null.
- Preserve the original source value and affected source column in exception evidence. Use meaningful reason codes such as `INVALID_NUMERIC_VALUE`, `INVALID_DATE_FORMAT`, `INVALID_BOOLEAN_VALUE`, and `MISSING_REQUIRED_VALUE`, reusing existing rule IDs where appropriate. Do not store only the failed conversion's null result.
- Permitted standardization remains deterministic: trim whitespace, convert valid numeric/date/timestamp representations, normalize approved categorical aliases, and convert a valid `YYYY-MM` month to the first day of that month when the source contract defines a month-level date. Do not replace invalid values with zero, averages, plausible estimates, snapshot dates, inferred values, or values from another attribute.
- A value may convert successfully and still fail a business rule. For example, a negative score, invalid winning side, reversed date interval, or unsupported country domain must be rejected under its specific rule rather than treated as a generic conversion failure. Do not replace table-specific business rules with null checks.
- An unreadable Parquet table, missing required physical column, unsupported conversion expression, unrecoverable schema incompatibility, or unexpected Spark exception is a technical pipeline failure. A recoverable malformed value is a record-level quality defect and must not abort processing of other records.

Required conversion outcomes:

| Source condition | Required outcome |
|---|---|
| Valid non-null value | Convert and continue with the remaining validation rules |
| Legitimate null in an optional field | Preserve null and accept if other rules pass |
| Null in a required field | Reject with a missing-required-value reason |
| Non-null malformed value | Reject with an invalid-type or invalid-format reason and preserve the original value |
| Successfully converted value violating a business rule | Reject under the specific business rule |

For example, a source rating of `"3.45"` converts to `3.45` and can be accepted; `"INVALID"` converts to no accepted value and is rejected as `INVALID_NUMERIC_VALUE`; an optional source null remains null; and a required source null is rejected as `MISSING_REQUIRED_VALUE`. These outcomes must be distinguishable in tests and audit evidence.

Transformation sequence:

1. Select and normalize the table-specific input projection while retaining every Bronze business attribute; trim string representations, turn blank identifiers/domain values into null where the SQL does so, and uppercase domain inputs. Natural and foreign identifiers in Silver are strings, even though source IDs are INT64.
2. Preserve every Bronze source-row occurrence through normalization and validation. Do not use `DISTINCT`, grouping, projection, or a winner-selection step that can remove duplicate multiplicity before duplicate-key evaluation. Normalize each primary key according to its documented type and whitespace rules; evaluate null/blank keys separately with the missing-key rule.
3. Detect duplicate non-null primary keys before any operation that could collapse source multiplicity. Reject **every occurrence** sharing a duplicated key—including identical duplicates and conflicting attribute values—with a clear reason such as `DUPLICATE_PRIMARY_KEY`. Do not select a winner using completeness, timestamp, hash, source order, or any other ranking. Record `duplicate_key_rejected_count` as a diagnostic subset of rejected rows. Stable surrogate-key derivation remains permitted only for accepted records with valid, unique natural keys.
4. Apply safe conversions and explicit conversion-validity checks before business rules; preserve the original source values for diagnostics. Resolve required foreign keys against accepted parent Silver tables and evaluate table-specific invalid-row conditions. Record the first matching rejection reason in the documented SQL CASE order rather than emitting multiple rejected occurrences for one source row unless an existing rule explicitly requires additional evidence. Data-quality rejection does not itself fail the pipeline.
5. Publish accepted rows only when the source occurrence has a valid unique primary key, valid required values, and valid required parent references. Write every other source occurrence to the appropriate exception table with its original diagnostic values. Match participants have additional structural rejection rules described below; those rules must preserve one outcome per source occurrence.
6. Append seven accepted-row metadata columns: `_pipeline_run_id`, `_pipeline_version`, `_source_dataset`, `_source_table`, `_load_ts`, `_record_hash`, `_data_quality_status`. `_source_dataset` is the release name. `_source_table` is the Bronze source name, including `player_master` for `players`. SQL sets `_data_quality_status` to `ACCEPTED` even when separate quality indicators are true.
7. Enforce required foreign keys in the established build order by joining against accepted Silver parent records. If the referenced identifier is absent from the original Bronze parent dataset, reject the child with `INVALID_FOREIGN_KEY`. If the parent exists in Bronze but was rejected from Silver, reject the child with `PARENT_REJECTED`. Do not manufacture parent records, redirect references, or recursively reject unrelated hierarchy members. A nullable optional reference may remain null when the source contract permits it. Preserve the original child occurrence and distinguish direct orphan defects from parent-rejection consequences.
8. Each `<entity>_sk` is SHA-256 of its natural key as a string, with `<NULL>` as the null sentinel. It does not include a table, release, or run salt. Foreign surrogate keys are obtained from accepted parents. Accepted record hashes use selected fields, not every output column; never treat them as proof that all attributes are unchanged.

Use these record-hash field sequences, joined with `|`, with null represented by `<NULL>` and values cast to strings before hashing:

| Table | Ordered record-hash inputs |
|---|---|
| monthly_batches | batch_id, batch_sequence, derived_batch_date |
| regions | region_id, region_name, country_code |
| players | player_id, first_name, last_name, birth_date, home_region_id |
| player_registrations | registration_id, player_id, batch_id, registration_date, effective_start_date, effective_end_date |
| player_assessment_history | assessment_id, player_id, batch_id, assessment_date, assessment_type, assessment_value |
| clubs | club_id, club_name, region_id, country_code |
| teams | team_id, team_category, team_identity_type, country_code |
| club_memberships | club_membership_id, player_id, club_id, membership_start_date, membership_end_date |
| team_memberships | team_membership_id, team_id, player_id, membership_start_date, membership_end_date |
| matches | match_id, batch_id, region_id, match_date, winning_team_id, derived_winning_team_number |
| match_teams | match_team_id, match_id, team_id, team_number |
| match_team_players | match_team_player_id, match_team_id, player_id |
| match_games | match_game_id, match_id, game_number, team_one_score, team_two_score |

Reject tables are named `<target_table>_exceptions` in the reject schema and contain this shared projection:

```text
source_table, target_table, source_business_key, reject_reason,
reject_reason_code, reject_reason_detail, rule_id, rule_severity,
failed_source_column, failed_source_value,
pipeline_run_id, _pipeline_run_id, _source_dataset, load_ts, _load_ts,
source_record_json, _record_hash
```

`reject_reason_code` repeats `reject_reason`; duplicate run/time fields support operational traceability. JSON represents the transformation's selected source/diagnostic fields, not necessarily the complete original Bronze row. Reject hash is SHA-256 of that JSON. Keep original Bronze data for full evidence.

Alias handling for foundational, athlete, and organization tables must choose the **first column present in the schema**, not the first nonnull value in each row. A present-but-null preferred column does not trigger an alias fallback. Competition transformations may prepare missing candidate columns as typed nulls and use row-level `COALESCE` in their expressions. Test this distinction. Alias selection and deterministic standardization must not conceal an explicitly invalid supplied value.

Use safe, non-throwing conversion expressions for every potentially malformed source value. For dates and timestamps, use supported defensive parsing that distinguishes valid values from malformed values without aborting the transformation. Validate the expressions in the actual Databricks serverless environment and document runtime compatibility. Preserve the original offending value in `failed_source_value` and/or `source_record_json`; never emit only the null conversion result. Conversion failures are recoverable record-level rejections, while unsupported expressions, unreadable inputs, failed writes, and unexpected runtime exceptions remain technical failures.

After the required Stage 3 pre-build reset, publish the complete expected set of 13 accepted Silver tables and 13 reject/exception tables with `CREATE OR REPLACE TABLE <fqn> USING DELTA AS <select>`, including empty results with their declared SQL schema. Every accepted Silver table must contain all mapped Bronze business attributes, existing standardized fields, derived business fields, data-quality indicators, surrogate keys, and operational metadata. Use full-refresh table replacement for the specified targets, never append. A completely successful full job produces a complete selected-profile release with no business rows carried forward from the preceding run. Do not automatically delete unexpected database objects. If a technical failure occurs after the reset or after some tables are published, preserve available Silver tables, exception records, quality evidence, and operational audit evidence; mark the run incomplete/FAILED and require a subsequent successful full-job rerun before downstream Gold consumption. The Bronze, Raw, and operations schemas must remain intact.

For each table, record source/exact-duplicate/duplicate-key-rejected/accepted/rejected/warning/published counts, quality evidence, accepted-table schema snapshot, and terminal status. Reject quality evidence is grouped by rule ID/severity, with failed count, evaluated source count, percentage (null for zero denominator), and up to ten sorted sampled business keys. No reject groups means no successful row-rule records are automatically emitted. Warning counts and row flags are separate from these reject groups. Direct defects, duplicate-primary-key rejections, parent-rejection consequences, and non-rejecting warnings must remain distinguishable without double-counting rejected occurrences. A successful run must verify that the selected Silver schema contains exactly the 13 expected accepted tables plus the documented convenience views, and that the selected Silver-reject schema contains exactly the 13 expected reject/exception tables, with no unexpected tables.

### Stage 5 — Reference tables

Use this target registry. Every source file is `<Bronze source>.parquet`; natural key is source `id` except `player_master.player_id`. Transform names are `build_<Silver target>`.

All conversions in Stages 5–8 use the safe-conversion policy above. This includes ratings and confidence values, assessment values, registration/birth/membership/team/match dates, integer identifiers and sequences, match-side ratings, game scores, target scores, win-by values, score shares, booleans, and categorical domains. Where the Parquet schema already supplies an appropriate physical numeric, date, timestamp, or boolean type, validate that physical contract rather than inventing a string-malformation case. Distinguish an invalid record value from a missing required physical column or incompatible source schema; only the latter is a technical failure.

| Registry order | Stage | Bronze source → Silver target | Target primary key |
|---:|---|---|---|
| 10 | reference | monthly_batches → monthly_batches | batch_id |
| 20 | reference | regions → regions | region_id |
| 30 | athlete | player_master → players | player_id |
| 40 | organization | clubs → clubs | club_id |
| 50 | partnership | teams → teams | team_id |
| 60 | athlete | player_registrations → player_registrations | registration_id |
| 70 | athlete | player_assessment_history → player_assessment_history | assessment_id |
| 80 | organization | club_memberships → club_memberships | club_membership_id |
| 90 | partnership | team_memberships → team_memberships | team_membership_id |
| 100 | competition | matches → matches | match_id |
| 110 | competition | match_teams → match_teams | match_team_id |
| 120 | competition | match_team_players → match_team_players | match_team_player_id |
| 130 | competition | match_games → match_games | match_game_id |

Execution is by stage, then order **within** stage: batches/regions; players/registrations/assessments; clubs/club memberships; teams/team memberships; matches/sides/participants/games. Do not globally sort the registry and assume that is the workflow. Organization and partnership execute sequentially inside one task.

The registry also declares `assessment_confidence`, `membership_start_date`, and `competition_category` as required contract columns for assessments, team memberships, and matches respectively. These are Silver contract expectations, not literal schema 1.6 source columns. Validate them explicitly rather than relying only on generic preflight checks.

**monthly_batches:** Map `id` (fallback `batch_id`) to string `batch_id`. Parse batch date from existing `batch_date`, `release_date`, or `date`, otherwise `batch_month`; convert a valid `YYYY-MM` month to its first day. Safely convert `batch_sequence` (fallback `sequence`) to integer; uppercase `batch_type` and optional `batch_status`/`status`. Derive year, month, quarter. Reject missing/blank ID (`BATCH_001`, CRITICAL), malformed nonblank date (`BATCH_002`, ERROR), malformed or negative sequence (`BATCH_003`, ERROR). Zero sequence is not rejected. Null date/sequence are not comprehensively rejected by these rules. No full historical sequence validation occurs here.

**regions:** Map `id`/`region_id`, `region_name`/`name`, `province_state`/`province`/`state`, `country_code`/`country`, and preserve `state_province_code` as its own source attribute. Standardize country to USA/CAN. Reject missing key (`REGION_001`, CRITICAL), missing name (`REGION_002`, CRITICAL), and invalid/missing country (`REGION_003`, ERROR). Document any canonical `province_state` mapping separately from the preserved source value.

Use the manifest-derived `snapshot_as_of_date` for current-state calculations. Do not use `MAX(accepted monthly_batches.derived_batch_date)` as a universal as-of date; batch dates remain available for batch-period context and included-period validation.

### Stage 6 — Athlete tables

**players:** Map `player_master` to `players`. Prefer `player_id` over `id`; names retain trimmed case. Use `display_name`/`full_name` if supplied, otherwise combine available first/last names. Resolve `birth_date`/`date_of_birth`/`dob`; `dominant_hand`/`handedness`; `home_region_id`/`region_id`; `rating_value`/`rating`/`player_rating`; `confidence_score`/`rating_confidence`/`confidence`; status from `player_status`/`active_flag`/`status`. Safely parse populated birth dates and numeric rating/confidence values, retaining original values for any rejected record. Join accepted regions. Country is normalized from the supplied source country; a region country may be used only when the source country is absent and the documented contract permits that derivation. An explicitly invalid supplied country must be rejected, not masked by a region fallback.

Safely convert rating/confidence to DOUBLE and reject non-null malformed values with the appropriate conversion reason. Compute `derived_age_at_snapshot` and `derived_age_group_at_snapshot` using `snapshot_as_of_date` with birthday adjustment and the configured age groups. Status strings `TRUE,T,YES,Y,1,ACTIVE` give true; `FALSE,F,NO,N,0,INACTIVE` give false; other/null values give null, with unsupported required status values rejected under the categorical-domain rule. Produce `derived_active_flag` from the documented status semantics and do not filter inactive players from the base table.

Reject in this priority: missing key (`PLAYER_001`, CRITICAL); malformed or future birth date (`PLAYER_005`); populated orphan home region (`PLAYER_002`); invalid supplied gender (`PLAYER_003`); hand (`PLAYER_004`); unresolved invalid supplied country (`PLAYER_007`); malformed rating (`PLAYER_008`); malformed confidence (`PLAYER_009`). These other findings are ERROR. Do not invent rating/confidence bounds. Apply the required source-field/nullability contract in Appendix A, and permit optional nulls where that contract allows them.

**player_registrations:** Map `registration_id`/`id`; preserve `registration_month`, `registration_source`, `assigned_region_id`, `initial_rating_value`, and `initial_confidence_score`; resolve required accepted player and optional populated `batch_id`/`monthly_batch_id`. Safely parse populated registration/start/end values; date uses `registration_date`, otherwise `registration_month`; start uses `effective_start_date`/`start_date`, otherwise `registration_month`; end uses optional `effective_end_date`. A valid `YYYY-MM` month becomes the first day of that month. Uppercase `registration_type`/`registration_source`.

Reject missing key (`REG_001`, CRITICAL), missing/orphan player (`REG_002`), populated orphan batch (`REG_003`), malformed registration/start/end dates (`REG_004`/`005`/`006`), and end before start (`REG_007`). Derive `derived_registration_status` ACTIVE when end is null or at/after `COALESCE(snapshot_as_of_date, registration_date)`, otherwise INACTIVE; if that comparison date is null, status is null. `derived_current_registration_flag` additionally considers future start relative to `snapshot_as_of_date`, with inclusive boundaries and the same fallback date. `derived_registration_duration_days = snapshot_as_of_date - effective_start_date`, regardless of end date. Assign `derived_registration_sequence` per player ordered by registration date nulls first, effective start nulls first, then registration ID. Do not substitute membership-duration logic.

**player_assessment_history:** Map `assessment_id`/`id`, player, batch alias as above; date from `assessment_date`, otherwise `assessment_month`/`snapshot_month`; uppercase assessment type; value from `assessment_value`/`value`; confidence from `assessment_confidence`/`confidence`/`confidence_score`. Safely convert value/confidence to DOUBLE and preserve the original assessment date and batch reference. Reject missing key (`ASSESS_001`, CRITICAL), missing/orphan player (`ASSESS_002`), populated orphan batch (`ASSESS_003`), malformed or future assessment date relative to `snapshot_as_of_date` (`ASSESS_004`), malformed value (`ASSESS_005`), malformed confidence (`ASSESS_006`); other severities ERROR. Do not use later assessments to rewrite earlier events. Enforce the required batch/date/type coverage defined in Appendix A and Stage 9.

### Stage 7 — Organization and partnership tables

**clubs:** Map `club_id`/`id`, `club_name`/`name`, required region, country with region fallback only when the source country is absent and the contract permits it, opening date from `open_date`/`start_date`/`formation_date`/`founding_date`, and closing date from `close_date`/`end_date`/`dissolution_date`. Safely parse each populated date and reject malformed values without aborting other records. Preserve `club_type`, `competitiveness_level`, `member_capacity`, `founding_date`, `indoor_court_count`, and `outdoor_court_count`. Resolve region surrogate against accepted regions. Reject missing key/name, missing/invalid required parent, invalid explicitly supplied country, malformed dates, or closing before opening. Rule IDs `CLUB_001` through `CLUB_007` cover key, name, region, country, opening date, closing date, and reversed interval. Missing key/name are CRITICAL; the other failures are ERROR.

**teams:** Preserve source `team_type`, `team_division`, `team_status`, `country_code`, `formation_date`, and `dissolution_date` while also producing canonical category, identity, country, status, and date fields. Category precedence is supplied `team_division`, then `team_category`/`category`, then legacy division-valued `team_type`. Invalid explicit higher-priority values do not silently fall back. Modern `team_type` becomes identity COMPETITIVE/AD_HOC, while legacy category values may yield null identity. Standardize status, country, formation/start and dissolution/end dates. Active boolean-like `active_flag`/`status` overrides; otherwise normalized team status ACTIVE yields true, another populated status false, absent status null. `derived_team_age_at_snapshot_days` is `snapshot_as_of_date` minus formation date, and the resulting flag is `derived_active_flag`.

Reject missing key (`TEAM_001`, CRITICAL), invalid category (`TEAM_002`), invalid identity not explainable as legacy category (`TEAM_008`), invalid status (`TEAM_003`), invalid country (`TEAM_004`), malformed formation/dissolution (`TEAM_005`/`006`), or dissolution before formation (`TEAM_007`); other severities ERROR. Also apply the explicit release-boundary checks in Stage 9. Never interpret a team identity as its gender division or create replacement team IDs.

**club_memberships:** Resolve membership ID, required accepted player and club, start from `membership_start_date`/`joined_date`/`start_date`, end from `membership_end_date`/`left_date`/`end_date`. Safely parse populated start/end values and reject malformed values without aborting other records. Reject missing key, missing/orphan parents, malformed dates, or end before start (`CLUB_MEMBERSHIP_001` through `006`; key CRITICAL, others ERROR). Start null can pass when the source contract permits it. Duration is `COALESCE(end, snapshot_as_of_date) - start` when an open interval is measured for current-state context. `derived_current_membership_flag` uses inclusive interval bounds at `COALESCE(snapshot_as_of_date, start)`, null if no comparison date. Historical validity uses the actual event date, not the snapshot date.

**team_memberships:** Same interval handling with required accepted team/player, membership ID, optional `player_role`/`role`, and normalized `player_position`/`preferred_side`/`position`. Safely parse populated start/end values and reject malformed values without aborting other records. Reject missing key, missing/orphan player or team, malformed start/end, reversed interval, or invalid position (`TEAM_MEMBERSHIP_001` through `007`; key CRITICAL, others ERROR). Unlike club memberships, duration is end minus start **only when both are supplied**; an open-ended membership has null duration. `derived_current_membership_flag` uses the same inclusive `snapshot_as_of_date` comparison, while event-time validity uses the historical event date.

For both membership tables, set `dq_membership_overlap_flag` against the immediately preceding interval within `(player_id, club_id)` or `(player_id, team_id)`, ordered by start ascending nulls first, end ascending nulls last, membership ID. Treat preceding null end as `9999-12-31`; next start at or before preceding end is overlap. Keep these rows accepted and count warnings. This `LAG` implementation does not detect every nested-interval overlap; a complete interval-overlap validator is an explicit compliance improvement.

### Stage 8 — Competition tables and persistent team identity

**matches:** Resolve ID from `match_id`/`id`; safely parse populated `match_date`/`date`; optional populated batch and region must resolve to accepted parents. Preserve `court_type`, `match_format`, and `total_points_played` in addition to the standardized match attributes. Derive `competition_category` by uppercasing `match_type`, not from a separate category column. Preserve persistent `winning_team_id` from `winning_team_id`/`winner_team_id`. Derive side number by joining **Bronze** `match_teams` on the same match ID and persistent team ID, requiring an unambiguous side 1 or 2. An explicit `winning_team_number`/`winner_team_number` must be safely converted when populated, valid, and agree with the derived value when both exist. This Bronze lookup avoids a circular dependency before Silver sides are built.

Reject missing key (`MATCH_001`, CRITICAL), populated orphan batch (`MATCH_002`), populated orphan region (`MATCH_003`), malformed date (`MATCH_004`), invalid/ambiguous winner mapping or conflicting side number (`MATCH_005`); others ERROR. `derived_completed_flag` is true when a winning side number exists; derive `derived_match_year` and `derived_match_month` from date. Also enforce the required batch/date/type fields in Appendix A and snapshot-boundary checks in Stage 9.

**match_teams:** A row is a match side with its own `match_team_id`; `team_id` is the persistent team. Preserve source `team_score` and `average_team_rating`. Require accepted match and accepted team. Safely convert `team_number`/`side_number` to integer 1 or 2 and safely parse pre-match rating from `pre_match_team_rating`/`team_rating_before`/`average_team_rating`. Post-match rating and rating change remain null when no source-supported calculation exists; do not manufacture post-match outcomes. Inherit match date/keys from the accepted match and produce `derived_winner_flag`. Count accepted side rows per match and set `dq_side_cardinality_warning_flag` for counts other than two.

Reject missing key (`MATCH_TEAM_001`, CRITICAL), missing/orphan match (`MATCH_TEAM_002`), missing/orphan persistent team (`MATCH_TEAM_006`), invalid side number (`MATCH_TEAM_003`), or malformed pre-match rating (`MATCH_TEAM_004`); others ERROR. A count of two is not proof of distinct side numbers or distinct persistent teams; separately apply the structural checks in Stage 9.

**match_team_players:** Require accepted match side and accepted player. Preserve `player_rating_at_match` as the historical match-time rating; never replace it with snapshot `rating` or `rating_value`. Normalize position and optional rating from `player_rating_at_match`/`rating_at_match`; inherit match/team/date context from the side. Calculate `derived_age_at_match` and `derived_age_group_at_match` from the accepted player's birth date and the actual match date using the configured age groups. Do not use `snapshot_as_of_date` for these event-time calculations. Initial rejects: missing key (`MATCH_TEAM_PLAYER_001`, CRITICAL), missing/orphan side (`002`), missing/orphan player (`003`), invalid position (`004`), malformed rating (`005`); others ERROR. After duplicate-primary-key rejection, reject all implicated participant occurrences when the same player occurs repeatedly on one side (`006`, `INVALID_PARTICIPANT_CARDINALITY`) or appears on both sides of the same match (`007`, `PLAYER_ON_BOTH_MATCH_SIDES`), respecting first-reason precedence and preserving one final outcome per source occurrence.

For accepted participants, set `dq_membership_history_warning_flag` only when history exists for that team/player but no interval contains the actual match date inclusively. **No history at all does not trigger this flag.** Do not reject historical participation solely because the player or team is inactive at the snapshot. Count these warnings plus side groups whose accepted participant count differs from two. Do not confuse this limited check with proof of valid membership for every participant; report missing history separately.

**match_games:** Safely convert game number, scores, winner side, optional target score, optional win-by, and optional score share using non-throwing expressions. Require accepted match, positive integer game number, nonnegative integer scores, and winner side 1 or 2. Reject non-null malformed values with conversion-specific evidence; then reject ties and any winner inconsistent with the larger score. Optional target score and win-by must be nonnegative; optional actual side-one score share must, when total points > 0, agree with `team_one_score / total_points` within 0.0001. No invented universal score target or rating range.

Rule IDs `MATCH_GAME_001` through `011` cover, respectively: missing key (CRITICAL), missing/orphan match, invalid game number, invalid side-one score, invalid side-two score, invalid winner side, winner/score disagreement, invalid target, invalid win-by, invalid score-share type, and score-share mismatch (others ERROR). Derive absolute `derived_score_margin`, `derived_total_points`, `derived_close_game_flag` for margin <= 2, and `derived_extended_game_flag` when winning score is strictly greater than target. Also check uniqueness of `(match_id, game_number)` and report target/win-by anomalies under the explicit game rules; do not invent additional tournament score thresholds.

### Stage 9 — Cross-table validation and required quality evidence

Implement the 11 cross-table SQL checks after all tables are built. Emit one PASSED/FAILED quality result per check, including denominator, failed count, severity, timestamp, and up to ten sampled keys. Aggregate warning/error counts are counts of findings, not necessarily distinct affected entities.

Recoverable conversion failures are data-quality findings: record the rejection and original value, continue processing valid records, and do not fail the workflow solely because conversion-rejection counts are positive. Unsupported conversion expressions, unreadable inputs, failed writes, unexpected Spark exceptions, missing required execution evidence, and unexplained reconciliation differences remain technical failures.

| Rule ID | Quality check | Severity |
|---|---|---|
| `CROSS_TEAM_001` | Active teams have two current membership rows | WARNING |
| `CROSS_MATCH_001` | Completed matches have two accepted side rows | WARNING |
| `CROSS_MATCH_002` | Completed matches have at least one accepted game | WARNING |
| `CROSS_MATCH_TEAM_001` | Each accepted side has two accepted participants | WARNING |
| `CROSS_MATCH_TEAM_002` | Populated side team ID resolves to Silver teams | ERROR |
| `CROSS_PLAYER_001` | Team-membership player IDs resolve to Silver players | ERROR |
| `CROSS_PLAYER_002` | Match-participant player IDs resolve to Silver players | ERROR |
| `CROSS_TEAM_002` | Active teams have two distinct current players | ERROR |
| `CROSS_TEAM_003` | Active teams do not duplicate the same sorted pair of two distinct current players | configured, default WARNING |
| `CROSS_WINNER_001` | Match winner agrees with side winner flags when present and with game-win aggregation | ERROR |
| `CROSS_WINNER_002` | Populated match winning team ID occurs among that match's persistent side team IDs | ERROR |

For game-win aggregation, choose the side with most game wins; a tie is broken by ascending side number. Treat that tie-break as a deterministic calculation convention, not a rule for deciding actual tournament outcomes. The cross-table task records data-quality findings but does not fail solely because ERROR or WARNING findings exist. Technical query failures or missing required cross-table evidence still fail the pipeline.

Record quality evidence across **all eight** categories below. The High/Medium labels describe priority, not the pipeline's CRITICAL/ERROR/WARNING enum, and are not automatic whole-run abort rules. Document the distinction without inventing percentage allowances.

| Quality category | Priority | Required evidence |
|---|---|---|
| Release integrity | High | Expected files/manifest, checksums, ordered schemas and schema hashes, row counts, correct release/variant/version |
| Primary keys | High | Every accepted published PK nonnull and unique; report every original duplicate occurrence as rejected evidence |
| Foreign keys | High | Every relationship listed below resolves within the same release; distinguish nullable references and rejected-parent cascades |
| Match structure | High | Two sides, coherent side numbering, persistent team identity, valid participants, coherent winner relationships |
| Game integrity | Medium | Nonnegative scores and consistent game winner; explain treatment of target/win-by/share anomalies |
| Temporal integrity | Medium | Registration, formation, membership, rating, assessment, match, and dissolution dates have logical ordering and fit the release snapshot |
| Historical completeness | High | Included months/sequences and completed-batch history agree with manifest; fact history and as-of snapshots are coherent |
| Tournament eligibility | High | Referenced team IDs exist, memberships and divisions are valid; retain evidence for downstream eligibility without selecting a roster |

Validate these source relationships while retaining the related source attributes in Silver. All listed foreign keys are nonnull unless marked nullable:

| Source child field | Referenced field | Nullable |
|---|---|---|
| clubs.region_id | regions.id | no |
| club_memberships.player_id | player_master.player_id | no |
| club_memberships.club_id | clubs.id | no |
| match_games.match_id | matches.id | no |
| match_team_players.match_team_id | match_teams.id | no |
| match_team_players.player_id | player_master.player_id | no |
| match_teams.match_id | matches.id | no |
| match_teams.team_id | teams.id | no |
| matches.region_id | regions.id | yes |
| matches.batch_id | monthly_batches.id | no |
| matches.winning_team_id | teams.id, and match_teams.team_id for the same match | yes |
| player_assessment_history.player_id | player_master.player_id | no |
| player_assessment_history.batch_id | monthly_batches.id | no |
| player_master.home_region_id | regions.id | yes |
| player_registrations.player_id | player_master.player_id | no |
| player_registrations.batch_id | monthly_batches.id | no |
| player_registrations.assigned_region_id | regions.id | yes |
| team_memberships.team_id | teams.id | no |
| team_memberships.player_id | player_master.player_id | no |

Temporal/history coverage must use the following complete manifest-derived requirements:

- `snapshot_end_exclusive` is the first day after the snapshot month and `snapshot_as_of_date` is the preceding calendar day. Included event dates and timestamps are **strictly before** the exclusive boundary, not <= it. Use `snapshot_as_of_date` for current-state calculations and the actual event date for historical calculations.
- Full historical fact tables retain included completed batches and their matches/games/sides/participants, registrations, and assessments. Reconcile included batch months/sequences and parent-child history; do not append only the newest month or remove old inactive entities.
- `player_master` is a snapshot with players registered before the boundary and latest eligible rating before the boundary. Validate `snapshot_month`, registration/rating dates, `rating` alias consistency with `rating_value`, and available batch evidence. Preserve snapshot rating fields separately from match-time ratings and historical assessments. Do not invent a standalone source `players` or `player_rating_history` file to reconstruct unavailable history. State when latest-rating completeness cannot be proved from published evidence alone.
- Clubs/teams must satisfy their release eligibility dates. Memberships start before the boundary. Membership left dates and team dissolution dates beyond the snapshot are projected to null by the release; interpret these as snapshot open intervals, not proof that the real-world relationship never ends.
- For every historical match, evaluate player age and membership validity using the match date, not the snapshot date. A player who is inactive or no longer a current member at the snapshot may still have valid historical participation. Distinguish missing membership history from known contradictory history, preserve both membership endpoints, and do not manufacture intervals.
- Preserve historical participation context. A current roster is not proof of membership at a past match. Validate event-date membership and division consistency using available published fields; do not impose unstated competition eligibility thresholds.

Run source-contract/history checks on Bronze and retain the corresponding source attributes in Silver (for example registration assigned region, player registration/rating/snapshot attributes, and monthly counts). Retain run-linked evidence. Do not claim an attribute was validated or preserved unless its mapping is present in the source-to-Silver matrix. Silver may calculate deterministic temporal context, but must not add age-adjusted ratings, experience adjustments, fatigue estimates, recency models, opponent-strength adjustments, partnership scores, or roster recommendations; those belong in Gold.

### Stage 10 — Convenience views, reconciliation, and finalization

Create these five views in the release Silver schema, using explicit selected columns and fully qualified tables:

- `vw_players_current`: active players only, `derived_active_flag = true` (null is excluded); player ID/key, display name, home region, country, `derived_age_at_snapshot`, `derived_age_group_at_snapshot`, and active flag.
- `vw_current_team_memberships`: membership rows with `derived_current_membership_flag = true`, preserving membership fields and the technical projection. This is a filter, not a team/player enrichment join.
- `vw_team_rosters`: one row per team with current membership-row count, expected count two, `OK`/`WARNING` cardinality status, and sorted roster-player structures containing player ID/key/display name/role/position. Include teams without a valid current roster; do not select recommended teams.
- `vw_match_results`: match context, sides 1/2, game count and aggregated side scores, region and batch labels. Missing game aggregates default to zero. Duplicate side numbers can fan out joins; expose that quality problem.
- `vw_player_match_history`: accepted participant joined to side and match, player and opponent context, `derived_age_at_match`, `derived_age_group_at_match`, `player_rating_at_match`, and WIN/LOSS/null outcome. Opponent is another side row in the same match, not a fabricated team.

**Required reconciliation:**

```text
reconciliation_difference = bronze_row_count - (
    accepted_row_count + rejected_row_count
)
status = MATCHED only when difference == 0 and required schema/column checks pass
```

Every original Bronze row occurrence must contribute to exactly one accepted Silver occurrence or one rejected exception occurrence. Identical duplicates remain separately counted rejected occurrences. `duplicate_key_rejected_count`, parent-rejection counts, and other quality subsets are diagnostic subsets of `rejected_row_count`; never add them again to the reconciliation total. Reject tables must preserve row multiplicity, and all 13 sources must be reconciled independently. A nonzero unexplained difference is a technical pipeline failure, not merely a data-quality finding. Persist reconciliation results and diagnostic messages before raising where audit storage is available. Reconcile participants after their additional structural rejections as well.

Every reconciliation result with `status != "MATCHED"` must cause the reconciliation orchestration task to fail, including column-order, row-count, missing-column, and unexpected-column mismatches. Check these schema/column contracts separately from the row-count equation, attach their status to the relevant reconciliation result, and persist the result and diagnostics before raising whenever audit storage is available. The task must never report successful reconciliation when any source has a `MISMATCH` result.

The finalizer reads evidence for the run and expects 13 completed table records, all SUCCEEDED, and 13 reconciliation records, all MATCHED. The pipeline must separate execution status from data quality: CRITICAL, ERROR, and WARNING findings, rejected rows, and nonzero quality counts do not independently fail execution. Missing required execution evidence, failed tasks, failed writes, incomplete publication, or unexplained reconciliation differences do fail execution. The finalizer must identify selected-profile coverage, preserve quality summaries, mark failed/incomplete runs unsuitable for downstream Gold, and never delete available Silver, exception, quality, or operational evidence.

Finalize an open pipeline run with Delta `MERGE` on run ID/open completion timestamp; append an end record if no open record exists. Emit the terminal message, expose `final_status` as a task value, and raise when final status is FAILED. Repeated finalization is not guaranteed audit-idempotent. The finalizer must still attempt a failure summary after upstream failures; test failures before the run ID is available rather than assuming `ALL_DONE` solves missing task values.

The completion gate must verify exact required table/check/view evidence, distinct run-scoped coverage, reconciliation with disjoint categories, and the severity/disposition rules already stated here. Keep diagnostic quality results, workflow execution status, and downstream eligibility separate. Do not label a dataset production-certified merely because the pipeline run returns SUCCEEDED.

### Stage 11 — Databricks bundle, task entry points, and deployment runbook

The deliverable must be runnable from a clean clone and a synced bundle, not only from an interactive notebook with an accidental `sys.path`.

Requirements:

1. The root Databricks bundle must include the Bronze-to-Silver workflow definition and sync the configuration, notebooks/scripts, source modules, tests, and requirements actually used by the pipeline. Do not sync data or secrets.
2. Define `script_root` with a workspace-relative default. Provide a development target and a workspace host supplied by the team. Use a distinct team namespace. Configure an authenticated CLI profile locally; never embed tokens.
3. Job resource key: `napa_bronze_to_silver`; display name `NAPA Bronze to Silver`; `max_concurrent_runs: 1`; `performance_target: STANDARD`; pipeline/owner tags, with team ownership. One job parameter `dataset_profile`, default `napa_5k_clean`.
4. Use serverless Python where supported: environment key `napa_serverless_python`, `spec.environment_version: "4"`, dependency `pyyaml`. Every task uses this environment key and `spark_python_task`. Do not add a personal cluster ID or assume a SQL warehouse supplies this execution environment. Verify workspace support for the declared environment and task type using bundle validation and a smoke run; any alternative compute design must be documented.
5. Keep entry-point scripts thin: resolve project imports, parse arguments, obtain Spark/dbutils from supported runtime context, invoke reusable code, publish task values, and propagate exceptions. Test bundle-relative configuration lookup without a machine-specific local path. PySpark is runtime-provided; local tests should use the project's configured test dependencies, with Spark integration dependencies only when explicitly required.
6. All scripts accept `--dataset-profile` with exactly `napa_5k_clean`, `napa_5k_tainted`, `napa_50k_clean`, `napa_50k_tainted`, and `napa_250k_production`, plus optional `--config-path`; downstream scripts require `--run-id`. Resolve configuration publishes task value `run_id`. Do not generate a different run ID in each task.

Task definition (ordinary tasks depend on the immediately preceding task):

| Task key | Script under `${var.script_root}` | Timeout seconds |
|---|---|---:|
| resolve_configuration | 11_b2s_resolve_configuration.py | 900 |
| validate_environment | 12_b2s_validate_environment.py | 1200 |
| validate_bronze_sources | 13_b2s_validate_bronze_sources.py | 1800 |
| build_reference | 14_b2s_build_reference.py | 3600 |
| build_athlete | 15_b2s_build_athlete.py | 3600 |
| build_organization_partnership | 16_b2s_build_organization_partnership.py | 3600 |
| build_competition | 17_b2s_build_competition.py | 3600 |
| run_cross_table_validation | 18_b2s_run_cross_table_validation.py | 1800 |
| publish_convenience_views | 19_b2s_publish_convenience_views.py | 1800 |
| finalize_pipeline_summary | 20_b2s_finalize_pipeline_summary.py | 1200 |

Finalization depends on **all nine preceding tasks** and has `run_if: ALL_DONE`, not merely a success dependency on the view task. Task parameters are:

The `validate_bronze_sources` task performs all Bronze/manifest preflight checks **before** its final, targeted Silver output reset. Its success means both preflight and the complete reset succeeded; `build_reference` must not run if either fails. Do not add a separate cleanup-on-failure task or reset operations tables. Include the verified reset target list and outcome in run-linked operational messages.

```yaml
# Every task:
parameters:
  - --dataset-profile
  - "{{job.parameters.dataset_profile}}"
# Also append these two list entries for every downstream task:
  - --run-id
  - "{{tasks.resolve_configuration.values.run_id}}"
```

The resolve task has only the first pair. Each `python_file` is `${var.script_root}/<script>`, not a local repo path. The Python environment dependency is separate from `requirements.txt`; syncing that file does not automatically install it on job compute.

Prerequisites to document and verify: supported Databricks CLI; workspace authentication; serverless job permissions; catalog access; USE CATALOG/USE SCHEMA as applicable; source-table SELECT; authority to create needed schemas/tables/views and write/replace the team's targets; access to manifest/Raw evidence if additional checks require it. Existing objects may require ownership or additional rights for replacement. Setup must report missing authority, not attempt to grant itself broad privileges.

From the bundle root, after local tests and authorization:

```text
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev napa_bronze_to_silver --params dataset_profile=napa_5k_clean
databricks bundle run -t dev napa_bronze_to_silver --params dataset_profile=napa_5k_tainted
databricks bundle run -t dev napa_bronze_to_silver --params dataset_profile=napa_50k_clean
databricks bundle run -t dev napa_bronze_to_silver --params dataset_profile=napa_50k_tainted
databricks bundle run -t dev napa_bronze_to_silver --params dataset_profile=napa_250k_production
```

Select the team's CLI profile through local CLI configuration or `DATABRICKS_CONFIG_PROFILE`. Validate/render the bundle before deployment and inspect actual deployed paths/task dependencies. Run Raw-to-Bronze first for each chosen profile so its destructive Bronze refresh and manifest validation complete before Silver starts. Validate 5K clean and tainted, then 50K clean and tainted, and finally 250K production as the production-scale case. A successful deploy proves resource creation, not data correctness; capture job/task status, run ID, configuration hash, table/reject counts, reconciliation, quality evidence, and views after each run.

Document troubleshooting for missing Bronze tables, wrong catalog/prefix, unavailable serverless environment, missing PyYAML, import/config paths, absent task values, cast failures, rejected-parent cascades, manifest mismatches, partial table replacement, and preserved evidence after technical failure. Do not retry blindly into an unexplained partial release. Do not use `bundle destroy` as routine troubleshooting.

### Stage 12 — Tests, staged acceptance, and handoff

Write small, deterministic synthetic fixtures rather than requiring the full dataset for unit tests. Preserve the existing test stack. Separate pure/unit tests, generated-SQL contract tests, and actual Spark/Delta integration tests. A fake Spark object or SQL string assertion cannot prove a query parses, casts safely, or executes correctly under the selected runtime.

Minimum test matrix:

| Area | Required cases |
|---|---|
| Configuration | All five dataset profiles, merge precedence, nested placeholders, missing file/key, invalid profile/mode/stage/transform, invalid clean/tainted/production combination, duplicate build order, stable config hash, isolated schemas |
| Source/manifest | All 13 files/tables for the selected profile; colocated manifest/provenance; missing manifest/file; wrong release/variant/version; row count/checksum/schema/ordered-column mismatch; technical Bronze columns allowed; no guessed schema hash |
| Manifest temporal contract | Valid `snapshot_month` and `snapshot_end_exclusive`; exclusive boundary is the first day after the declared month; verified example parses to 2025-12-31; included months/sequences are consistent; selected profile maps to manifest provenance; no wall-clock or maximum-batch fallback |
| Contracts | Every Appendix A published column/null rule/type; schema 1.6 ID/decimal/boolean/string-date handling; `state_province_code` mapping; rating alias and team identity/division separation |
| Normalization | Whitespace/blank/null; all domain aliases; invalid enums; present-but-null preferred alias versus absent alias; competition COALESCE behavior; canonical source names |
| Attribute completeness | Every Appendix A Bronze business attribute maps to an accepted Silver column; optional attributes are not discarded; distinct source attributes remain distinguishable; new or unmapped source attributes are detected and reported; mapping matrix is complete |
| Naming conventions | New derived business attributes use `derived_`; data-quality indicators use `dq_`; surrogate keys retain `_sk`; operational metadata retains leading underscores; source fields are not renamed solely by name; documented mappings resolve collisions |
| Value preservation | Accepted rows retain source business information; approved standardization and type conversions preserve meaning and precision; derived formulas remain unchanged; no synthetic correction, imputation, or interpolation is introduced |
| Keys/dedup | Unique valid keys are accepted; missing keys are rejected; identical and conflicting duplicate-key records are all rejected; no `keep_best` winner or `DISTINCT` multiplicity loss; normalized keys; stable surrogate keys only for accepted records |
| Referential integrity | Valid parent references are accepted; missing required parents produce `INVALID_FOREIGN_KEY`; rejected parents produce `PARENT_REJECTED`; direct orphans are distinguishable; valid nullable references are allowed; no replacement parents or redirected keys are manufactured |
| Foundational/athlete | Negative/zero batch sequence, month conversion, birthday/leap-day age cases, inactive/unknown player flags, country fallback, missing optional region, numeric cast errors, registration ordering/status/duration, confidence mapping, future assessments, complete attribute retention |
| Safe numeric conversion | Valid numeric strings; surrounding whitespace; malformed numeric strings; null optional and required numerics; overflow/out-of-range conversion where relevant; valid numerics violating separate business rules; original values and conversion reasons retained |
| Safe date/timestamp conversion | Valid ISO dates; valid `YYYY-MM` month values; invalid calendar dates; malformed date strings; missing optional and required dates; snapshot/event boundaries; final-day timestamp comparisons; original values retained |
| Safe boolean/categorical conversion | Supported representations; unsupported values; null handling; approved alias normalization; invalid required values; conversion reasons distinct from business-rule reasons |
| Conversion rejection evidence | One rejected occurrence per defective source row; affected column and original value retained; multiple findings do not double-count reconciliation; valid records continue processing |
| Conversion pipeline behavior | Recoverable malformed records do not abort processing; valid records continue; quality findings do not independently fail execution; technical failures still fail; accepted plus rejected counts reconcile exactly; representative expressions execute in Spark rather than only through mocks or SQL-string assertions |
| Temporal age | Correct age and age group at snapshot; correct age and age group at match; birthday between matches; birthday on match date; age-group boundary transitions; leap-day convention |
| Historical membership | Valid historical membership for a player inactive at snapshot; membership beginning after a match; membership ending before a match; endpoint inclusivity; missing versus contradictory history; no manufactured intervals |
| Historical ratings | Snapshot and match-time ratings remain distinct; historical ratings are not overwritten; assessment dates/values/confidence/batch references are preserved; no future rating information enters earlier events |
| Release boundaries | Events before `snapshot_end_exclusive` are eligible; an event exactly at the boundary is excluded; historical records before the snapshot month remain; final-day timestamps compare against the exclusive boundary |
| Organization | Modern/legacy teams, invalid explicit division, identity errors, open/ended club versus team membership duration, inclusive endpoints, overlaps including nested intervals, future formation and snapshot eligibility |
| Competition | Persistent team ID versus side-row ID; missing/ambiguous/conflicting winner; valid modern match_teams.team_id; duplicate/missing sides; same player repeated or on both sides; absent versus noncovering history; no fabricated post-rating |
| Games | Negative/null/malformed scores, zero/negative game number, tie, incorrect winner, optional target/win-by/share, share tolerance just inside/outside 0.0001, close margin at 2/3, extended score equal to/above target, duplicate game number within match |
| Historical integrity/FKs | Every listed FK, nullable references, required batch nulls, assigned region validation, complete manifest sequences, date exactly at end-exclusive boundary, future left/dissolution projected null, first-of-month versus month-end as-of, available rating snapshot evidence |
| Quality/audit | First-rule precedence, one reject occurrence per source occurrence, reject JSON/hash, grouped counts/samples, direct versus parent-rejection subsets, zero-row denominator, all 11 cross-check results including PASSED, all eight quality categories, append schema normalization, UTC timestamps and schema hash |
| Reconciliation | Every Bronze row is counted exactly once as accepted or rejected; duplicate-key and parent-rejection subsets are not double-counted; multiplicity is preserved; unexplained differences fail; results and diagnostics are persisted |
| Publication/views | Successful preflight followed by a targeted reset of exactly five views, 13 Silver tables, and 13 reject tables for only the selected profile; missing targets on first run; no reset when preflight fails; unrelated objects and operations history preserved; subsequent run removes outputs of earlier success/failure before building; failure during reset or between table publications preserves remaining outputs and evidence; no uncontrolled append or stale business rows after successful completion; complete source-attribute projections, exactly 13 accepted tables, exactly 13 reject tables, all five views, renamed-column compatibility, null active flags excluded, empty roster, duplicate-side join fanout |
| Finalization | 13 successful builds/reconciliations, exact accepted/reject table coverage, missing/duplicated evidence, CRITICAL vs ERROR vs WARNING without quality-only execution failure, cross-check/view task failure, missing run ID, repeated finalization, failed count read, preserved evidence after failure, approved gate policy |
| Bundle/runtime | YAML parse, include/sync coverage, ten tasks/scripts, timeouts/environment, release/run parameters, complete finalizer dependencies/ALL_DONE, imports/config resolution from deployed path, Spark SQL execution under actual ANSI/timezone settings |

Use the repository's configured Python environment and test paths. Run configured lint/format checks if present; do not introduce a new lint stack for this assignment. Add real Spark/Delta tests that execute every accepted/reject query on toy data, publish tables in a disposable team-authorized schema, query all views, and verify counts, metadata, lineage, failure behavior, and reruns. Do not delete unrelated schemas or data in cleanup.

Staged acceptance:

1. Configuration, source-to-Silver mappings, naming contracts, and source contracts pass local tests before data writes.
2. Foundational tables pass before athletes; athletes before organization/partnership; these before competition.
3. A complete synthetic integration run produces exactly 13 accepted tables, 13 reject tables (possibly empty), six operations tables, five views, and run-linked quality/reconciliation evidence; the specified tables are full-refresh replacements rather than appends.
4. A 5K clean run and rerun demonstrate that the next run resets the selected Silver/reject tables and views after successful preflight, rebuilds stable business outputs, and preserves prior operational audit history. Also test a preflight failure that leaves existing outputs intact and a post-reset build failure that preserves partial outputs until the following run clears them. Run IDs/load timestamps naturally differ; do not demand byte-identical audit data.
5. 5K tainted and 50K clean/tainted runs validate defect handling and required quality coverage with the same code. 250K production is the production-scale case dataset, not evidence that Gold analytics have been performed. Performance optimization is outside the required student scope.
6. Do not claim all acceptance requirements passed until each required check has evidence or a clearly stated limitation. No unexplained reconciliation difference, stale Silver/reject table, or hidden partial refresh is acceptable as a successful handoff; a failed run preserves available Silver, exception, quality, and operations evidence, is marked incomplete/unsuitable for Gold, and requires a subsequent successful full-job rerun.

Final deliverables: source/config/task files; validated bundle; synthetic fixtures and tests; source-to-target/type/rule matrices; decision/gap log; deployment and recovery runbook; run evidence when execution is authorized; concise summary of checks actually run versus not run. Do not fabricate successful deployment, live table counts, or dataset findings.

### Appendix A — Published source contract for schema 1.6

Use this appendix to validate the source and enumerate every business attribute that must be mapped into accepted Silver. The delivered manifest and Parquet schemas provide the final ordered physical evidence; inspect actual precision, scale, and nullability rather than guessing. Source IDs and counts are INT64; dates/timestamps and UUIDs are published as strings; numeric measurement fields use DECIMAL; `club_memberships.is_primary` is BOOLEAN. Source country examples US/CA are not Silver's standardized USA/CAN.

Ordered source columns:

```text
clubs:
  id, club_name, region_id, club_type, competitiveness_level, member_capacity,
  founding_date, indoor_court_count, outdoor_court_count
club_memberships:
  id, player_id, club_id, membership_type, joined_date, left_date, is_primary
match_games:
  id, match_id, game_number, team_one_score, team_two_score,
  winning_team_number, target_score, win_by, actual_team_one_score_share
match_team_players:
  id, match_team_id, player_id, player_position, player_rating_at_match
match_teams:
  id, match_id, team_number, team_id, team_score, average_team_rating
matches:
  id, match_date, region_id, match_type, court_type, match_format,
  winning_team_id, total_points_played, batch_id
monthly_batches:
  id, batch_month, batch_sequence, batch_type, active_player_count_start,
  new_player_count, active_player_count_end, match_count_generated,
  rating_update_count, assessment_update_count
player_assessment_history:
  id, player_id, assessment_date, assessment_type, assessment_value,
  confidence_score, derived_from_matches, batch_id
player_master:
  player_id, external_player_key, first_name, last_name, gender, birth_date,
  dominant_hand, home_region_id, country_code, registration_date,
  player_status, rating, rating_value, confidence_score, volatility_score,
  global_percentile, match_count_used, rating_date, rating_batch_id,
  snapshot_month
player_registrations:
  id, player_id, batch_id, registration_month, registration_source,
  assigned_region_id, initial_rating_value, initial_confidence_score
regions:
  id, country_code, region_type, region_name, state_province_code,
  population, latitude, longitude
team_memberships:
  id, team_id, player_id, player_position, joined_date, left_date
teams:
  id, team_type, team_division, team_status, country_code,
  formation_date, dissolution_date
```

Required nonnull source fields (all other fields are nullable under this source contract):

| Source | Required fields |
|---|---|
| clubs | id, club_name, region_id |
| club_memberships | id, player_id, club_id, joined_date |
| match_games | id, match_id, game_number, team_one_score, team_two_score, winning_team_number |
| match_team_players | id, match_team_id, player_id |
| match_teams | id, match_id, team_number, team_id, team_score |
| matches | id, match_date, match_type, batch_id |
| monthly_batches | id, batch_month, batch_sequence, batch_type |
| player_assessment_history | id, player_id, assessment_date, assessment_type, batch_id |
| player_master | player_id, external_player_key, first_name, last_name, birth_date, registration_date, player_status, snapshot_month |
| player_registrations | id, player_id, batch_id, registration_month |
| regions | id, country_code, region_name |
| team_memberships | id, team_id, player_id, player_position, joined_date |
| teams | id, team_type, team_division, team_status, formation_date |

Decimal scale expectations: score share 4; player/assessment/team ratings and confidence/volatility measurements 3; global percentile 2; region latitude/longitude 6. Do not infer unprovided precision or numeric bounds. `rating` is a compatibility alias of `rating_value`; `external_player_key` is UUID text. No standalone `players.parquet` or `player_rating_history.parquet` is published.

### Appendix B — Accepted Silver projections

The ordered business columns below precede the seven shared metadata columns from Stage 4. These projections are attribute-complete: every published Bronze business attribute appears directly or through a documented canonical rename, standardization, or type conversion. Derived business attributes use `derived_`; data-quality indicators use `dq_`; surrogate keys use `_sk`; operational metadata uses leading underscores. IDs/surrogate keys are STRING; typed dates DATE; flags BOOLEAN; ratings/confidence/share DOUBLE; date parts/sequences/scores/count-derived fields use the corresponding SQL integer types (window row numbers may be BIGINT). Verify exact Spark types/nullability with integration schema tests rather than inferring them solely from Python fixtures.

```text
monthly_batches:
  batch_id, batch_sk, batch_month, batch_sequence, batch_type,
  active_player_count_start, new_player_count, active_player_count_end,
  match_count_generated, rating_update_count, assessment_update_count,
  derived_batch_date, derived_batch_year, derived_batch_month,
  derived_batch_quarter
regions:
  region_id, region_sk, country_code, region_type, region_name,
  state_province_code, province_state, population, latitude, longitude
players:
  player_id, player_sk, external_player_key, first_name, last_name,
  display_name, gender, birth_date, dominant_hand, home_region_id,
  home_region_sk, country_code, registration_date, player_status, rating,
  rating_value, confidence_score, rating_confidence, volatility_score,
  global_percentile, match_count_used, rating_date, rating_batch_id,
  snapshot_month, derived_active_flag, derived_age_at_snapshot,
  derived_age_group_at_snapshot
player_registrations:
  registration_id, registration_sk, player_id, player_sk, batch_id, batch_sk,
  registration_month, registration_source, assigned_region_id,
  assigned_region_sk, initial_rating_value, initial_confidence_score,
  registration_date, registration_type, effective_start_date,
  effective_end_date, derived_registration_status,
  derived_current_registration_flag, derived_registration_duration_days,
  derived_registration_sequence
player_assessment_history:
  assessment_id, assessment_sk, player_id, player_sk, batch_id, batch_sk,
  assessment_date, assessment_type, assessment_value, confidence_score,
  assessment_confidence, derived_from_matches
clubs:
  club_id, club_sk, club_name, region_id, region_sk, country_code,
  club_type, competitiveness_level, member_capacity, founding_date,
  indoor_court_count, outdoor_court_count, open_date, close_date
teams:
  team_id, team_sk, team_type, team_division, team_category,
  team_identity_type, country_code, team_status, formation_date,
  dissolution_date, derived_active_flag, derived_team_age_at_snapshot_days
club_memberships:
  club_membership_id, club_membership_sk, player_id, player_sk,
  club_id, club_sk, membership_type, joined_date, left_date, is_primary,
  membership_start_date, membership_end_date,
  derived_membership_duration_days, derived_current_membership_flag,
  dq_membership_overlap_flag
team_memberships:
  team_membership_id, team_membership_sk, team_id, team_sk, player_id,
  player_sk, player_position, joined_date, left_date, player_role,
  membership_start_date, membership_end_date,
  derived_membership_duration_days, derived_current_membership_flag,
  dq_membership_overlap_flag
matches:
  match_id, match_sk, batch_id, batch_sk, region_id, region_sk, match_date,
  match_type, court_type, match_format, total_points_played,
  competition_category, winning_team_id, derived_winning_team_number,
  derived_completed_flag, derived_match_year, derived_match_month
match_teams:
  match_team_id, match_team_sk, match_id, match_sk, team_id, team_sk,
  team_number, team_score, average_team_rating, match_date,
  pre_match_team_rating, derived_winner_flag, derived_post_match_team_rating,
  derived_rating_change, dq_side_cardinality_warning_flag
match_team_players:
  match_team_player_id, match_team_player_sk, match_team_id, match_team_sk,
  match_id, match_sk, team_id, team_sk, player_id, player_sk,
  player_position, player_rating_at_match, derived_age_at_match,
  derived_age_group_at_match, match_date,
  dq_membership_history_warning_flag
match_games:
  match_game_id, match_game_sk, match_id, match_sk, game_number,
  team_one_score, team_two_score, winning_team_number, target_score, win_by,
  actual_team_one_score_share, derived_score_margin, derived_total_points,
  derived_close_game_flag, derived_extended_game_flag
```

The mapping matrix must accompany these projections and identify the source origin of every column. A canonical Silver name does not authorize dropping original information: preserve both `rating` and `rating_value`, preserve `team_type` separately from `team_division`, preserve `derived_from_matches` as a source attribute, and retain source lifecycle, activity-count, score, rating, location, and membership attributes even when no current Silver rule uses them. Reject tables continue to preserve rejected source occurrences and diagnostic evidence through the established exception-table contract; rejected rows do not need to appear in accepted Silver.

### Appendix C — Implementation risks and required safeguards

The requirements in this prompt govern duplicate handling, reconciliation accounting, quality-versus-execution status, and failure preservation. Implement and test them directly. These are safeguards for construction, not a list of defects in an inaccessible existing implementation or a request to recreate old code.

| Implementation risk | Required handling in the student build |
|---|---|
| Manifest/package evidence cannot be established from table existence alone | Raw-to-Bronze validates the colocated manifest for the selected profile. Reuse verified run-scoped evidence when available; otherwise read the colocated manifest/provenance location without rebuilding Bronze |
| Required null/type/FK checks and complete attribute mappings | Check the Appendix A source contract on Bronze; include registration assigned region and required batch references; maintain a complete source-to-Silver mapping |
| As-of calculations can use maximum batch date instead of the manifest boundary | Derive `snapshot_as_of_date` from `snapshot_end_exclusive`; use it for current-state calculations and use actual event dates or batch dates for their respective temporal contexts |
| `state_province_code` mapping can lose the original source attribute | Preserve `state_province_code` and document any canonical `province_state` mapping separately |
| Duplicate-key records can be double-counted in reconciliation | Reject every occurrence of a duplicated non-null primary key and reconcile `Bronze = accepted + rejected`; keep duplicate and parent-rejection counts as diagnostic subsets, not additive categories |
| Runtime conversion behavior can vary by expression and Databricks runtime | Validate the selected non-throwing numeric/date/timestamp/boolean expressions in the actual serverless environment and document supported runtime behavior |
| Ordinary `CAST`/`TO_DATE` expressions can abort a transformation or confuse failed conversion with a legitimate null | Use runtime-validated non-throwing conversion expressions; distinguish optional source nulls, missing required values, malformed non-null values, and post-conversion business-rule violations; preserve original offending values and reject only the affected records |
| A fresh run can inherit stale outputs, while failure cleanup can erase evidence | Only after successful preflight, drop the five designated views, 13 Silver tables, and 13 exception tables for the selected profile before rebuilding. Never drop schemas, unrelated objects, or operations history; preserve available outputs and evidence after any technical failure until a later run completes preflight and resets them |
| Quality findings can be confused with execution failure | Record CRITICAL/ERROR/WARNING findings without failing execution solely for quality; missing evidence, failed tasks, publication failures, and unexplained reconciliation differences still fail finalization |
| Missing membership history and nested interval overlaps | Report missing history separately from known contradictory history; test nested overlaps and document any interval-warning limitation without inventing membership data |
| Two rows do not prove distinct sides/teams; game-number and score integrity | Check distinct sides/teams and game-number uniqueness; report anomalies under the existing rules without inventing tournament thresholds |
| Narrow source projections or partial hashes can omit attributes | Preserve every Bronze business attribute in accepted Silver, keep source attributes distinguishable, and ensure record hashes/mappings document the selected field coverage without discarding source information |
| Local mocks are not proof of live Spark execution | Test SQL/DataFrame transformations in a real Spark/Delta integration environment and verify actual Databricks deployment before claiming runtime success |
| Count queries or publication evidence fail | Propagate the failure and retain diagnostics; never interpret an unavailable count as zero |
| Repeated-task evidence does not establish distinct table coverage | Test duplicate evidence and whole-job reruns; preserve evidence after failure and require a subsequent successful full-job run for a complete Silver release |

## End prompt

---

