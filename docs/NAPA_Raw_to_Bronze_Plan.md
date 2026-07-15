# NAPA Raw to Bronze Pipeline Construction Plan

**Purpose:** This document converts `docs/NAPA_Raw_to_Bronze_Spec.md` into a staged construction plan for rebuilding the Raw-to-Bronze Databricks pipeline from scratch using the defined architecture.

**Source specification:** `docs/NAPA_Raw_to_Bronze_Spec.md`  
**Target platform:** Databricks Free Edition, Unity Catalog, Delta Lake, PySpark  
**Pipeline mode:** Configuration-driven full refresh  
**Primary output:** Release-specific Bronze Delta tables, source metadata, schema snapshots, reconciliation evidence, and operations records

---

## 1. Planning Boundary

The Raw-to-Bronze pipeline will ingest delivered Raw Parquet files into managed Bronze Delta tables while preserving source business values exactly as delivered. It will add only operational metadata and audit evidence.

This plan intentionally excludes Silver cleansing, domain normalization, duplicate removal, referential-integrity checks, feature engineering, Gold analytics, model development, dashboards, and roster recommendations.

Because development is restarting from scratch, the new implementation should treat the existing ad hoc Bronze notebook work as disposable implementation detail. Existing Databricks objects should not be deleted immediately; they should be retained until the new release-specific pipeline has been validated.

---

## 2. Target Architecture

The pipeline will create and operate three separately materialized release instances with one shared codebase and one shared Databricks pipeline workflow.

| Release | Raw Schema | Raw Volume | Bronze Schema |
|---|---|---|---|
| `napa_5k` | `workspace.instructor_5k_raw` | `napa_files` | `workspace.instructor_5k_bronze` |
| `napa_50k` | `workspace.instructor_50k_raw` | `napa_files` | `workspace.instructor_50k_bronze` |
| `napa_250k` | `workspace.instructor_250k_raw` | `napa_files` | `workspace.instructor_250k_bronze` |

All releases share:

- one Git repository;
- one Raw-to-Bronze codebase;
- one source registry;
- one source contract set;
- one Databricks pipeline workflow;
- one operations schema: `workspace.instructor_ops`.

Raw files live directly under the release-specific `napa_files` Volume. A nested folder such as `napa_files/napa_5k/` is not part of the target design because the release is represented by the schema and environment configuration.

---

## 3. Construction Sequence

Build the pipeline in stages so each stage can be reviewed and tested before moving to the next.

| Stage | Focus | Primary Outcome |
|---|---|---|
| 0 | Restart baseline and migration decision | Current ad hoc Bronze assumptions are isolated from the new build |
| 1 | Configuration model | Release-specific YAML config and shared source registry |
| 2 | Catalog, schema, and Volume setup | Raw, Bronze, and operations namespaces can be created or validated |
| 3 | Operations foundation | Durable run, table, schema, reconciliation, and message logs |
| 4 | Raw inventory and source contract validation | Exact file inventory and Parquet schema checks run before ingestion |
| 5 | Bronze ingestion framework | Reusable full-refresh ingestion and metadata enrichment functions |
| 6 | Delta publication and reconciliation | Staging, final table replacement, row counts, and schema evidence |
| 7 | Databricks pipeline workflow | One parameterized workflow processes all releases |
| 8 | Testing and failure handling | Unit, integration, full-refresh, and failure-publication tests |
| 9 | Release acceptance | 5K, 50K, and 250K acceptance evidence is produced |
| 10 | Documentation and migration closeout | Runbook, architecture, lineage, and migration status are updated |

---

## 4. Stage 0 - Restart Baseline and Migration Decision

**Objective:** Start the Bronze rebuild cleanly without accidentally depending on the earlier instructor schema layout.

**Tasks:**
- Confirm branch, remote, and working-tree status.
- Inventory current repo files that were created during earlier Bronze experimentation.
- Decide whether to replace, archive, or ignore existing notebooks such as `01_setup_catalog.py` and `01_ingest_raw_to_bronze.py`.
- Record the target migration from `workspace.instructor_raw` and `workspace.instructor_bronze` to release-specific schemas.
- Confirm that current Databricks objects are retained until the new pipeline passes acceptance.
- Identify whether the implementation remains instructor-only or whether any components will later be adapted for students.

**Exit criteria:**
- The restart boundary is documented.
- No new implementation depends on `workspace.instructor_raw` or `workspace.instructor_bronze` as the default target architecture.
- Existing Databricks objects have not been destructively changed.

---

## 5. Stage 1 - Configuration Model

**Objective:** Build the configuration layer that controls all release-specific behavior.

**Files to add or revise:**
- `config/raw_to_bronze/base.yml`
- `config/raw_to_bronze/environments/napa_5k.yml`
- `config/raw_to_bronze/environments/napa_50k.yml`
- `config/raw_to_bronze/environments/napa_250k.yml`
- `config/raw_to_bronze/raw_sources.yml`
- `config/raw_to_bronze/logging.yml`
- `src/napa_pipeline/config.py` or a dedicated raw-to-bronze config module
- `tests/test_raw_to_bronze_config.py`

**Configuration responsibilities:**
- Active `release_name`.
- Catalog, Raw schema, Bronze schema, and operations schema.
- Raw Volume name and path.
- Source inventory and authoritative filenames.
- Source-to-Bronze table mappings.
- Build order.
- Key columns.
- Expected source schema contracts.
- Metadata settings.
- Publication settings.
- Unexpected-file policy.
- Schema-drift policy.
- Performance settings.

**Tasks:**
- Implement YAML loading, deep merge, placeholder resolution, release validation, and configuration hash calculation.
- Reject unsupported releases, unresolved placeholders, duplicate build-order values, undefined source references, and missing required keys.
- Add all thirteen source files to `raw_sources.yml`.
- Keep the three environment files limited to release-specific schemas, Volume paths, role, scale, and performance settings.
- Avoid hard-coded release names, schema names, table names, or source paths in notebooks.

**Exit criteria:**
- A resolved configuration can be produced for `napa_5k`, `napa_50k`, and `napa_250k`.
- Unit tests cover path construction, target name construction, source ordering, placeholder resolution, and invalid release handling.

---

## 6. Stage 2 - Catalog, Schema, and Volume Setup

**Objective:** Create or validate the required Unity Catalog objects for all releases.

**Objects:**
- `workspace.instructor_5k_raw`
- `workspace.instructor_50k_raw`
- `workspace.instructor_250k_raw`
- `workspace.instructor_5k_bronze`
- `workspace.instructor_50k_bronze`
- `workspace.instructor_250k_bronze`
- `workspace.instructor_ops`
- `napa_files` Volume under each Raw schema

**Tasks:**
- Implement setup code that can create schemas and Volumes when authorized.
- Implement validation code that confirms catalog access and required permissions.
- Print and log whether each object already existed or was created.
- Ensure the pipeline never writes transformed data into Raw Volumes.
- Keep object names fully configuration-driven.

**Exit criteria:**
- All required schemas and Volumes exist or fail with clear permission diagnostics.
- Setup is idempotent and safe to rerun.
- Raw Volume paths match the release-specific environment configuration.

---

## 7. Stage 3 - Operations Foundation

**Objective:** Build durable audit tables before ingestion logic so every run is observable.

**Operations tables:**
- `pipeline_runs`
- `table_runs`
- `schema_snapshots`
- `reconciliation_results`
- `run_messages`

**Tasks:**
- Define Delta schemas or DDL for all operations tables.
- Implement helpers to write pipeline start, pipeline success, pipeline failure, table start, table success, and table failure records.
- Include `release_name`, `pipeline_name`, `pipeline_run_id`, source name, source file, target table, status, timestamps, elapsed time, row counts, and error details where applicable.
- Implement message codes such as `CONFIG_LOADED`, `ENVIRONMENT_VALIDATED`, `RAW_FILE_MISSING`, `RAW_FILE_UNEXPECTED`, `SOURCE_SCHEMA_MISMATCH`, `SOURCE_READ_STARTED`, `BRONZE_WRITE_COMPLETED`, `ROW_COUNT_MISMATCH`, `PIPELINE_SUCCEEDED`, and `PIPELINE_FAILED`.
- Ensure failures are recorded before exceptions are re-raised.

**Exit criteria:**
- A synthetic run can write success and failure records.
- Operations tables distinguish Raw-to-Bronze runs from later Bronze-to-Silver runs using `pipeline_name = 'raw_to_bronze'`.

---

## 8. Stage 4 - Raw Inventory and Source Contract Validation

**Objective:** Validate Raw files before any Bronze table is published.

**Required source files:**
- `regions.parquet`
- `clubs.parquet`
- `club_memberships.parquet`
- `player_master.parquet`
- `player_registrations.parquet`
- `player_assessment_history.parquet`
- `teams.parquet`
- `team_memberships.parquet`
- `matches.parquet`
- `match_teams.parquet`
- `match_team_players.parquet`
- `match_games.parquet`
- `monthly_batches.parquet`

**Tasks:**
- Validate the exact Raw file inventory for the selected release.
- Fail if any configured required file is missing.
- Fail on unexpected files by default, including backups, old versions, text files, and OS artifacts.
- Capture file metadata, including path, size where available, and modification timestamp where available.
- Read Parquet schema for each file and compare it to the configured source contract.
- Fail on reserved metadata-column collisions, missing required columns, incompatible type changes, and unexpected columns unless explicitly configured otherwise.
- Capture schema snapshots before ingestion.

**Exit criteria:**
- No Bronze table build starts unless inventory and contract validation pass for the selected source.
- Schema differences are logged to operations tables with actionable diagnostics.

---

## 9. Stage 5 - Bronze Ingestion Framework

**Objective:** Implement reusable ingestion logic that preserves Raw business data and adds only Bronze metadata.

**Recommended modules:**
- `src/napa_pipeline/raw_to_bronze/config.py`
- `src/napa_pipeline/raw_to_bronze/context.py`
- `src/napa_pipeline/raw_to_bronze/environment.py`
- `src/napa_pipeline/raw_to_bronze/inventory.py`
- `src/napa_pipeline/raw_to_bronze/contracts.py`
- `src/napa_pipeline/raw_to_bronze/ingest.py`
- `src/napa_pipeline/raw_to_bronze/metadata.py`
- `src/napa_pipeline/raw_to_bronze/publish.py`
- `src/napa_pipeline/raw_to_bronze/reconcile.py`
- `src/napa_pipeline/raw_to_bronze/operations.py`
- `src/napa_pipeline/raw_to_bronze/exceptions.py`

**Bronze metadata columns:**
- `_pipeline_run_id`
- `_pipeline_name`
- `_pipeline_version`
- `_release_name`
- `_source_file_name`
- `_source_file_path`
- `_source_file_size`
- `_source_file_modification_ts`
- `_ingested_ts`
- `_source_record_hash`

**Tasks:**
- Read each configured Parquet file from the release-specific Volume path.
- Preserve all source business columns, names, values, row grain, duplicates, nulls, invalid domains, and orphan relationships.
- Add only the configured Bronze metadata columns.
- Generate `_source_record_hash` from all original source columns using deterministic column ordering, consistent null representation, and SHA-256.
- Preserve duplicate rows as duplicate hashes.
- Fail if source data contains reserved metadata column names.
- Avoid joins, trimming, standardization, deduplication, imputation, domain mapping, or analytical derivation.

**Exit criteria:**
- A single valid fixture file can be ingested into Bronze with row preservation, metadata, and deterministic hashes.
- Duplicate and null source records are preserved.

---

## 10. Stage 6 - Delta Publication and Reconciliation

**Objective:** Publish managed Bronze Delta tables through a full-refresh pattern with reconciliation evidence.

**Tasks:**
- Construct target names from configuration using `catalog.bronze_schema.bronze_table`.
- Write to a run-scoped staging table where practical.
- Validate staging row count and schema.
- Replace the final managed Delta table only after staging validation passes.
- Use `format("delta")`, `mode("overwrite")`, and `overwriteSchema = true`.
- Add table comments and table properties where supported.
- Do not partition Bronze tables by default.
- Keep `OPTIMIZE` and `VACUUM` configuration-controlled and disabled by default.
- Record Raw row count, Bronze row count, count match status, source schema hash, and Bronze schema hash.
- Remove staging objects after successful publication.

**Exit criteria:**
- Each source publishes to exactly one managed Bronze Delta table.
- Raw and Bronze row counts match.
- Existing valid Bronze targets are not replaced by failed staging output.
- Full-refresh reruns do not append duplicate records.

---

## 11. Stage 7 - Databricks Pipeline Workflow

**Objective:** Build one reusable Databricks pipeline workflow for Raw-to-Bronze.

**Workflow name:**
- `NAPA Raw to Bronze`

**Required parameter:**
- `release_name`, with allowed values `napa_5k`, `napa_50k`, `napa_250k`

**Pipeline task graph:**
- `01_resolve_configuration`
- `02_validate_release_environment`
- `03_validate_raw_inventory`
- `04_build_bronze`
- `05_validate_bronze`
- `06_publish_ingestion_summary`

**Databricks construction tasks:**
- Create thin notebook entry points for each task.
- Keep reusable logic in Python modules.
- Pass `release_name` from the job parameter into the configuration resolver.
- Use task values, run parameters, or operations records to pass `pipeline_run_id`, resolved config path, and configuration hash between tasks.
- Configure task dependencies so Bronze build cannot run before environment and Raw inventory validation.
- Configure retries only for transient infrastructure failures.
- Ensure deterministic data or schema failures are not retried as if they were transient.
- Store the workflow definition in source control if export as YAML, JSON, or bundle configuration is available.

**Exit criteria:**
- One Databricks pipeline workflow can process all three releases by changing only `release_name`.
- The Databricks run page shows the complete task graph, dependencies, parameters, retries, and run history.
- The repo contains enough workflow definition or runbook detail to recreate the workflow in a clean workspace.

---

## 12. Stage 8 - Testing and Failure Handling

**Objective:** Prove core behavior before running large release data.

**Unit tests:**
- YAML loading.
- Deep merge.
- Placeholder resolution.
- Release-name validation.
- Path construction.
- Target-name construction.
- Source registry ordering.
- Schema hash calculation.
- Source record hash calculation.
- Metadata-column addition.
- Reconciliation arithmetic.
- Exception formatting.

**Integration tests:**
- Create a temporary test Raw schema and Volume or an approved test equivalent.
- Upload small Parquet fixtures.
- Run source validation and Bronze build.
- Verify row preservation, schema preservation, metadata columns, row-count reconciliation, and rerun idempotence.
- Cover valid source, missing file, unexpected file, unreadable file, empty file, missing required column, incompatible type, unexpected column, duplicate rows, and null business values.

**Failure-publication tests:**
- Force a schema mismatch.
- Confirm the target table is not published.
- Confirm any existing valid Bronze target is not replaced.
- Confirm table run and pipeline run statuses are failed.
- Confirm root exception details are recorded.

**Exit criteria:**
- Unit tests pass locally.
- Integration tests pass in Databricks or the chosen Spark test environment.
- Failure behavior matches the spec.

---

## 13. Stage 9 - Release Acceptance

**Objective:** Validate the new pipeline against each release in order.

**5K acceptance:**
- All thirteen files are present.
- All thirteen source contracts pass.
- All thirteen Bronze tables are created.
- Raw and Bronze row counts match.
- Metadata columns are present.
- Rerun succeeds without appending duplicates.
- Operations records are complete.

**50K acceptance:**
- Only release configuration changes.
- Same source inventory and table names.
- Same source contracts.
- Successful full refresh.
- Performance metrics recorded.
- No code changes from 5K.

**250K acceptance:**
- Only release configuration changes.
- Complete full refresh.
- No full-data driver collection.
- All row-count reconciliations pass.
- All schema contracts pass.
- All Bronze tables query successfully.
- Operations summary complete.

**Cross-release acceptance:**
- Same thirteen Bronze table names.
- Compatible business schemas.
- Same metadata columns.
- Same pipeline version.
- Same source contracts.
- Separate namespaces.
- Increasing row volumes where expected.

---

## 14. Stage 10 - Documentation and Migration Closeout

**Objective:** Make the rebuild reproducible and retire old assumptions only after validation.

**Documentation updates:**
- `docs/architecture.md`: Raw-to-Bronze architecture and workflow task graph.
- `docs/medallion_design.md`: release-specific Raw and Bronze responsibilities.
- `docs/data_dictionary.md`: Bronze table inventory and metadata columns.
- `docs/lineage.md`: Raw file to Bronze table mapping.
- `docs/runbook.md`: Raw file loading, workflow execution, failure investigation, reviewer queries.
- `docs/data_quality_report.md`: ingestion and reconciliation evidence.
- `docs/NAPA_Bronze_to_Silver_Plan.md`: confirm Silver plan consumes the new release-specific Bronze schemas.

**Migration tasks:**
- Load files into release-specific Raw Volumes.
- Run `napa_5k`, then `napa_50k`, then `napa_250k`.
- Compare results against current instructor schemas where useful.
- Mark the old current structure as superseded only after release-specific acceptance passes.
- Do not delete old objects until instructor approval.

**Exit criteria:**
- The repository documents how to recreate the Raw-to-Bronze pipeline from scratch.
- Old schema assumptions are removed from active Bronze pipeline code and runbook steps.
- Migration status is explicit.

---

## 15. Key Risks and Required Decisions

| Risk or Decision | Required Handling |
|---|---|
| Current repo has ad hoc Bronze notebooks | Restart implementation around release-specific config and workflow tasks |
| Existing Databricks objects use `instructor_raw` and `instructor_bronze` | Keep them until validation, but do not use them as target architecture |
| Raw files may still contain native Parquet UUID types | Prefer upstream UUID-as-string export; keep ingestion compatibility decisions documented |
| Source contracts require actual schema inspection | Do not invent columns; inspect delivered Parquet schemas before final contracts |
| Unexpected files may appear in Raw Volumes | Fail by default unless instructor changes policy through configuration |
| Full-refresh overwrite can replace valid data if staging is skipped | Use staging and publish final tables only after validation |
| Large releases may expose driver-side processing | Avoid pandas, full `collect()`, and full-data driver operations |
| Workflow definition may be manual in Free Edition | Store enough workflow definition or runbook detail to recreate the pipeline reliably |

---

## 16. Immediate Next Implementation Steps

1. Freeze the current ad hoc Bronze work as historical context and stop extending it.
2. Create the `config/raw_to_bronze/` configuration file set.
3. Define the Databricks `NAPA Raw to Bronze` pipeline workflow task graph.
4. Build the configuration loader and source registry validation.
5. Build setup and validation logic for release-specific schemas and Raw Volumes.
6. Build operations tables and logging helpers before ingestion.
7. Implement one end-to-end source ingestion using a small fixture.
8. Generalize ingestion across all thirteen configured sources.
9. Validate `napa_5k` first, then scale to `napa_50k` and `napa_250k`.
