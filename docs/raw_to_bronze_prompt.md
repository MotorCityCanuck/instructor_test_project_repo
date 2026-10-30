# Student AI Build Prompt: NAPA Raw-to-Bronze Pipeline

**Purpose:** Give this complete construction specification to your AI coding assistant to build a configuration-driven NAPA Raw-to-Bronze pipeline. You and your team remain responsible for reviewing the generated implementation, running it, validating the results, and explaining the decisions. The specification is standalone; no completed pipeline or unpublished repository is required.

**Student review:** Read the requirements before construction. You may propose and document justified changes, but do not silently weaken the source-preservation, manifest-validation, namespace-safety, or acceptance requirements.

> ## Task
>
> You are working in a Databricks/PySpark repository for the NAPA Olympic Analytics Platform. Build a configuration-driven **Raw-to-Bronze** pipeline from the supplied Raw Parquet files using the complete construction contract below. The repository may be a clean student template with no existing pipeline code. Inspect it before changing it, preserve its existing structure where reasonable, make focused changes, and do not invent data fields, source schemas, row counts, or results.
>
> Your deliverable must satisfy the following functional contract. Do not seek or assume access to an existing completed pipeline. If a repository convention conflicts with this contract, stop, identify the conflict, and explain it before choosing an approach.
>
> ### Scope and boundaries
>
> Include only Raw-to-Bronze responsibilities:
>
> - provision or validate release-specific Unity Catalog schemas and Raw Volumes;
> - validate the colocated `manifest.json`, delivered Raw-file inventory, and Parquet readability;
> - destructively refresh the selected Bronze schema, then copy each source into a managed Bronze Delta table with operational metadata appended;
> - record operational audit evidence and schema snapshots;
> - reconcile manifest, Raw, and Bronze row counts and column contracts;
> - provide a Databricks Asset Bundle job plus local unit tests.
>
> Explicitly exclude Silver cleansing, source-field renaming, type standardization, business-rule validation, deduplication, referential-integrity enforcement, feature engineering, analytics, models, roster recommendations, streaming, Auto Loader, CDC, and incremental `MERGE` into Bronze data tables. Delta `MERGE` is used to finalize the operations run record. Bronze must preserve duplicates, nulls, invalid business values, and relationships exactly as delivered. Every ingest run is a destructive full refresh of the selected profile's dedicated Bronze schema; it must not preserve tables or rows from prior runs or failed partial runs.
>
> Do not commit source data, generated Delta storage, credentials, tokens, or personal workspace paths. Do not hard-code Windows paths, a cluster ID, or a personal Databricks path.
>
> ### First actions: inspect and report before editing
>
> 1. Read `AGENTS.md` if present, the root `README.md`, and relevant documentation, bundle configuration, notebooks, source modules, and tests that actually exist in the student repository. Missing optional starter files are not a reason to stop construction.
> 2. Run `git status --short`, `git branch --show-current`, and `git remote -v`. Report unrelated existing changes before editing them.
> 3. Identify whether the repository already has an equivalent configuration/package/test convention. Reuse it rather than introducing a framework or dependency unnecessarily.
> 4. State the files you will create or change, the assumptions that depend on the delivered data, and the validation plan. Then implement.
>
> ### Required runtime design
>
> Build one shared implementation supporting these five dataset profiles:
>
> | Dataset profile / release name | Role | Data condition | Raw schema | Bronze schema | Raw Volume path |
> |---|---|---|---|---|---|
> | `napa_5k_clean` | development | clean | `napa_platform_5k_clean_raw` | `napa_platform_5k_clean_bronze` | `/Volumes/workspace/napa_platform_5k_clean_raw/napa_files` |
> | `napa_5k_tainted` | development | tainted | `napa_platform_5k_tainted_raw` | `napa_platform_5k_tainted_bronze` | `/Volumes/workspace/napa_platform_5k_tainted_raw/napa_files` |
> | `napa_50k_clean` | validation | clean | `napa_platform_50k_clean_raw` | `napa_platform_50k_clean_bronze` | `/Volumes/workspace/napa_platform_50k_clean_raw/napa_files` |
> | `napa_50k_tainted` | validation | tainted | `napa_platform_50k_tainted_raw` | `napa_platform_50k_tainted_bronze` | `/Volumes/workspace/napa_platform_50k_tainted_raw/napa_files` |
> | `napa_250k_production` | production-scale case data | production | `napa_platform_250k_raw` | `napa_platform_250k_bronze` | `/Volumes/workspace/napa_platform_250k_raw/napa_files` |
>
> Use catalog `workspace` and the shared operations schema `napa_platform_ops` as defaults, but put those values in configuration rather than scattering them through code. The Raw Volume name is `napa_files`. The processing mode is `full_refresh`. Use the same code and source registry for all profiles; only the environment configuration may vary.
>
> The names above are illustrative defaults. Before running writes, resolve the team's assigned catalog and prefix in configuration. Keep the naming pattern `<team_prefix>_<scale>_<condition>_raw` and `<team_prefix>_<scale>_<condition>_bronze` for 5K/50K clean and tainted profiles; use `<team_prefix>_250k_raw` and `<team_prefix>_250k_bronze` for the single 250K production profile; and use shared `<team_prefix>_ops`. The five profiles are independent full-refresh datasets, not monthly increments; `monthly_batches.parquet` is an input table and does not drive incremental processing. Do not allow a clean or tainted selector for the 250K production dataset.
>
> Raw files and the matching `manifest.json` must already be placed directly in each configured Volume; the pipeline does not upload, generate, or copy them from a local directory. Do not add an extra release subdirectory. The manifest is part of the delivered Raw input, is not a Bronze source table, and must be read from the same directory as the Parquet files. Confirm authenticated Databricks CLI access, a usable catalog, serverless job access, and permissions to create/use the configured schemas, Volume, and tables. Environment setup does not create a catalog or grant privileges.
>
> The source registry must enable the following sources, in this exact build order. Each source's configured primary/natural key is descriptive metadata for downstream consumers; do **not** use it to deduplicate Bronze.
>
> | Order | Source name / Bronze table | Raw file | Key column(s) |
> |---:|---|---|---|
> | 10 | `regions` | `regions.parquet` | `id` |
> | 20 | `clubs` | `clubs.parquet` | `id` |
> | 30 | `club_memberships` | `club_memberships.parquet` | `id` |
> | 40 | `player_master` | `player_master.parquet` | `player_id` |
> | 50 | `player_registrations` | `player_registrations.parquet` | `id` |
> | 60 | `player_assessment_history` | `player_assessment_history.parquet` | `id` |
> | 70 | `teams` | `teams.parquet` | `id` |
> | 80 | `team_memberships` | `team_memberships.parquet` | `id` |
> | 90 | `matches` | `matches.parquet` | `id` |
> | 100 | `match_teams` | `match_teams.parquet` | `id` |
> | 110 | `match_team_players` | `match_team_players.parquet` | `id` |
> | 120 | `match_games` | `match_games.parquet` | `id` |
> | 130 | `monthly_batches` | `monthly_batches.parquet` | `id` |
>
> Do not fabricate field-level source schemas in YAML. The delivered Parquet schemas are the source of truth for Bronze. Capture their actual schema evidence at runtime.
>
> ### Required configuration
>
> Use the repository's equivalent of the following configuration layout (adapt paths only if the existing repository has a documented alternative):
>
> ```text
> config/raw_to_bronze/
>   base.yml
>   raw_sources.yml
>   logging.yml
>   environments/
>     napa_5k_clean.yml
>     napa_5k_tainted.yml
>     napa_50k_clean.yml
>     napa_50k_tainted.yml
>     napa_250k_production.yml
> ```
>
> Implement a configuration loader that:
>
> - accepts only `napa_5k_clean`, `napa_5k_tainted`, `napa_50k_clean`, `napa_50k_tainted`, and `napa_250k_production`;
> - deep-merges `base.yml`, the selected environment file, the common source registry, and logging configuration without mutating inputs;
> - resolves `${section.nested_key}` placeholders repeatedly until all are resolved, and fails on an unknown or unresolved placeholder;
> - validates required top-level sections (`project`, `runtime`, `objects`, `execution`, `publication`, `metadata`, `performance`, `release`, `schemas`, `volume`, `sources`, `logging`), matching selected/reported dataset profile, valid `release.data_condition` (`clean`, `tainted`, or `production`), disallows `clean`/`tainted` conditions for the 250K profile and disallows `production` for 5K/50K profiles, `full_refresh` processing mode, agreement between `volume.name` and `objects.raw_volume_name`, `volume.manifest_name == manifest.json`, valid `.parquet` source filenames, nonempty source mapping, nonempty `bronze_table` values, and unique integer build order across all configured sources;
> - fails on missing YAML files or non-mapping file content;
> - produces the configuration hash as SHA-256 of UTF-8 encoded `yaml.safe_dump(resolved_config, sort_keys=True)`;
> - exposes enabled sources in ascending build order.
>
> Configure these defaults:
>
> ```yaml
> project:
>   name: napa_olympic_analytics
>   pipeline_name: raw_to_bronze
>   pipeline_version: "1.0.0"
>   processing_mode: full_refresh
> runtime:
>   catalog: workspace
>   team_prefix: napa_platform
>   default_release: napa_5k_clean
>   timezone: America/New_York
> objects:
>   raw_volume_name: napa_files
>   operations_schema: ${runtime.team_prefix}_ops
> publication:
>   format: delta
>   mode: overwrite
>   overwrite_schema: true
> execution:
>   fail_fast: true
>   require_exact_source_inventory: true
>   unexpected_file_policy: fail
>   missing_file_policy: fail
> ```
>
> Complete the configuration with the following required values and documented behavior. Do not assume a flag implements functionality unless the construction contract explicitly specifies that behavior.
>
> | Section | Additional configuration values |
> |---|---|
> | `execution` | `unexpected_column_policy: fail`, `missing_required_column_policy: fail`, `type_mismatch_policy: fail`, `publish_only_after_validation: true`, `destructive_bronze_refresh: true`, `cleanup_bronze_on_failure: true`; manifest-matched readable zero-row inputs are allowed |
> | `publication` | `use_staging: false`; publish directly into the freshly recreated Bronze schema |
> | `metadata` | The ten metadata columns in Step 5 are mandatory, including `_pipeline_name`; define their fixed ordered list in configuration rather than exposing ineffective per-column switches |
> | `performance` | `cache_source_during_reconciliation: false`, `optimize_after_publish: false`, `vacuum_after_publish: false` |
> | `logging` | `default_level: INFO`; message codes `CONFIG_LOADED`, `ENVIRONMENT_VALIDATED`, `BRONZE_SCHEMA_RESET`, `BRONZE_SCHEMA_CLEANED`, `RAW_FILE_MISSING`, `RAW_FILE_UNEXPECTED`, `SOURCE_SCHEMA_MISMATCH`, `SOURCE_READ_STARTED`, `SOURCE_READ_COMPLETED`, `BRONZE_WRITE_STARTED`, `BRONZE_WRITE_COMPLETED`, `ROW_COUNT_MISMATCH`, `PIPELINE_SUCCEEDED`, `PIPELINE_FAILED` |
>
> Each environment file supplies `release.release_name` and `release.dataset_profile` (one of the five profiles above), `release.expected_player_scale` (5000/50000/250000), `release.data_condition` (`clean`, `tainted`, or `production`), `release.role` (`development`/`validation`/`production`), `schemas.raw`, `schemas.bronze`, `schemas.operations: ${objects.operations_schema}`, `volume.name: ${objects.raw_volume_name}`, `volume.manifest_name: manifest.json`, and `volume.path: /Volumes/${runtime.catalog}/${schemas.raw}/${objects.raw_volume_name}`. `performance.shuffle_partitions` is 16 for 5K, 64 for 50K, and `auto` for 250K. Each registry entry contains `enabled: true`, `file_name`, `bronze_table`, `build_order`, a descriptive `grain`, and `key_columns` from the source table above.
>
> Enforce the behavior explicitly described here. Readable zero-row data is permitted only when it agrees with the delivered manifest; readiness uses the manifest as its source contract, not an invented fixed field schema. Publication is direct, not staged, and all ten ordered metadata columns are always appended. Performance and optimization settings are informational unless explicitly implemented; optimization is not required for this assignment. Missing files fail validation, and unexpected inventory entries follow the configured inventory policy. Log the prescribed diagnostic events without presenting a logging-code list as an exhaustive restriction.
>
> ### Multi-step build plan
>
> Implement and validate each step before moving to the next.
>
> 1. **Configuration and package foundation.** Create a small reusable Python package, for example `src/napa_pipeline/raw_to_bronze/`, with focused modules for configuration, environment setup, inventory, Bronze publication, reconciliation, operations, finalization, and CLI helpers. Keep Databricks scripts/notebooks thin orchestration entrypoints.
>
> 2. **Environment validation and destructive Bronze reset.** Resolve a release environment containing catalog, Raw schema, Bronze schema, operations schema, Raw Volume name, and Raw Volume path. Query the catalog's schemas and the Raw schema's Volumes. Create missing Raw, Bronze, and operations schemas with `CREATE SCHEMA IF NOT EXISTS`; create the Raw Volume with `CREATE VOLUME IF NOT EXISTS catalog.raw_schema.napa_files`. Return whether each object existed or was created. If an object is missing and creation is disabled, or catalog/Volume listing fails, raise `EnvironmentValidationError`. Creation failures propagate their Spark exception. Disabling creation still permits validation of existing objects. After confirming that the resolved Bronze schema belongs to the selected dataset profile and is the configured dedicated target, perform the required destructive reset: drop the selected Bronze schema with `DROP SCHEMA IF EXISTS <catalog>.<bronze_schema> CASCADE`, then recreate it with `CREATE SCHEMA <catalog>.<bronze_schema>`. Do not drop or recreate the Raw schema, Raw Volume, operations schema, or any schema outside the selected profile. Do not proceed if the fully qualified target cannot be resolved safely. A reset of an existing Bronze schema intentionally removes all tables, views, data, and prior partial-run artifacts in that schema. Do not write transformed data into the Raw Volume.
>
> 3. **Run context and operations tables.** Generate one UUID `pipeline_run_id` per workflow run, then use it in every task and every audit record. Its context must include pipeline name/version, release, processing mode, configuration hash, and fully qualified operations schema. Create these managed Delta tables if absent:
>
> | Table | Required fields |
> |---|---|
> | `pipeline_runs` | run ID, pipeline name/version, release, mode, config hash, optional workflow/trigger IDs, status, start/end timestamps, duration, error class/message |
> | `table_runs` | run ID, pipeline/release, source file/table, target table, status, start/end/duration, file size, Raw/Bronze counts and difference, source/Bronze schema hashes, error |
> | `schema_snapshots` | run ID, pipeline/release, layer, object, column name/type/nullability/ordinal, schema hash, capture timestamp |
> | `reconciliation_results` | run ID, pipeline/release, source file, Bronze table, Raw/Bronze counts and difference, Raw/Bronze business-column counts, metadata count, status, evaluation timestamp |
> | `run_messages` | run ID, pipeline/release, optional source name, message level/code/text, timestamp |
>
> Record a `RUNNING` pipeline row once environment validation succeeds. Use UTC-aware timestamps and calculate duration from start and completion timestamps. Persist messages such as `CONFIG_LOADED`, `ENVIRONMENT_VALIDATED`, `BRONZE_SCHEMA_RESET`, `BRONZE_SCHEMA_CLEANED`, `BRONZE_WRITE_STARTED`, `BRONZE_WRITE_COMPLETED`, `ROW_COUNT_MISMATCH`, `PIPELINE_SUCCEEDED`, and `PIPELINE_FAILED`.
>
> Use these exact operations column names and types. In the following contract, `!` means `NOT NULL`; all other columns are nullable. Every table uses Delta. Keep the listed column order for compatibility.
>
> ```text
> pipeline_runs:
>   pipeline_run_id STRING!, pipeline_name STRING!, pipeline_version STRING!,
>   release_name STRING!, processing_mode STRING!, configuration_hash STRING!,
>   workflow_run_id STRING, status STRING!, started_ts TIMESTAMP!,
>   completed_ts TIMESTAMP, duration_seconds DOUBLE, triggered_by STRING,
>   error_class STRING, error_message STRING
> table_runs:
>   pipeline_run_id STRING!, pipeline_name STRING!, release_name STRING!,
>   source_file_name STRING!, source_table STRING!, target_table STRING!,
>   status STRING!, started_ts TIMESTAMP!, completed_ts TIMESTAMP,
>   duration_seconds DOUBLE, source_file_size BIGINT, source_row_count BIGINT,
>   bronze_row_count BIGINT, row_count_difference BIGINT,
>   source_schema_hash STRING, bronze_schema_hash STRING, error_message STRING
> schema_snapshots:
>   pipeline_run_id STRING!, pipeline_name STRING!, release_name STRING!,
>   layer_name STRING!, object_name STRING!, column_name STRING!, data_type STRING!,
>   nullable BOOLEAN, ordinal_position INT, schema_hash STRING!, captured_ts TIMESTAMP!
> reconciliation_results:
>   pipeline_run_id STRING!, pipeline_name STRING!, release_name STRING!,
>   source_file_name STRING!, bronze_table STRING!, raw_row_count BIGINT!,
>   bronze_row_count BIGINT!, row_count_difference BIGINT!,
>   raw_business_column_count INT!, bronze_business_column_count INT!,
>   metadata_column_count INT!, status STRING!, evaluated_ts TIMESTAMP!
> run_messages:
>   pipeline_run_id STRING!, pipeline_name STRING!, release_name STRING!,
>   source_name STRING, message_level STRING!, message_code STRING!,
>   message_text STRING!, created_ts TIMESTAMP!
> ```
>
> Append audit records using the target table's explicit schema, including records with null values; do not infer their types. Empty record lists are a no-op. `table_runs` stores separate appended `RUNNING` and terminal rows, so an ordinary successful run has 26 table-run rows, of which 13 have `completed_ts`. The original `RUNNING` table rows are not updated. Pipeline finalization updates its open pipeline row. `workflow_run_id` and `triggered_by` are nullable; populate them only when trustworthy runtime values are available. Normalize naive audit start/end timestamps as UTC and convert aware timestamps to UTC before calculating duration.
>
> 4. **Manifest, inventory, and readiness validation.** List the selected Raw Volume using `dbutils.fs.ls`. Require exactly one readable `manifest.json` in the same directory as the Parquet files; do not treat it as an ignorable inventory entry. `release_manifest.json`, if supported by the repository convention, may remain a non-source auxiliary file, but it must not replace or override `manifest.json`. Parse the manifest and require a per-source contract for every enabled source. The manifest's `output_files` entries are the authoritative delivered-file metadata: `file_name`/`file_path`, `table_name`, `checksum`, `row_count`, ordered `columns`, and `schema_hash`. Normalize trailing slashes from file names and paths before comparison. The selected configuration profile and isolated Volume/schema path identify whether the package is 5K clean, 5K tainted, 50K clean, 50K tainted, or 250K production; do not require a new clean/tainted field in the existing manifest. Preserve the manifest's own release metadata as source evidence and record the selected profile alongside it. Fail before Bronze publication if the manifest is missing, malformed, duplicated, missing a configured source, contains an unexpected source, points to a missing/unexpected file, or disagrees with the configured source-to-table mapping. For every configured file, capture file path, size when available, and modification timestamp when available; read it with `spark.read.parquet`; calculate its row count; capture each field as column name, `simpleString()` data type, and nullability; calculate the same ordered schema hash used by the manifest; calculate the SHA-256 checksum of the delivered Parquet file bytes; and compare the actual checksum, row count, ordered column list, and schema hash to the manifest before publication. Any mismatch or unreadable Parquet must fail the task and prevent Bronze publication. Persist the expected and actual manifest values, mismatch type, source file, selected profile, and release/run identifiers in auditable validation evidence. The manifest itself is not copied into a Bronze data table.
>
> Fall back to the path's final component when a listing entry has no name. Use the listing's actual path, including any `dbfs:` prefix, for reads and lineage. Modification times may be absent, already be datetime values, or be epoch milliseconds; normalize available values to UTC-aware timestamps, including conversion of epoch milliseconds. Keep missing modification timestamps nullable rather than inventing values.
>
> Hash a schema by joining ordered `column_name:data_type:nullable` strings with `|`, then applying SHA-256 to UTF-8 bytes; use Python's `True`, `False`, or `None` spelling. Schema snapshot ordinals start at 1. Raw `object_name` is the source filename, and Bronze `object_name` is the unqualified Bronze table name. Append Raw snapshots only after all sources pass readiness. Construction and reconciliation must independently verify that their inputs remain manifest-matched; they need not share in-memory DataFrames across separate tasks. Keep Raw files unchanged throughout a run.
>
> 5. **Bronze construction.** For each source in build order, first require that the Raw readiness record is manifest-matched, then read the Parquet file again and retain every original column in its original order. Fail if a source already has any of the reserved metadata column names below. Append these columns in this exact order:
>
> ```text
> _pipeline_run_id
> _pipeline_name
> _pipeline_version
> _release_name
> _source_file_name
> _source_file_path
> _source_file_size
> _source_file_modification_ts
> _ingested_ts
> _source_record_hash
> ```
>
> Use the shared run context and file-readiness record for the first eight values; use the table's UTC start timestamp for `_ingested_ts` in the workflow. Any helper invoked without an explicit ingestion timestamp must also use a UTC-aware timestamp. Compute `_source_record_hash` with Spark built-in functions only: cast every **original source column** to string, replace null with the literal `__NULL__`, concatenate values in original source-column order with `||`, then apply SHA-256. These casts apply only inside the hash expression, not to stored business columns. Do not collect source data to the driver, use Python UDFs, join, filter, sort, or otherwise transform business rows. Duplicate source rows must remain duplicate Bronze rows, even if their hashes match. The hash encoding does not escape `||` or the null sentinel, so it is not a guaranteed unique row key.
>
> 6. **Full-refresh Delta publication and table metadata.** For each configured target `catalog.bronze_schema.bronze_table`, write the enriched DataFrame using `format("delta")`, `mode("overwrite")`, `option("overwriteSchema", "true")`, and `saveAsTable`. The schema-level reset has already removed prior tables and data; table-level overwrite remains required for deterministic reruns and must never be replaced with append. Then apply a table comment stating that it is a Raw-preserving Delta representation for the release, and set these table properties: `napa.layer=bronze`, `napa.release=<release>`, `napa.pipeline=raw_to_bronze`, and `napa.processing_mode=full_refresh`. Record table start and terminal success/failure audit rows. On a failed source, persist error evidence, re-raise, and ensure finalization clears the selected Bronze schema so the failed run does not leave partial tables. A successful whole-workflow rerun must create a new run ID, reset the schema, and publish exactly the 13 configured Bronze tables rather than append or retain prior tables.
>
> Count the enriched DataFrame and capture its schema/hash before writing. After successful publication and table metadata updates, append its Bronze schema snapshots. The stored table-run Bronze count is this pre-write count; task 5 separately counts the persisted table. Both count differences are `bronze - raw`. Raw and Bronze full-schema hashes normally differ because Bronze includes metadata; do not require their equality. The exact table comment is `Raw-preserving Delta representation of the NAPA <source_name> Parquet file for release <release_name>. Business values are unchanged; operational ingestion metadata is appended.`
>
> Publication resets and overwrites the selected Bronze schema directly; there is no staging, release-wide transaction, or rollback of previously published tables. A comment/property error can fail a build after some tables have been written. The failure path must drop/recreate the selected Bronze schema again, leaving no Bronze tables from the failed run; if cleanup itself fails, surface that failure prominently in finalization and audit evidence. The Raw schema, Raw Volume, and operations schema must remain intact for diagnosis and rerun. Wrap publication failures as `BronzePublicationError` with the original exception chained; record `BRONZE_WRITE_FAILED` and preserve the root-cause traceback in job logs. Record other failure evidence where audit storage is available, without promising durable records when storage itself is unavailable. Whole-run idempotency means unchanged inputs produce the same business rows/counts after a new destructive reset; run IDs, ingestion timestamps, and appended audit history intentionally change.
>
> 7. **Reconciliation.** For each target table, read it back and compare it to its manifest-matched Raw readiness record. The helper returns `MATCHED` only for matching manifest/Raw/Bronze row counts, all ten metadata columns, all manifest/Raw business columns, no additional business columns, and exact manifest/Raw business-column order; otherwise it returns `MISMATCH`. It does not compare business values, hashes, data types, nullability, metadata values/types, or the order of the metadata columns unless the team explicitly adds those checks. A column-order mismatch is therefore a reconciliation failure and must be treated the same as a row-count, missing-column, or unexpected-column mismatch. The task must persist all reconciliation results and relevant diagnostic messages before raising, wherever audit storage is available. After persistence, the task must raise a reconciliation failure whenever any result has `status != "MATCHED"`; it must never log or report successful reconciliation when any source has a `MISMATCH`. An unreadable target raises `ReconciliationError`, logs `SCHEMA_RECONCILIATION_FAILED`, and exits only after any available buffered results and diagnostics are persisted. Finalization must continue to evaluate the persisted reconciliation results and mark the pipeline `FAILED` if any result is not `MATCHED`.
>
> 8. **Finalization.** Summarize audit evidence for the original run ID. Mark it `SUCCEEDED` only when the number of completed table-run records equals the enabled source count, every completed record is `SUCCEEDED`, the reconciliation-record count equals the enabled source count, every reconciliation is `MATCHED`, and the selected Bronze schema contains exactly the enabled source tables with no unexpected tables; otherwise mark it `FAILED`. Count only table-run records with non-null `completed_ts`. These are row-count checks, not checks of distinct source coverage or actual Databricks task states. Error messages alone do not force failure. A started run with missing evidence fails; no pipeline/table/reconciliation evidence at all raises `PipelineFinalizationError` even if messages exist. For any failed run, finalization must perform the configured destructive Bronze cleanup and verify that the selected Bronze schema is empty or absent before marking the run failed. Do not delete Raw or operations evidence during cleanup.
>
> Update open `pipeline_runs` records with a Delta `MERGE` on `pipeline_run_id` and `target.completed_ts IS NULL`, using the existing table schema and cleaning up the temporary view. Preserve the original start time, workflow ID, and trigger. If no open row exists but summary evidence does exist, append a terminal record with a new start timestamp. Failed terminal records use `error_class = PipelineRunFailed` and the summary as `error_message`. Append a final success/failure message, set the `final_status` task value, and raise if the summary is failed. Task-level repair with the same run ID is not supported by this audit contract; use a new whole-job run ID after correcting a failure.
>
> ### Databricks Asset Bundle requirement
>
> Use a Databricks Asset Bundle rather than a manually configured job. Create or update root `databricks.yml` with a team-specific `bundle.name`, `include: [config/raw_to_bronze/workflows/*.yml]`, and `sync.include` entries for `config/raw_to_bronze/**`, `notebooks/**`, `src/**`, and `requirements.txt`. Define `variables.script_root.default: ${workspace.file_path}/notebooks`. Define target `dev` with `default: true`, `mode: development`, and the team's workspace host/authentication configuration. Never embed another person's host, credentials, secrets, or local paths. Preserve existing includes if other workflows are already in scope; a standalone Bronze build does not need Silver, Gold, certification, or orchestration jobs.
>
> Define `config/raw_to_bronze/workflows/napa_raw_to_bronze.job.yml` with job resource key `napa_raw_to_bronze`, display name `NAPA Raw to Bronze`, `max_concurrent_runs: 1`, tag `pipeline_name: raw_to_bronze`, `performance_target: STANDARD`, and one job parameter:
>
> ```yaml
> - name: dataset_profile
>   default: napa_5k_clean
> ```
>
> The CLI `dataset_profile` parser must accept exactly `napa_5k_clean`, `napa_5k_tainted`, `napa_50k_clean`, `napa_50k_tainted`, and `napa_250k_production`; the standalone helper trims whitespace and lowercases its input before validation. The selected profile maps directly to the corresponding environment file and configured `release.release_name`; this need not equal the generator's native manifest `release_name`. Do not silently map a clean or tainted profile to a generic scale-only profile. Configure the job environment with `environment_key: napa_serverless_python`, `spec.environment_version: "4"`, and `spec.dependencies: [pyyaml]`; assign this environment key to every script task. Set the owner tag to the assigned team. Validate these construction settings in the target workspace. Do not use an existing cluster, job cluster, cluster ID, or warehouse ID.
>
> Create six `spark_python_task` tasks, using a configurable `${var.script_root}` that points to the synced `notebooks` directory:
>
> ```text
> resolve_configuration
>   -> validate_release_environment
>   -> validate_raw_inventory
>   -> build_bronze_tables
>   -> validate_bronze_reconciliation
>   -> finalize_pipeline_run
> ```
>
> Use the exact entrypoints and timeouts below. Tasks 2-5 each depend on the immediately preceding task. Task 6 depends on all five earlier tasks and uses `run_if: ALL_DONE`; it is not simply an ordinary success-only dependency on task 5. Do not configure automatic task repair or retries that reuse the same run ID.
>
> | Task key | File under `notebooks/` | Timeout (seconds) |
> |---|---|---:|
> | `resolve_configuration` | `01_resolve_configuration.py` | 900 |
> | `validate_release_environment` | `02_validate_release_environment.py` | 1200 |
> | `validate_raw_inventory` | `03_validate_raw_inventory.py` | 1800 |
> | `build_bronze_tables` | `04_build_bronze_tables.py` | 7200 |
> | `validate_bronze_reconciliation` | `05_validate_bronze_reconciliation.py` | 3600 |
> | `finalize_pipeline_run` | `06_finalize_pipeline_run.py` | 1200 |
>
> Each task receives `--dataset-profile {{job.parameters.dataset_profile}}`. Task 1 creates the run ID and publishes it with `dbutils.jobs.taskValues.set(key="run_id", value=...)`; every later task requires `--run-id {{tasks.resolve_configuration.values.run_id}}`. All scripts support optional `--config-path`, defaulting to the repository's `config/raw_to_bronze`; blank paths are treated as unset. The job need not expose this override as a job parameter. Each script reloads the selected configuration. Task 2 allows creation of missing environment objects; tasks 4 and 5 require them to exist.
>
> Publish diagnostic task values: task 1 sets `run_id`, `pipeline_run_id`, `release_name`, `config_hash`, `catalog`, `raw_schema`, `bronze_schema`, and `source_path`; task 2 adds `operations_schema_fqn`; task 4 adds `bronze_table_count`; task 5 adds `reconciled_source_count`; task 6 adds `final_status`. Tasks 2-6 also set `run_id`. Runtime helpers may resolve `spark`/`dbutils` from the supported execution context and must fail clearly if unavailable. Do not suppress failure to publish the critical `run_id` value or invent a replacement run ID. `ALL_DONE` cannot guarantee finalization if task 1 never produced its run ID or audit storage is unavailable.
>
> Include a compact bootstrap helper if needed so the bundled entrypoints can locate `src/napa_pipeline` without relying on a local Windows path.
>
> The finished repository should support:
>
> ```bash
> databricks bundle validate -t dev
> databricks bundle deploy -t dev
> databricks bundle run -t dev napa_raw_to_bronze --params dataset_profile=napa_5k_clean
> databricks bundle run -t dev napa_raw_to_bronze --params dataset_profile=napa_5k_tainted
> databricks bundle run -t dev napa_raw_to_bronze --params dataset_profile=napa_50k_clean
> databricks bundle run -t dev napa_raw_to_bronze --params dataset_profile=napa_50k_tainted
> databricks bundle run -t dev napa_raw_to_bronze --params dataset_profile=napa_250k_production
> ```
>
> Add a runbook covering authentication/configuration, initial schema/Volume setup and colocated manifest/Raw-file placement, deployment, 5K clean and tainted execution and whole-job reruns, then 50K clean and tainted execution, and finally 250K production execution. Document clearly that every run drops/recreates only the selected dedicated Bronze schema, that a failed run is cleaned back to an empty Bronze schema, and that Raw and operations evidence remain available. Resolve the target namespace before deployment or execution. Do not claim deployment or runtime acceptance if only YAML/local tests were checked.
>
> For each successful run, record the run ID, config hash, one validated manifest, 13 readable and manifest-matched inputs, 13 published tables, 13 completed successful table-run records, 13 matched reconciliations, zero manifest/Raw/Bronze count differences, ten metadata columns per table, and a terminal successful pipeline row. Verify business-row preservation in integration tests; count equality alone does not prove it. For troubleshooting, query each operations table by `pipeline_run_id` and `pipeline_name = 'raw_to_bronze'`, inspect completed table-run rows, order messages by `created_ts`, and check the failed task's traceback. Early failures may have no durable audit record. Prefer a whole-job rerun after correcting the cause; task repair with the same run ID can duplicate audit evidence and fail finalization's count checks.
>
> ### Required tests
>
> The following is the required test plan for the implementation. Use small local fakes for logic tests, but validate actual Spark/Delta behavior with integration tests where available. The test plan is specified here and does not depend on a pre-existing test suite.
>
> Use the repository's existing test framework (pytest if none exists) and small fakes/fixtures so local unit tests do not require Databricks, network access, or production data. Add tests for at least:
>
> - valid configuration resolution for all five dataset profiles, deep merge, placeholder resolution, unsupported profile rejection, invalid clean/tainted/production combinations, duplicate build-order rejection, and deterministic config hash;
> - release-specific schema, Volume path, fully-qualified Bronze target construction, destructive schema reset, exact 13-table success state, and empty-schema failure cleanup;
> - missing/malformed manifest files, missing or unexpected manifest entries, checksum mismatches, manifest row-count/column/schema-hash mismatches, missing and unexpected inventory entries, trailing-slash names, unreadable Parquet, readiness row-count/schema capture, and ordered schema hashes;
> - creation and detection of schemas/Volumes, disabled creation, and inaccessible catalog errors;
> - the exact metadata-column tuple/order, metadata collision failure, source-record hash semantics, Delta writer format/mode/schema overwrite options, standardized comment/properties, and wrapped publication failures;
> - reconciliation helper and orchestration behavior: a column-order mismatch produces `MISMATCH`, every `status != "MATCHED"` causes the reconciliation task to fail even when it is the only mismatch, the mismatch and diagnostic evidence are persisted before failure where storage is available, a fully matched dataset allows reconciliation to succeed, and finalization identifies the persisted failed reconciliation;
> - operations-table DDL/record construction, UTC duration behavior, audit finalization success and failure, and use of `MERGE` to close an open pipeline row;
> - dataset-profile CLI validation, invalid profile behavior, and rejection of clean/tainted selection for 250K;
> - bundle workflow definition: one `dataset_profile` parameter with the five allowed values, the exact six-task order/dependencies, Python-script task files, downstream run-ID dynamic references, final `ALL_DONE`, serverless environment references, and no cluster configuration;
> - cross-pipeline compatibility if a Bronze-to-Silver configuration already exists: same Bronze catalog/schema, source/table mapping, and natural keys.
>
> Include tests for the documented limits: readable zero-row input with a matching manifest passes readiness; an order-only mismatch is persisted and fails the reconciliation task and finalization; repeated same-run terminal audit records fail the count checks; missing audit evidence raises; and a finished pipeline with no open row takes the append fallback. Also test that a run drops prior Bronze tables before publication, never appends to a prior run, publishes exactly the configured 13 tables on success, removes partial tables after a publication failure, preserves Raw/operations evidence during cleanup, and refuses to drop an unresolved or non-dedicated schema. Test task orchestration as well as helper results so a correct helper cannot conceal an incorrect task status. Additional coverage should include metadata collisions, hash sentinel/delimiter ambiguity, and environment/CLI error paths. Do not require staging, cross-table rollback, or a fixed expected business schema beyond the manifest contract.
>
> If a Spark-enabled test environment is available, add a small integration test with synthetic Parquet that verifies original row/column preservation, metadata values, hash behavior for nulls and duplicates, reconciliation, and overwrite idempotency. Do not require the real NAPA data for unit tests.
>
> ### Verification and final response
>
> After implementation, run the relevant tests, bundle validation if the Databricks CLI is configured, applicable formatting/lint checks already present in the repository, `git diff --check`, and inspect the final diff. Do not claim Databricks runtime success unless you actually deployed and executed it.
>
> In your final response, use this exact factual structure:
>
> ```text
> Summary
> - What changed
>
> Files changed
> - path/to/file
>
> Validation
> - Tests or checks run, including results
>
> Assumptions
> - Actual assumptions made; distinguish them from validated facts
>
> Remaining items
> - Anything not run, unresolved, or needing team review
> ```

## Student review checklist

Before accepting AI-produced code, your team should be able to explain why each source is copied without business cleansing, how the run ID moves through the workflow, how a rerun avoids append duplicates, what causes a run to fail, and how to locate evidence for a failed run in the operations schema. Record meaningful AI assistance in your project's AI-usage documentation.

