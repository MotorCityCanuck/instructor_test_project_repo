# NAPA Olympic Analytics Platform

# Bronze → Silver Layer Engineering Specification

**Document Version:** 1.0  
**Status:** Instructor Reference Implementation  
**Audience:** Instructor, Solution Architect, Data Engineering Lead, AI Coding Assistant (Codex)  
**Platform:** Databricks Free Edition (serverless compute only), Unity Catalog, Delta Lake, PySpark  
**Pipeline Type:** Configuration-Driven Full Refresh

---

# Table of Contents

1. Purpose
2. Architectural Context
3. Design Principles
4. Supported Dataset Sizes
5. Bronze Layer Inventory
6. Silver Layer Objectives
7. Repository Organization
8. Configuration Architecture
9. Processing Lifecycle
10. Global Transformation Standards
11. Standard Pipeline Processing
12. Source Contract Validation
13. Data Type Standardization
14. Column Naming Standards
15. Standard Metadata Columns
16. Deterministic Record Hash
17. Surrogate Keys
18. String Standardization
19. Numeric Standardization
20. Date Standardization
21. Null Handling
22. Domain Normalization
23. Duplicate Resolution
24. Reject Processing
25. Data Quality Framework
26. Validation Severity
27. Referential Integrity
28. Data Quality Score
29. Logging Framework
30. Operational Tables
31. Error Handling
32. Reconciliation
33. Build Order
34. Definition of a Successful Table Build
35. Silver Layer Table Specifications
36. Regions
37. Clubs
38. Club Memberships
39. Players
40. Player Registrations
41. Player Assessment History
42. Teams
43. Team Memberships
44. Monthly Batches
45. Matches
46. Match Teams
47. Match Team Players
48. Match Games
49. Cross-Table Validation
50. Operational Views
51. Publication Standards
52. Reconciliation
53. Silver Completion Checklist
54. Complete Configuration Model
55. `base.yml`
56. Release Environment Configuration
57. `sources.yml`
58. `silver_tables.yml`
59. `domains.yml`
60. `quality_rules.yml`
61. Configuration Validation
62. Databricks Workflow Design
63. Running the Three Releases
64. Operations Layer
65. Reject Table Standard
66. Testing Strategy
67. Performance and Delta Guidance
68. Security and Governance
69. Runbook
70. Final Definition of Done
71. Codex Implementation Instructions

---

# 1. Purpose

## 1.1 Overview

This document defines the engineering specification for constructing the Silver layer of the NAPA Olympic Analytics Platform.

It is intended to serve as the authoritative technical design for the instructor reference implementation. The specification describes the required architecture, transformation standards, data quality framework, configuration model, and table-level design that collectively transform the Bronze layer into a trusted enterprise data foundation.

Because the target workspace is Databricks Free Edition, the orchestration design in this document assumes serverless compute only. Workflow tasks must be implemented as serverless job tasks rather than cluster-based tasks.

This document is **not** intended to describe analytical models, feature engineering, or Olympic roster selection logic. Those capabilities belong exclusively within the Gold layer.

## 1.2 Objectives

The Silver layer shall:

- transform raw Bronze data into standardized business entities;
- enforce consistent schemas;
- validate data quality;
- remove duplicate business records;
- standardize domains;
- validate relationships;
- create reusable business datasets;
- preserve complete lineage;
- expose clean datasets for downstream analytics.

The Silver layer shall **not**:

- calculate player chemistry;
- calculate fatigue;
- generate machine-learning features;
- perform explainability;
- generate dashboards;
- rank players;
- recommend Olympic teams.

## 1.3 Guiding Philosophy

The NAPA project intentionally teaches students enterprise data engineering using AI-assisted development.

Accordingly, the Silver layer should resemble a professional production data platform rather than a simplified classroom exercise.

The architecture should emphasize:

- maintainability;
- readability;
- modularity;
- configuration;
- repeatability;
- transparency;
- testability.

---

# 2. Architectural Context

## 2.1 Medallion Architecture

The platform follows the Databricks Medallion Architecture.

```text
                Raw Parquet Files
                       │
                       ▼
                Bronze Delta Tables
                       │
         Standardization
         Validation
         Cleansing
         Conformance
         Metadata
                       │
                       ▼
                Silver Delta Tables
                       │
      Business Analytics
      Feature Engineering
      Machine Learning
      Dashboards
      Executive Reporting
                       │
                       ▼
                 Gold Products
```

## 2.2 Layer Responsibilities

### Raw

**Purpose:** Store immutable source Parquet files exactly as delivered.

**Characteristics:**

- immutable;
- no transformations;
- organized by release;
- file-oriented.

### Bronze

**Purpose:** Create one Delta table for each delivered source file.

**Characteristics:**

- preserves original structure;
- minimal transformations;
- ingestion metadata only;
- source of truth for Silver.

Bronze may contain:

- duplicates;
- nulls;
- inconsistent formatting;
- invalid relationships;
- malformed records.

### Silver

**Purpose:** Create trusted enterprise business datasets.

**Characteristics:**

- standardized;
- validated;
- relationship-aware;
- reusable;
- configuration-driven.

Silver represents the operational business model.

### Gold

**Purpose:** Produce analytical assets.

Examples:

- machine-learning features;
- dashboards;
- executive reporting;
- player evaluation;
- chemistry calculations;
- Olympic roster recommendations.

---

# 3. Design Principles

The following principles govern every component of the Silver layer.

## 3.1 Configuration-Driven

Pipeline behavior shall be controlled through external configuration.

Configuration determines:

- source tables;
- target tables;
- schemas;
- catalogs;
- domains;
- validation rules;
- build order;
- execution settings.

Business logic shall not contain environment-specific constants.

## 3.2 Deterministic

Executing the pipeline multiple times against identical Bronze data shall produce identical Silver business data.

Business records shall not change because of execution order or Spark partitioning.

## 3.3 Idempotent

Executing the pipeline repeatedly shall always leave the Silver layer in the same business state.

Running the pipeline twice shall not duplicate data.

## 3.4 Full Refresh

Every execution rebuilds the complete Silver layer.

The implementation shall not include:

- incremental loading;
- change data capture;
- streaming;
- watermark processing;
- Delta `MERGE`;
- append processing.

## 3.5 Enterprise Readability

The implementation should optimize for maintainability.

Preferred:

- small reusable functions;
- descriptive variable names;
- modular transforms;
- centralized validation.

Avoid:

- monolithic notebooks;
- duplicated code;
- hidden business logic.

## 3.6 Fail Fast

Critical problems shall immediately terminate execution.

Examples:

- missing Bronze table;
- missing required column;
- invalid configuration;
- incompatible schema.

The pipeline should never silently ignore critical errors.

---

# 4. Supported Dataset Sizes

The architecture must support all three NAPA datasets without code modification.

| Dataset | Approximate Players |
|---|---:|
| `napa_5k` | 5,000 |
| `napa_50k` | 50,000 |
| `napa_250k` | 250,000 |

Changing datasets shall require only a configuration change.

Each dataset represents a separate instance of the same medallion architecture. The codebase, table definitions, transformation logic, validation rules, and Workflow design remain shared.

---

# 5. Bronze Layer Inventory

The NAPA platform contains thirteen delivered Parquet source files and, correspondingly, thirteen expected Bronze source tables. The implementation shall discover and validate the configured source inventory rather than relying only on a hard-coded count.

The known operational domains are:

## Geographic

- `regions`

## Clubs

- `clubs`
- `club_memberships`

## Players

- `player_master`
- `player_registrations`
- `player_assessment_history`

## Teams

- `teams`
- `team_memberships`

## Competition

- `matches`
- `match_teams`
- `match_team_players`
- `match_games`

## Processing

- `monthly_batches`

## Authoritative Source Count

The authoritative NAPA Dataset Specification identifies thirteen delivered source files:

1. `regions`
2. `clubs`
3. `club_memberships`
4. `player_master`
5. `player_registrations`
6. `player_assessment_history`
7. `teams`
8. `team_memberships`
9. `matches`
10. `match_teams`
11. `match_team_players`
12. `match_games`
13. `monthly_batches`

The physical Parquet files and Bronze schemas remain authoritative. The pipeline shall validate that all configured sources exist and shall fail clearly if the delivered inventory differs.

---

# 6. Silver Layer Objectives

The Silver layer should remain close to the operational business model.

Unlike a traditional Kimball warehouse, the Silver layer intentionally preserves business entities rather than transforming immediately into dimensions and facts.

Advantages include:

- easier lineage;
- simpler debugging;
- clearer relationship to source data;
- more intuitive understanding;
- cleaner Gold feature engineering.

Accordingly, Silver tables will retain names such as:

```text
players
teams
matches
match_games
team_memberships
```

rather than:

```text
dim_player
fact_match
dim_team
```

A dimensional warehouse or semantic model, if required, belongs in Gold.

---

# 7. Repository Organization

Recommended repository layout:

```text
config/
    base.yml
    environments/
        napa_5k.yml
        napa_50k.yml
        napa_250k.yml
    sources.yml
    silver_tables.yml
    quality_rules.yml
    domains.yml
    logging.yml

src/
    pipeline/
        config.py
        orchestration.py
        validation.py
        logging.py
        io.py
        metadata.py
        transforms/
        quality/

notebooks/
    00_setup_catalog.py
    01_validate_bronze.py
    02_build_silver.py
    03_validate_silver.py
    04_publish.py

tests/

docs/
```

Notebooks should act as thin execution wrappers.

Business logic belongs in reusable Python modules.

The Databricks Workflow controls task execution, dependencies, retries, and run history. YAML configuration controls how the notebooks and reusable code behave for each release.

---

# 8. Configuration Architecture

The pipeline shall be entirely configuration-driven.

Configuration files include:

| Configuration | Purpose |
|---|---|
| `base.yml` | Global defaults |
| `environments/*.yml` | Release-specific settings |
| `sources.yml` | Bronze source registry |
| `silver_tables.yml` | Silver target registry |
| `quality_rules.yml` | Validation rules |
| `domains.yml` | Domain standardization |
| `logging.yml` | Logging configuration |

No notebook shall hard-code:

- release names;
- schema names;
- catalog names;
- execution order;
- quality thresholds.

Those values belong in configuration.

The three release configurations should differ only where necessary, such as:

- release name;
- source location;
- Bronze schema;
- Silver schema;
- Gold schema;
- expected approximate scale;
- optional performance settings.

They should not redefine common business rules or transformations.

---

# 9. Processing Lifecycle

Every execution shall follow the same lifecycle.

```text
Read Configuration
        ↓
Validate Environment
        ↓
Validate Bronze Layer
        ↓
Create Pipeline Run
        ↓
Process Silver Tables
        ↓
Validate Silver
        ↓
Publish Tables
        ↓
Reconcile Counts
        ↓
Publish Operational Metrics
        ↓
Complete Pipeline
```

Each phase shall generate operational logging and validation metrics.

A Databricks Workflow should orchestrate the tasks. A typical Workflow may include:

```text
01_resolve_configuration
        ↓
02_validate_environment
        ↓
03_validate_bronze
        ↓
04_build_silver_reference
        ↓
05_build_silver_players
        ↓
06_build_silver_teams
        ↓
07_build_silver_competition
        ↓
08_validate_silver
        ↓
09_publish_views
        ↓
10_publish_run_summary
```

The same Workflow should be run with different release parameters for `napa_5k`, `napa_50k`, and `napa_250k`.

---

# 10. Global Transformation Standards

## 10.1 Purpose

Every Silver transformation shall follow a common set of engineering standards regardless of the source table being processed.

These standards ensure that all Silver tables behave consistently and provide a predictable interface for downstream consumers.

Transformation logic should be implemented once within reusable framework components whenever possible.

---

# 11. Standard Pipeline Processing

Each table shall pass through the same processing stages.

```text
Read Bronze
    ↓
Validate Source Contract
    ↓
Standardize Columns
    ↓
Cast Data Types
    ↓
Normalize Domains
    ↓
Apply Business Transformations
    ↓
Resolve Duplicates
    ↓
Validate Records
    ↓
Split Accepted / Rejected
    ↓
Add Metadata
    ↓
Publish Silver
    ↓
Reconcile Counts
    ↓
Log Results
```

The implementation should avoid creating unique processing logic for every table when common framework components can be reused.

---

# 12. Source Contract Validation

Before any transformation occurs, the pipeline shall validate the Bronze source table.

Required validations include:

- table exists;
- schema exists;
- required columns exist;
- required data types can be cast;
- primary business key exists;
- configuration entry exists.

If any required validation fails:

- terminate processing;
- record the failure;
- do not publish the Silver table.

## 12.1 Unexpected Columns

Unexpected columns shall not automatically cause pipeline failure.

Instead:

- record a warning;
- preserve the column if configured;
- document the schema change.

This approach makes the pipeline resilient to controlled schema evolution while preventing unreviewed fields from becoming trusted Silver attributes.

---

# 13. Data Type Standardization

All Silver columns shall use explicit data types.

Never rely on Spark schema inference.

Typical mappings include:

| Bronze | Silver |
|---|---|
| String | `string` |
| Integer | `integer` |
| Long | `long` |
| Float | `double` |
| Boolean | `boolean` |
| Date String | `date` |
| Timestamp String | `timestamp` |

Invalid casts shall generate rejected records where the column is required or the rule severity requires rejection.

---

# 14. Column Naming Standards

Silver uses consistent `snake_case` naming.

Examples:

| Bronze | Silver |
|---|---|
| `PlayerID` | `player_id` |
| `HomeRegionID` | `home_region_id` |
| `MatchDate` | `match_date` |
| `TeamOneScore` | `team_one_score` |

Suffix conventions:

| Suffix | Meaning |
|---|---|
| `_id` | Natural business key |
| `_sk` | Surrogate key |
| `_date` | Date |
| `_ts` | Timestamp |
| `_flag` | Boolean |
| `_count` | Integer count |
| `_pct` | Percentage represented as 0–100 |
| `_ratio` | Decimal proportion |

---

# 15. Standard Metadata Columns

Every Silver table shall contain operational metadata.

Required metadata:

| Column | Purpose |
|---|---|
| `_pipeline_run_id` | Pipeline execution identifier |
| `_pipeline_version` | Version of pipeline |
| `_source_dataset` | 5K, 50K, or 250K release |
| `_source_table` | Bronze source |
| `_load_ts` | Timestamp loaded |
| `_record_hash` | Deterministic business hash |

Optional metadata:

| Column |
|---|
| `_data_quality_score` |
| `_data_quality_status` |

Metadata should be appended by the publishing framework rather than by individual transformation notebooks.

---

# 16. Deterministic Record Hash

Each accepted record shall receive a deterministic SHA-256 hash.

Purpose:

- lineage;
- debugging;
- reproducibility;
- validation;
- regression testing.

Hash inputs should include:

- natural business key;
- standardized business columns.

Exclude:

- load timestamp;
- run identifier;
- other execution metadata.

---

# 17. Surrogate Keys

Silver shall generate deterministic surrogate keys where useful.

Preferred implementation:

```text
SHA256(natural business key)
```

Avoid:

- `monotonically_increasing_id()`;
- random UUIDs;
- `row_number()` without deterministic ordering.

Natural keys remain the authoritative business identifiers.

Surrogate keys support reliable joins and potential downstream dimensional models.

---

# 18. String Standardization

Every string field shall pass through a reusable standardization function.

Processing includes:

- trim whitespace;
- collapse repeated spaces;
- convert empty string to null;
- normalize line endings;
- remove leading and trailing tabs.

Configured coded values shall be converted to uppercase.

Examples:

```text
" usa "
    ↓
"USA"
```

```text
"   "
    ↓
NULL
```

Free-text descriptive columns should retain original capitalization whenever practical.

---

# 19. Numeric Standardization

Numeric processing shall include:

- explicit casting;
- overflow detection;
- range validation;
- precision preservation.

Do not silently convert invalid values.

Invalid numeric values become rejected records unless configured otherwise.

---

# 20. Date Standardization

Dates shall be converted into Spark date types.

Validation includes:

- valid format;
- valid calendar date;
- not before configured minimum;
- not after configured maximum;
- logical start and end ordering.

Dates should be interpreted relative to the NAPA dataset snapshot rather than the wall-clock execution date.

For example, player age should be calculated relative to the dataset batch date or configured release as-of date.

Do not use `current_date()` for reproducible business calculations unless explicitly configured.

---

# 21. Null Handling

Columns shall be classified as:

- required;
- conditionally required;
- optional;
- derived.

Required fields containing null values shall fail validation.

Optional null values shall remain null.

Never replace missing values with:

- `0`;
- `UNKNOWN`;
- `N/A`;
- blank string;

unless explicitly required by a documented business rule.

---

# 22. Domain Normalization

Domain values shall be configuration-driven.

Examples include:

### Gender

```text
MALE
  ↓
M
```

```text
Female
   ↓
F
```

### Country

```text
Canada
  ↓
CAN
```

```text
United States
      ↓
USA
```

### Handedness

```text
Left
 ↓
LEFT
```

Unknown values shall not be silently mapped.

---

# 23. Duplicate Resolution

Duplicate processing consists of two stages.

## 23.1 Exact Duplicates

Records identical across all business columns.

Processing:

- retain one;
- discard the remainder;
- record the duplicate count.

## 23.2 Duplicate Business Keys

Records sharing the same primary business identifier.

Selection priority:

1. most complete record;
2. valid record;
3. latest authoritative snapshot or batch;
4. lowest deterministic hash as final tie-breaker.

Remaining records become rejected records.

No duplicate should disappear without traceability.

---

# 24. Reject Processing

Every Silver target shall have corresponding rejected records.

Rejected records shall include:

| Column |
|---|
| `reject_reason` |
| `rule_id` |
| `rule_severity` |
| `source_table` |
| `source_business_key` |
| `pipeline_run_id` |
| `load_ts` |
| `source_record_json` |

Reject tables provide transparency and support instructor debugging.

---

# 25. Data Quality Framework

Validation shall be rule-based.

Supported rule types include:

- required field;
- unique key;
- foreign key;
- allowed values;
- numeric range;
- date range;
- custom expression;
- record count;
- cardinality;
- reconciliation.

Rules shall be defined through configuration.

---

# 26. Validation Severity

Every validation rule has a severity.

| Severity | Behavior |
|---|---|
| `CRITICAL` | Reject record immediately and fail dependent processing where appropriate |
| `ERROR` | Reject record |
| `WARNING` | Accept record and log warning |
| `INFO` | Record informational metric |

Severity must be configurable.

---

# 27. Referential Integrity

Foreign key validation shall occur after standardization.

Examples:

```text
Players → Regions
Teams → Players through Team Memberships
Matches → Monthly Batches
Match Teams → Matches and Teams
Match Team Players → Match Teams and Players
Match Games → Matches
```

Missing required parent records shall normally result in rejection.

---

# 28. Data Quality Score

Every accepted record may receive a quality score.

Suggested calculation:

```text
100 - configured warning deductions
```

Critical failures bypass scoring and reject the record.

Quality scores exist for operational monitoring, not analytical ranking.

---

# 29. Logging Framework

Every pipeline execution shall record:

## Pipeline Level

- run ID;
- start time;
- completion time;
- duration;
- release;
- pipeline version;
- status.

## Table Level

- source rows;
- accepted rows;
- rejected rows;
- duplicate rows;
- warning count;
- publication status;
- duration.

---

# 30. Operational Tables

Recommended operational tables:

- `pipeline_runs`;
- `table_runs`;
- `quality_results`;
- `reconciliation_results`;
- `schema_snapshots`;
- `run_messages`.

These tables provide permanent evidence of pipeline execution.

A shared operations schema may serve all three releases, provided each record contains the release identifier.

---

# 31. Error Handling

The implementation shall define explicit exception classes.

Recommended exceptions:

- `ConfigurationError`;
- `SchemaValidationError`;
- `TransformationError`;
- `ValidationError`;
- `PublicationError`;
- `ReconciliationError`.

Never use:

```python
except:
```

Always preserve root exception information.

---

# 32. Reconciliation

Every published Silver table shall reconcile.

Expected equation:

```text
Bronze Rows
=
Accepted Rows
+ Rejected Rows
+ Exact Duplicate Rows
+ Duplicate Business-Key Loser Rows
```

Every discrepancy must be explained.

---

# 33. Build Order

Tables shall execute according to configuration.

Typical dependency order:

```text
monthly_batches
      ↓
regions
      ↓
players
      ↓
clubs
      ↓
teams
      ↓
player_registrations
      ↓
player_assessment_history
      ↓
club_memberships
      ↓
team_memberships
      ↓
matches
      ↓
match_teams
      ↓
match_team_players
      ↓
match_games
```

The Databricks Workflow controls stage-level dependencies. The configuration controls table-level ordering within a stage.

---

# 34. Definition of a Successful Table Build

A Silver table is considered complete only when:

- source contract validated;
- transformations completed;
- duplicate resolution completed;
- validation completed;
- rejects written;
- metadata added;
- Silver table published;
- reconciliation balanced;
- operational metrics recorded.

---

# 35. Silver Layer Table Specifications

## 35.1 Overview

This section defines the business contract for each Silver table.

Unlike the Bronze layer, which preserves the physical structure of the source data, the Silver layer standardizes and validates the data while preserving the operational business model.

Every Silver table shall follow the standards defined in the preceding sections.

Every table specification includes:

- business purpose;
- business grain;
- Bronze source;
- primary key;
- relationships;
- required transformations;
- derived columns;
- validation rules;
- reject conditions;
- metadata;
- notes.

The physical Bronze schema remains authoritative. Representative fields in this specification must be reconciled to the actual delivered columns before implementation.

---

# 36. Silver Table — Regions

## Business Purpose

The `regions` table provides the authoritative geographic reference used throughout the platform.

It represents the regions where clubs operate, players reside, and matches occur.

## Business Grain

One record per region.

## Bronze Source

```text
bronze.regions
```

## Primary Key

```text
region_id
```

## Foreign Keys

None.

## Typical Relationships

Referenced by:

- `clubs`;
- `players`;
- `matches`.

## Required Transformations

- standardize column names;
- trim string values;
- normalize country codes;
- normalize province or state codes;
- remove exact duplicates;
- resolve duplicate region identifiers;
- validate required fields;
- generate deterministic surrogate key.

## Representative Required Columns

| Column | Description |
|---|---|
| `region_id` | Natural region identifier |
| `region_sk` | Deterministic surrogate key |
| `region_name` | Display name |
| `province_state` | Province or state |
| `country_code` | `CAN` or `USA` |
| `active_flag` | Active indicator where supported |

## Derived Columns

- `region_sk`;
- standardized country fields;
- `active_flag` where supported by source status or dates.

## Validation Rules

Required:

- `region_id`;
- `region_name`;
- `country_code`.

Valid country codes:

```text
CAN
USA
```

Duplicate region identifiers are not permitted after resolution.

## Reject Conditions

Reject when:

- `region_id` is null;
- `region_name` is null;
- country is invalid;
- duplicate business key cannot be resolved;
- required values cannot be cast.

## Metadata

Standard Silver metadata columns shall be appended.

---

# 37. Silver Table — Clubs

## Business Purpose

The `clubs` table represents official pickleball clubs or facilities within the NAPA ecosystem.

## Business Grain

One record per club.

## Bronze Source

```text
bronze.clubs
```

## Primary Key

```text
club_id
```

## Foreign Keys

```text
region_id → regions.region_id
```

## Required Transformations

- standardize column names;
- trim and normalize descriptive values;
- validate referenced region;
- normalize country values where present;
- remove exact duplicates;
- resolve duplicate club identifiers;
- generate deterministic surrogate key.

## Representative Required Columns

| Column | Description |
|---|---|
| `club_id` | Natural club identifier |
| `club_sk` | Deterministic surrogate key |
| `club_name` | Club name |
| `region_id` | Geographic region |
| `region_sk` | Conformed region key |
| `country_code` | Country where present |
| `active_flag` | Active status where supported |

## Derived Columns

- `club_sk`;
- `region_sk`;
- standardized country;
- active flag where supported.

## Validation Rules

- unique `club_id`;
- non-null `club_name`;
- valid region;
- valid country where populated;
- valid open and close date ordering where dates exist.

## Reject Conditions

Reject when:

- identifier is missing;
- required club name is missing;
- region is orphaned;
- duplicate key cannot be resolved;
- date sequence is invalid.

---

# 38. Silver Table — Club Memberships

## Business Purpose

The `club_memberships` table defines player membership within clubs and preserves membership history.

## Business Grain

One membership period between one player and one club.

## Bronze Source

```text
bronze.club_memberships
```

## Primary Key

```text
club_membership_id
```

## Foreign Keys

```text
player_id → players.player_id
club_id   → clubs.club_id
```

## Required Transformations

- validate player;
- validate club;
- standardize dates;
- compute membership duration;
- identify current memberships relative to the configured release as-of date;
- detect overlapping membership periods.

## Representative Required Columns

| Column | Description |
|---|---|
| `club_membership_id` | Membership identifier |
| `club_membership_sk` | Deterministic surrogate key |
| `player_id` | Player identifier |
| `player_sk` | Conformed player key |
| `club_id` | Club identifier |
| `club_sk` | Conformed club key |
| `membership_start_date` | Start date |
| `membership_end_date` | End date |
| `current_membership_flag` | Derived current-state indicator |

## Derived Columns

- `membership_duration_days`;
- `current_membership_flag`;
- conformed surrogate keys.

## Validation Rules

- player exists;
- club exists;
- start date is not after end date;
- required identifier exists;
- duplicate membership identifier does not remain;
- overlapping membership periods are flagged according to configured severity.

## Reject Conditions

Reject when:

- player is orphaned;
- club is orphaned;
- date ordering is invalid;
- membership identifier is missing;
- duplicate business key cannot be resolved.

---

# 39. Silver Table — Players

## Business Purpose

The `players` table represents the authoritative conformed player master record.

This is the central business entity used throughout Silver.

## Business Grain

One player.

## Bronze Source

```text
bronze.player_master
```

## Primary Key

```text
player_id
```

## Foreign Keys

```text
home_region_id → regions.region_id
```

## Referenced By

- `player_registrations`;
- `player_assessment_history`;
- `club_memberships`;
- `team_memberships`;
- `match_team_players`.

## Required Transformations

- standardize names;
- normalize country;
- normalize handedness where supplied;
- normalize gender where supplied;
- trim strings;
- calculate age relative to the release snapshot date;
- derive age group where configured;
- determine active flag from authoritative source fields;
- generate deterministic surrogate key.

## Representative Columns

| Column | Description |
|---|---|
| `player_id` | Natural player identifier |
| `player_sk` | Deterministic surrogate key |
| `first_name` | First name where supplied |
| `last_name` | Last name where supplied |
| `display_name` | Derived display name |
| `birth_date` | Birth date where supplied |
| `gender` | Standardized gender code |
| `dominant_hand` | Standardized handedness where supplied |
| `preferred_side` | Preferred court side where supplied |
| `home_region_id` | Home region |
| `home_region_sk` | Conformed region key |
| `country_code` | Standardized country |
| `active_flag` | Current active status |
| `age` | Age at release as-of date |
| `age_group` | Configured age band |

## Derived Columns

- `display_name`;
- `player_sk`;
- `home_region_sk`;
- `age`;
- `age_group`;
- `active_flag`.

## Validation Rules

- unique `player_id`;
- valid region where populated;
- valid gender where supplied;
- valid country where supplied;
- birth date not after release as-of date;
- rating or confidence values within configured ranges where present.

## Reject Conditions

Reject when:

- player identifier is missing;
- duplicate player cannot be resolved;
- required foreign key is invalid;
- required domain value is invalid;
- birth date is impossible;
- required cast fails.

## Gold Boundary

Do not derive:

- elite-player status;
- development-potential classification;
- Olympic eligibility score;
- roster-selection recommendation.

---

# 40. Silver Table — Player Registrations

## Business Purpose

The `player_registrations` table preserves player registration history.

## Business Grain

One registration event or registration period, according to the physical schema.

## Bronze Source

```text
bronze.player_registrations
```

## Primary Key

```text
registration_id
```

## Foreign Keys

```text
player_id → players.player_id
batch_id  → monthly_batches.batch_id
```

## Required Transformations

- validate player;
- validate batch;
- standardize registration dates;
- normalize registration type and status;
- determine current registration relative to the release as-of date;
- generate deterministic surrogate key.

## Representative Columns

| Column | Description |
|---|---|
| `registration_id` | Registration identifier |
| `registration_sk` | Deterministic surrogate key |
| `player_id` | Player identifier |
| `player_sk` | Conformed player key |
| `batch_id` | Monthly batch |
| `batch_sk` | Conformed batch key |
| `registration_date` | Registration date |
| `registration_type` | Registration category |
| `registration_status` | Standardized status |
| `effective_start_date` | Period start where applicable |
| `effective_end_date` | Period end where applicable |
| `current_registration_flag` | Derived current-state flag |

## Derived Columns

- registration duration;
- current-registration flag;
- registration sequence where reproducibly ordered;
- conformed keys.

## Validation Rules

- player exists;
- batch exists when populated or required;
- dates are valid;
- end date is not before start date;
- identifier is unique after resolution.

## Reject Conditions

Reject when:

- player is orphaned;
- required batch is orphaned;
- required identifier is missing;
- date ordering is invalid;
- duplicate business key cannot be resolved.

---

# 41. Silver Table — Player Assessment History

## Business Purpose

The `player_assessment_history` table stores periodic player assessments.

It preserves assessment history and does not aggregate or summarize it.

## Business Grain

One player assessment observation.

## Bronze Source

```text
bronze.player_assessment_history
```

## Primary Key

```text
assessment_id
```

## Foreign Keys

```text
player_id → players.player_id
batch_id  → monthly_batches.batch_id
```

## Required Transformations

- standardize assessment dates;
- normalize assessment types where supplied;
- validate assessment ranges;
- standardize numeric precision;
- generate deterministic surrogate key.

## Representative Columns

| Column | Description |
|---|---|
| `assessment_id` | Assessment identifier |
| `assessment_sk` | Deterministic surrogate key |
| `player_id` | Player identifier |
| `player_sk` | Conformed player key |
| `batch_id` | Monthly batch |
| `batch_sk` | Conformed batch key |
| `assessment_date` | Observation date |
| `assessment_type` | Assessment type where supplied |
| `assessment_value` | Assessment value where supplied |
| `assessment_confidence` | Confidence where supplied |
| `assessor_source` | Assessment source where supplied |

If the physical source stores assessments as multiple columns rather than type/value rows, preserve a wide Silver structure unless there is a documented business reason to normalize it.

## Derived Columns

No analytical aggregate or composite score shall be created.

Only standard metadata, conformed keys, and objective date attributes may be derived.

## Validation Rules

- valid player;
- valid batch where required;
- assessment values within configured ranges;
- assessment date not after release as-of date;
- duplicate assessment observations resolved deterministically.

## Reject Conditions

Reject when:

- player is orphaned;
- required batch is invalid;
- required assessment value cannot be cast;
- required range is violated;
- duplicate business key cannot be resolved.

## Gold Boundary

Do not:

- create a composite development score;
- normalize assessments against peers;
- weight assessment types;
- impute missing assessment values.

---

# 42. Silver Table — Teams

## Business Purpose

The `teams` table represents the authoritative definition of doubles teams within the NAPA platform.

A team is a persistent business entity that may participate in multiple matches and releases and may have changing membership over time.

The table does not contain player membership. Team composition is maintained in `team_memberships`.

## Business Grain

One row per team.

## Bronze Source

```text
bronze.teams
```

## Primary Key

```text
team_id
```

## Foreign Keys

None, unless the physical schema contains required conformed references.

## Referenced By

- `team_memberships`;
- `match_teams`.

## Required Transformations

- standardize identifiers;
- normalize category values;
- normalize country values where present;
- remove exact duplicates;
- resolve duplicate team identifiers;
- generate deterministic surrogate key;
- standardize status values.

## Representative Columns

| Column | Description |
|---|---|
| `team_id` | Natural team identifier |
| `team_sk` | Deterministic surrogate key |
| `team_name` | Display name where supplied |
| `team_category` | Men's, women's, or mixed category |
| `country_code` | Country where supplied |
| `team_status` | Standardized status |
| `formation_date` | Date established where supplied |
| `dissolution_date` | Date retired where supplied |
| `active_flag` | Derived or standardized active indicator |

## Derived Columns

- `team_sk`;
- `active_flag`;
- `team_age_days` relative to the release as-of date.

## Validation Rules

- unique `team_id`;
- valid category;
- formation date does not follow dissolution date;
- valid country where supplied;
- required status is valid where present.

## Reject Conditions

Reject when:

- identifier is missing;
- duplicate business key cannot be resolved;
- required category is invalid;
- date sequence is invalid.

---

# 43. Silver Table — Team Memberships

## Business Purpose

The `team_memberships` table represents player membership within teams and preserves historical membership periods.

## Business Grain

One player membership period within one team.

## Bronze Source

```text
bronze.team_memberships
```

## Primary Key

```text
team_membership_id
```

## Foreign Keys

```text
team_id   → teams.team_id
player_id → players.player_id
```

## Required Transformations

- validate player;
- validate team;
- standardize dates;
- compute membership duration;
- determine current membership relative to the release as-of date;
- detect overlapping periods;
- generate deterministic surrogate key.

## Representative Columns

| Column | Description |
|---|---|
| `team_membership_id` | Membership identifier |
| `team_membership_sk` | Deterministic surrogate key |
| `team_id` | Team identifier |
| `team_sk` | Conformed team key |
| `player_id` | Player identifier |
| `player_sk` | Conformed player key |
| `membership_start_date` | Membership start |
| `membership_end_date` | Membership end |
| `player_role` | Role where supplied |
| `player_position` | Position where supplied |
| `current_membership_flag` | Derived current-state flag |

## Derived Columns

- membership duration;
- current-membership flag;
- conformed player and team keys.

## Validation Rules

- player exists;
- team exists;
- start date does not follow end date;
- no unresolved duplicate membership key;
- overlapping membership periods are identified;
- roster cardinality findings are recorded.

A valid doubles team normally has two active members, but unexpected cardinality should be reported rather than automatically corrected.

## Reject Conditions

Reject when:

- player is orphaned;
- team is orphaned;
- required dates are invalid;
- required identifier is missing;
- duplicate business key cannot be resolved.

---

# 44. Silver Table — Monthly Batches

## Business Purpose

The `monthly_batches` table defines the release and processing timeline.

Operational events in the Silver layer should be traceable to a monthly batch where the source provides such a relationship.

## Business Grain

One monthly release or processing batch.

## Bronze Source

```text
bronze.monthly_batches
```

## Primary Key

```text
batch_id
```

## Referenced By

- `player_registrations`;
- `player_assessment_history`;
- `matches`;
- other release-dependent tables.

## Required Transformations

- standardize dates;
- validate chronological order;
- validate batch sequence;
- standardize batch status or type;
- generate deterministic surrogate key.

## Representative Columns

| Column | Description |
|---|---|
| `batch_id` | Batch identifier |
| `batch_sk` | Deterministic surrogate key |
| `batch_sequence` | Ordered release sequence |
| `batch_date` | Batch or release date |
| `batch_year` | Derived year |
| `batch_month` | Derived month |
| `batch_quarter` | Derived quarter |
| `batch_type` | Batch type where supplied |
| `batch_status` | Status where supplied |

## Derived Columns

- `batch_sk`;
- `batch_year`;
- `batch_month`;
- `batch_quarter`.

## Validation Rules

- unique batch identifier;
- unique sequence where required;
- chronological ordering;
- non-negative count metrics where present;
- reconciliation of batch count fields where supported by source.

## Reject Conditions

Reject when:

- batch identifier is missing;
- duplicate key cannot be resolved;
- batch date is invalid;
- sequence is invalid;
- required counts are impossible.

## Release As-Of Date

The maximum valid batch date should normally define the release as-of date unless explicitly overridden in configuration.

---

# 45. Silver Table — Matches

## Business Purpose

The `matches` table represents completed, scheduled, cancelled, or forfeited pickleball matches.

It defines the match itself. Participating teams and players are represented in related tables.

## Business Grain

One match.

## Bronze Source

```text
bronze.matches
```

## Primary Key

```text
match_id
```

## Foreign Keys

```text
batch_id  → monthly_batches.batch_id
region_id → regions.region_id
```

## Referenced By

- `match_teams`;
- `match_games`.

## Required Transformations

- validate batch;
- validate region where required;
- normalize match status;
- standardize dates;
- validate match category;
- generate deterministic surrogate key.

## Representative Columns

| Column | Description |
|---|---|
| `match_id` | Natural match identifier |
| `match_sk` | Deterministic surrogate key |
| `batch_id` | Monthly batch |
| `batch_sk` | Conformed batch key |
| `region_id` | Match region |
| `region_sk` | Conformed region key |
| `match_date` | Match date |
| `match_type` | Match type where supplied |
| `competition_category` | Men's, women's, or mixed |
| `match_status` | Standardized status |
| `winning_team_number` | Winning side number where supplied |
| `completed_flag` | Derived completion indicator |
| `match_year` | Derived year |
| `match_month` | Derived month |

## Derived Columns

- `match_sk`;
- conformed foreign keys;
- `completed_flag`;
- year and month fields.

## Validation Rules

- unique identifier;
- valid batch where required;
- valid region where required;
- valid status;
- completed match has a valid winner where required;
- winner side is valid;
- cancelled match is not presented as completed;
- match date is consistent with the batch period where required.

## Reject Conditions

Reject when:

- identifier is missing;
- required batch is orphaned;
- required region is orphaned;
- required status is invalid;
- completed match winner is impossible;
- duplicate business key cannot be resolved.

---

# 46. Silver Table — Match Teams

## Business Purpose

The `match_teams` table represents one participating team or side within a match.

Each completed doubles match should normally contain two records.

## Business Grain

One team within one match.

## Bronze Source

```text
bronze.match_teams
```

## Primary Key

```text
match_team_id
```

## Foreign Keys

```text
match_id → matches.match_id
team_id  → teams.team_id
```

## Required Transformations

- validate team;
- validate match;
- normalize side numbering;
- derive winner flag from authoritative match results;
- validate rating fields where supplied;
- generate deterministic surrogate key.

## Representative Columns

| Column | Description |
|---|---|
| `match_team_id` | Match-team identifier |
| `match_team_sk` | Deterministic surrogate key |
| `match_id` | Match identifier |
| `match_sk` | Conformed match key |
| `team_id` | Team identifier |
| `team_sk` | Conformed team key |
| `team_number` | Side number |
| `winner_flag` | Derived winner indicator |
| `pre_match_team_rating` | Rating before match where supplied |
| `post_match_team_rating` | Rating after match where supplied |
| `rating_change` | Objective change where derivable |

## Derived Columns

- `winner_flag`;
- `rating_change` where both source values exist;
- conformed keys.

## Validation Rules

- match exists;
- team exists;
- team number is valid;
- normally exactly two participating sides per completed match;
- no team appears on both sides of the same match;
- winner consistency with `matches`;
- rating values within configured ranges.

## Reject Conditions

Reject when:

- match is orphaned;
- team is orphaned;
- required side number is invalid;
- duplicate business key cannot be resolved;
- structural relationship is impossible.

---

# 47. Silver Table — Match Team Players

## Business Purpose

The `match_team_players` table defines the players participating for a specific team within a specific match.

## Business Grain

One player within one match-team record.

## Bronze Source

```text
bronze.match_team_players
```

## Primary Key

```text
match_team_player_id
```

## Foreign Keys

```text
match_team_id → match_teams.match_team_id
player_id     → players.player_id
```

## Required Transformations

- validate player;
- validate match-team;
- enrich match and team identifiers from `match_teams`;
- verify player membership in the team on the match date where history supports the check;
- generate deterministic surrogate key.

## Representative Columns

| Column | Description |
|---|---|
| `match_team_player_id` | Participant identifier |
| `match_team_player_sk` | Deterministic surrogate key |
| `match_team_id` | Match-team identifier |
| `match_team_sk` | Conformed match-team key |
| `match_id` | Denormalized match identifier |
| `match_sk` | Conformed match key |
| `team_id` | Denormalized team identifier |
| `team_sk` | Conformed team key |
| `player_id` | Player identifier |
| `player_sk` | Conformed player key |
| `player_position` | Position where supplied |
| `player_rating_at_match` | Rating at match where supplied |

## Derived Columns

No analytical values are required.

Conformed keys and denormalized operational identifiers may be added to simplify downstream joins.

## Validation Rules

- valid player;
- valid match-team;
- player is not duplicated on the same match side;
- player does not appear on both sides of the same match;
- normally two players per match side;
- membership-history consistency checked where possible;
- rating-at-match within configured range where supplied.

## Reject Conditions

Reject when:

- player is orphaned;
- match-team is orphaned;
- required identifier is missing;
- player appears in an impossible duplicate relationship;
- duplicate business key cannot be resolved.

Membership-history mismatches may be warnings or errors based on configuration.

---

# 48. Silver Table — Match Games

## Business Purpose

The `match_games` table represents one game within a pickleball match.

It contains the most detailed competition result information available in Silver.

## Business Grain

One game.

## Bronze Source

```text
bronze.match_games
```

## Primary Key

```text
match_game_id
```

## Foreign Keys

```text
match_id → matches.match_id
```

## Required Transformations

- standardize scores;
- validate winner;
- compute score margin;
- compute total points;
- derive close-game and extended-game flags;
- validate score-share fields where supplied;
- generate deterministic surrogate key.

## Representative Columns

| Column | Description |
|---|---|
| `match_game_id` | Game identifier |
| `match_game_sk` | Deterministic surrogate key |
| `match_id` | Match identifier |
| `match_sk` | Conformed match key |
| `game_number` | Sequence within match |
| `team_one_score` | Team-one score |
| `team_two_score` | Team-two score |
| `winning_team_number` | Winning side |
| `target_score` | Target score where supplied |
| `win_by` | Win-by requirement where supplied |
| `actual_team_one_score_share` | Source score share where supplied |
| `score_margin` | Absolute score difference |
| `total_points` | Sum of both scores |
| `close_game_flag` | Configured close-game indicator |
| `extended_game_flag` | Winning score exceeded target |

## Derived Columns

```text
score_margin = abs(team_one_score - team_two_score)
```

```text
total_points = team_one_score + team_two_score
```

```text
close_game_flag = score_margin <= configured_threshold
```

```text
extended_game_flag = greatest(team_one_score, team_two_score) > target_score
```

These are objective reusable facts and are appropriate for Silver.

## Validation Rules

- valid match;
- game number is positive;
- game number is unique within match;
- scores are non-negative;
- winner is consistent with scores;
- score share reconciles within configured tolerance where supplied;
- game sequence is contiguous where required;
- completed matches have a plausible number of games.

## Reject Conditions

Reject when:

- match is orphaned;
- required identifier is missing;
- winner is inconsistent with scores;
- required score cannot be cast;
- scores are impossible;
- duplicate business key cannot be resolved.

---

# 49. Cross-Table Validation

After every Silver table has been published, perform cross-table validation.

These validations confirm that the business model remains internally consistent.

## 49.1 Team Membership Validation

Every active team should normally contain two active players.

Exceptions should be logged rather than automatically corrected.

Validation outputs should identify:

- team identifier;
- release as-of date;
- active member count;
- expected count;
- status.

## 49.2 Match Validation

Every completed match should normally contain:

- two `match_teams`;
- a valid winner;
- one or more `match_games`.

## 49.3 Match-Team Validation

Every match-team should normally contain two participating players.

## 49.4 Player Validation

Every referenced player must exist in accepted `players`.

## 49.5 Region Validation

Every required referenced region must exist in accepted `regions`.

## 49.6 Batch Validation

Every batch-dependent operational record must reference a valid monthly batch where required.

## 49.7 Winner Consistency

Validate consistency among:

- `matches.winning_team_number`;
- `match_teams.winner_flag`;
- aggregate game winners in `match_games`.

Do not silently overwrite conflicting source values.

## 49.8 Membership-at-Match Validation

Where membership dates permit the check, validate that a player was a member of the participating team on the match date.

The severity should be configuration-driven.

---

# 50. Operational Views

The Silver layer may expose reusable convenience views.

These views simplify downstream joins without introducing analytical calculations.

Recommended views include:

- `vw_players_current`;
- `vw_team_rosters`;
- `vw_current_team_memberships`;
- `vw_match_results`;
- `vw_player_match_history`.

These views shall contain only reusable operational data.

## 50.1 `vw_team_rosters`

Should expose current members per team and include roster-cardinality status.

## 50.2 `vw_match_results`

Should expose one row per match with:

- both teams;
- winner;
- game count;
- aggregate score by side;
- category;
- region;
- batch.

## 50.3 `vw_player_match_history`

Should expose one row per player-match participation with:

- player;
- team;
- opponent;
- match date;
- category;
- win or loss result;
- rating at match where supplied;
- region;
- batch.

Do not add trend scores, chemistry, fatigue, or roster recommendations.

---

# 51. Publication Standards

Every Silver table shall be published only after:

- validation completed;
- reconciliation completed;
- metadata added;
- reject table written or staged;
- critical rule thresholds passed.

Publication shall occur using full replacement.

Recommended patterns include:

```python
(
    dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
)
```

or:

```sql
CREATE OR REPLACE TABLE target_table AS
SELECT *
FROM validated_staging_view;
```

The implementation should use one consistent publication pattern.

Do not expose partially rebuilt Silver tables.

---

# 52. Reconciliation

Every table shall satisfy:

```text
Bronze Rows
=
Accepted Rows
+ Rejected Rows
+ Exact Duplicate Rows
+ Duplicate Business-Key Loser Rows
```

Any imbalance represents a pipeline failure.

Rows failing multiple rules must be counted once in rejected-row reconciliation while retaining all rule findings in the quality-results detail.

---

# 53. Silver Completion Checklist

Every Silver table shall satisfy the following before publication.

## 53.1 Structure

- explicit schema;
- standard column names;
- documented grain;
- primary key defined;
- metadata columns appended.

## 53.2 Quality

- duplicate resolution completed;
- validation completed;
- rejects captured;
- warning metrics recorded.

## 53.3 Integrity

- foreign keys validated;
- required fields validated;
- domain values standardized;
- cross-table checks passed or documented.

## 53.4 Publication

- target table published using full refresh;
- reconciliation balanced;
- operational metrics written;
- rerun produces identical business data.

---

---

# 54. Complete Configuration Model

## 54.1 Configuration Responsibilities

The configuration layer defines the variable behavior of the pipeline. The reusable PySpark code defines how that behavior is executed.

Configuration shall control:

- active release;
- source and target namespaces;
- source inventory;
- source-to-target mappings;
- build stages and execution order;
- target primary keys;
- foreign-key relationships;
- column mappings and target types;
- domain normalization;
- duplicate-handling policies;
- data-quality rules and severities;
- reject-table behavior;
- metadata columns;
- publication settings;
- operational logging;
- performance options.

Complex transformation algorithms shall remain in tested Python functions. YAML shall identify the function to call and provide parameters; it shall not contain arbitrary executable code.

## 54.2 Configuration File Set

```text
config/
├── base.yml
├── environments/
│   ├── napa_5k.yml
│   ├── napa_50k.yml
│   └── napa_250k.yml
├── sources.yml
├── silver_tables.yml
├── quality_rules.yml
├── domains.yml
└── logging.yml
```

The configuration loader shall:

1. load `base.yml`;
2. load the selected environment override;
3. load the shared registries and rule files;
4. deep-merge the configuration;
5. resolve approved placeholders;
6. validate the final structure;
7. calculate a configuration hash;
8. return an immutable or read-only configuration object to pipeline code.

The loader must reject:

- unknown processing modes;
- unresolved placeholders;
- duplicate build-order values where order must be unique;
- undefined source references;
- undefined transform names;
- unsupported rule types;
- missing required keys;
- unsupported release names.

---

# 55. `base.yml`

The base file contains settings shared across all releases.

```yaml
project:
  name: napa_olympic_analytics
  pipeline_name: bronze_to_silver
  pipeline_version: "1.0.0"
  processing_mode: full_refresh

runtime:
  catalog: workspace
  team_prefix: napa
  default_release: napa_5k
  timezone: America/Toronto

schemas:
  raw: "${team_prefix}_raw"
  bronze: "${release_name}_bronze"
  silver: "${release_name}_silver"
  silver_reject: "${release_name}_silver_reject"
  stage: "${release_name}_stage"
  operations: "${team_prefix}_ops"

execution:
  fail_fast: true
  publish_only_after_validation: true
  stop_dependents_on_failure: true
  unexpected_column_policy: warn
  missing_required_column_policy: fail
  duplicate_strategy_default: keep_best
  optimize_after_publish: false
  vacuum_after_publish: false
  create_convenience_views: true

metadata:
  add_pipeline_run_id: true
  add_pipeline_version: true
  add_source_dataset: true
  add_source_table: true
  add_load_timestamp: true
  add_record_hash: true
  add_quality_status: true

thresholds:
  close_game_margin: 2
  score_share_tolerance: 0.0001
  warning_quality_deduction: 5
  expected_match_team_count: 2
  expected_match_team_player_count: 2

publication:
  format: delta
  mode: overwrite
  overwrite_schema: true
  use_staging: true
```

This convention produces separate namespaces such as:

```text
workspace.napa_5k_bronze
workspace.napa_5k_silver
workspace.napa_50k_bronze
workspace.napa_50k_silver
workspace.napa_250k_bronze
workspace.napa_250k_silver
workspace.napa_ops
```

The implementation shall choose one naming convention and apply it consistently.

---

# 56. Release Environment Configuration

Each environment file contains only release-specific values.

## 56.1 `environments/napa_5k.yml`

```yaml
release:
  release_name: napa_5k
  expected_player_scale: 5000
  role: development
  final_decision_dataset: false

paths:
  raw_dataset_path: "/Volumes/workspace/napa_raw/napa_files/napa_5k"

performance:
  shuffle_partitions: 16
  broadcast_row_threshold: 100000
```

## 56.2 `environments/napa_50k.yml`

```yaml
release:
  release_name: napa_50k
  expected_player_scale: 50000
  role: validation
  final_decision_dataset: false

paths:
  raw_dataset_path: "/Volumes/workspace/napa_raw/napa_files/napa_50k"

performance:
  shuffle_partitions: 64
  broadcast_row_threshold: 250000
```

## 56.3 `environments/napa_250k.yml`

```yaml
release:
  release_name: napa_250k
  expected_player_scale: 250000
  role: production
  final_decision_dataset: true

paths:
  raw_dataset_path: "/Volumes/workspace/napa_raw/napa_files/napa_250k"

performance:
  shuffle_partitions: auto
  broadcast_row_threshold: 500000
```

The three files shall not contain different transformation logic, table mappings, or business rules. They represent three scale instances of the same architecture.

---

# 57. `sources.yml`

The source registry defines all thirteen Bronze inputs.

```yaml
sources:
  regions:
    enabled: true
    source_file: regions.parquet
    bronze_table: regions
    grain: one row per geographic region
    natural_key: [id]

  clubs:
    enabled: true
    source_file: clubs.parquet
    bronze_table: clubs
    grain: one row per club or facility
    natural_key: [id]

  club_memberships:
    enabled: true
    source_file: club_memberships.parquet
    bronze_table: club_memberships
    grain: one player-club membership period
    natural_key: [id]

  player_master:
    enabled: true
    source_file: player_master.parquet
    bronze_table: player_master
    grain: one current or snapshot row per athlete
    natural_key: [player_id]

  player_registrations:
    enabled: true
    source_file: player_registrations.parquet
    bronze_table: player_registrations
    grain: one player registration event
    natural_key: [id]

  player_assessment_history:
    enabled: true
    source_file: player_assessment_history.parquet
    bronze_table: player_assessment_history
    grain: one athlete assessment observation
    natural_key: [id]

  teams:
    enabled: true
    source_file: teams.parquet
    bronze_table: teams
    grain: one doubles partnership or team
    natural_key: [id]

  team_memberships:
    enabled: true
    source_file: team_memberships.parquet
    bronze_table: team_memberships
    grain: one player-team membership period
    natural_key: [id]

  matches:
    enabled: true
    source_file: matches.parquet
    bronze_table: matches
    grain: one match
    natural_key: [id]

  match_teams:
    enabled: true
    source_file: match_teams.parquet
    bronze_table: match_teams
    grain: one team or side in a match
    natural_key: [id]

  match_team_players:
    enabled: true
    source_file: match_team_players.parquet
    bronze_table: match_team_players
    grain: one player participating on a match side
    natural_key: [id]

  match_games:
    enabled: true
    source_file: match_games.parquet
    bronze_table: match_games
    grain: one game within a match
    natural_key: [id]

  monthly_batches:
    enabled: true
    source_file: monthly_batches.parquet
    bronze_table: monthly_batches
    grain: one processing or snapshot period
    natural_key: [id]
```

The Bronze validation task shall compare this registry to:

- the source files in the selected raw release;
- the Bronze tables in the selected Bronze schema;
- the actual table schemas.

A missing configured source is a critical failure. An unexpected source is reported for instructor review and is not silently incorporated into Silver.

---

# 58. `silver_tables.yml`

The Silver registry identifies source-to-target behavior.

```yaml
silver_tables:
  monthly_batches:
    enabled: true
    source: monthly_batches
    target: monthly_batches
    stage: reference
    build_order: 10
    transform: build_monthly_batches
    primary_key: [batch_id]
    source_key_mapping: {id: batch_id}
    reject_table: monthly_batches_exceptions

  regions:
    enabled: true
    source: regions
    target: regions
    stage: reference
    build_order: 20
    transform: build_regions
    primary_key: [region_id]
    source_key_mapping: {id: region_id}
    reject_table: regions_exceptions

  players:
    enabled: true
    source: player_master
    target: players
    stage: athlete
    build_order: 30
    transform: build_players
    primary_key: [player_id]
    reject_table: players_exceptions

  clubs:
    enabled: true
    source: clubs
    target: clubs
    stage: organization
    build_order: 40
    transform: build_clubs
    primary_key: [club_id]
    source_key_mapping: {id: club_id}
    reject_table: clubs_exceptions

  teams:
    enabled: true
    source: teams
    target: teams
    stage: partnership
    build_order: 50
    transform: build_teams
    primary_key: [team_id]
    source_key_mapping: {id: team_id}
    reject_table: teams_exceptions

  player_registrations:
    enabled: true
    source: player_registrations
    target: player_registrations
    stage: athlete
    build_order: 60
    transform: build_player_registrations
    primary_key: [registration_id]
    source_key_mapping: {id: registration_id}
    reject_table: player_registrations_exceptions

  player_assessment_history:
    enabled: true
    source: player_assessment_history
    target: player_assessment_history
    stage: athlete
    build_order: 70
    transform: build_player_assessment_history
    primary_key: [assessment_id]
    source_key_mapping: {id: assessment_id}
    reject_table: player_assessment_history_exceptions

  club_memberships:
    enabled: true
    source: club_memberships
    target: club_memberships
    stage: organization
    build_order: 80
    transform: build_club_memberships
    primary_key: [club_membership_id]
    source_key_mapping: {id: club_membership_id}
    reject_table: club_memberships_exceptions

  team_memberships:
    enabled: true
    source: team_memberships
    target: team_memberships
    stage: partnership
    build_order: 90
    transform: build_team_memberships
    primary_key: [team_membership_id]
    source_key_mapping: {id: team_membership_id}
    reject_table: team_memberships_exceptions

  matches:
    enabled: true
    source: matches
    target: matches
    stage: competition
    build_order: 100
    transform: build_matches
    primary_key: [match_id]
    source_key_mapping: {id: match_id}
    reject_table: matches_exceptions

  match_teams:
    enabled: true
    source: match_teams
    target: match_teams
    stage: competition
    build_order: 110
    transform: build_match_teams
    primary_key: [match_team_id]
    source_key_mapping: {id: match_team_id}
    reject_table: match_teams_exceptions

  match_team_players:
    enabled: true
    source: match_team_players
    target: match_team_players
    stage: competition
    build_order: 120
    transform: build_match_team_players
    primary_key: [match_team_player_id]
    source_key_mapping: {id: match_team_player_id}
    reject_table: match_team_players_exceptions

  match_games:
    enabled: true
    source: match_games
    target: match_games
    stage: competition
    build_order: 130
    transform: build_match_games
    primary_key: [match_game_id]
    source_key_mapping: {id: match_game_id}
    reject_table: match_games_exceptions
```

Every transform name shall be resolved through a controlled transform registry. Do not execute arbitrary function names using `eval`.

---

# 59. `domains.yml`

Domain values shall be confirmed against actual source values during profiling.

```yaml
domains:
  country_code:
    allowed: [USA, CAN]
    synonyms:
      US: USA
      UNITED STATES: USA
      UNITED STATES OF AMERICA: USA
      CA: CAN
      CANADA: CAN

  gender:
    allowed: [M, F]
    synonyms:
      MALE: M
      FEMALE: F

  dominant_hand:
    allowed: [LEFT, RIGHT, AMBIDEXTROUS]
    synonyms:
      L: LEFT
      R: RIGHT
      BOTH: AMBIDEXTROUS

  player_status:
    allowed: [ACTIVE, INACTIVE]

  team_status:
    allowed: [ACTIVE, INACTIVE, DISSOLVED]

  team_type:
    allowed: [MENS, WOMENS, MIXED]

  player_position:
    allowed: [LEFT, RIGHT]
```

These examples are not permission to invent source-domain values. The actual distinct values must be inspected and documented.

---

# 60. `quality_rules.yml`

Quality rules shall be identified by stable rule IDs.

```yaml
quality_rules:
  regions:
    - id: REGION_001
      type: not_null
      columns: [region_id, region_name, country_code]
      severity: CRITICAL

    - id: REGION_002
      type: unique
      columns: [region_id]
      severity: CRITICAL

    - id: REGION_003
      type: allowed_values
      column: country_code
      domain: country_code
      severity: ERROR

  players:
    - id: PLAYER_001
      type: not_null
      columns: [player_id]
      severity: CRITICAL

    - id: PLAYER_002
      type: unique
      columns: [player_id]
      severity: CRITICAL

    - id: PLAYER_003
      type: foreign_key
      columns: [home_region_id]
      parent_table: regions
      parent_columns: [region_id]
      allow_null: false
      severity: ERROR

    - id: PLAYER_004
      type: date_not_after
      column: birth_date
      comparison_value: "${release_as_of_date}"
      allow_null: true
      severity: ERROR

  team_memberships:
    - id: TEAM_MEMBER_001
      type: foreign_key
      columns: [team_id]
      parent_table: teams
      parent_columns: [team_id]
      severity: CRITICAL

    - id: TEAM_MEMBER_002
      type: foreign_key
      columns: [player_id]
      parent_table: players
      parent_columns: [player_id]
      severity: CRITICAL

    - id: TEAM_MEMBER_003
      type: date_order
      start_column: joined_date
      end_column: left_date
      allow_null_end: true
      severity: ERROR

  match_games:
    - id: GAME_001
      type: foreign_key
      columns: [match_id]
      parent_table: matches
      parent_columns: [match_id]
      severity: CRITICAL

    - id: GAME_002
      type: range
      column: game_number
      min: 1
      severity: ERROR

    - id: GAME_003
      type: expression
      expression: >
        (winning_team_number = 1 AND team_one_score > team_two_score)
        OR
        (winning_team_number = 2 AND team_two_score > team_one_score)
      severity: ERROR
```

The framework shall support the rule types described earlier in this specification.

---

# 61. Configuration Validation

Before the Workflow builds Silver, it shall validate:

- selected release is one of `napa_5k`, `napa_50k`, or `napa_250k`;
- processing mode is `full_refresh`;
- all thirteen enabled sources are defined;
- all source files or Bronze tables are present;
- every Silver target references an enabled source;
- all transform names exist;
- all primary keys are defined;
- all rule IDs are unique;
- parent tables exist for all foreign-key rules;
- build order respects dependencies;
- target schemas are within the configured catalog;
- no unresolved `${...}` values remain.

The resolved configuration and its SHA-256 hash shall be written to the operations layer for every run.

---

# 62. Databricks Workflow Design

## 62.1 Responsibility Split

The Databricks Workflow controls:

- task execution;
- dependencies;
- retries;
- task parameters;
- success and failure state;
- run history;
- manual or scheduled triggering.

For Databricks Free Edition, this Workflow must run on serverless compute only. The implementation must not require `existing_cluster_id`, job clusters, or any other classic-compute configuration.

The configuration files control:

- active release;
- schemas and paths;
- enabled sources and targets;
- target build order;
- transformations;
- data-quality rules;
- domain mappings;
- publication behavior.

The Workflow does not replace configuration, and configuration does not replace orchestration.

## 62.2 Workflow Parameter

Define a job-level parameter:

```text
release_name
```

Allowed values:

```text
napa_5k
napa_50k
napa_250k
```

Optional parameters:

```text
team_prefix
config_root
pipeline_version
```

For Python script tasks on Databricks Free Edition, each task should reference a job-level serverless environment definition rather than a cluster definition.

## 62.3 Recommended Workflow Tasks

```text
01_resolve_configuration
          ↓
02_validate_environment
          ↓
03_validate_bronze
          ↓
04_build_reference
          ↓
05_build_athlete
          ↓
06_build_organization_and_partnership
          ↓
07_build_competition
          ↓
08_run_cross_table_validation
          ↓
09_publish_convenience_views
          ↓
10_reconcile_and_publish_summary
```

### Task 01 — Resolve Configuration

Responsibilities:

- read the Workflow release parameter;
- load and validate YAML;
- determine catalog, schemas, and paths;
- calculate configuration hash;
- create `pipeline_run_id`;
- write the run-start record;
- expose the run ID and resolved configuration path to downstream tasks.

### Task 02 — Validate Environment

Responsibilities:

- confirm catalog access;
- create required schemas if authorized;
- verify source and target namespaces;
- confirm required Python modules import;
- confirm the selected release is supported.

### Task 03 — Validate Bronze

Responsibilities:

- confirm all thirteen Bronze tables exist;
- compare actual and configured schemas;
- capture Bronze row counts;
- capture schema snapshots;
- fail on missing required columns;
- publish source-contract findings.

### Task 04 — Build Reference

Targets:

- `monthly_batches`;
- `regions`.

These tables are parents for several later entities.

### Task 05 — Build Athlete

Targets:

- `players`;
- `player_registrations`;
- `player_assessment_history`.

`players` must complete before registration and assessment children.

### Task 06 — Build Organization and Partnership

Targets:

- `clubs`;
- `teams`;
- `club_memberships`;
- `team_memberships`.

The reference and player tasks must complete first.

### Task 07 — Build Competition

Targets:

- `matches`;
- `match_teams`;
- `match_team_players`;
- `match_games`.

The task processes tables according to configured build order and dependencies.

### Task 08 — Cross-Table Validation

Runs all relationship and cardinality rules that require multiple completed Silver tables.

### Task 09 — Convenience Views

Creates the approved reusable Silver views.

### Task 10 — Reconcile and Publish Summary

Responsibilities:

- verify table-level reconciliation;
- publish run metrics;
- mark the run successful or failed;
- display a concise result summary.

## 62.4 Failure Behavior

- A failed reference task prevents all dependent tasks.
- A failed athlete parent prevents child membership and participation processing.
- A failed competition parent prevents its child tables.
- The summary task shall run on success and, where Workflow settings allow, on failure to capture diagnostic status.
- Retries should be limited to infrastructure or transient failures. Deterministic data-quality failures should not be retried automatically without correction.

---

# 63. Running the Three Releases

## 63.1 One Reusable Workflow

Maintain one Workflow definition and run it with:

```text
release_name=napa_5k
release_name=napa_50k
release_name=napa_250k
```

Do not manually maintain three divergent copies of the Workflow.

## 63.2 Separate Materialized Medallion Instances

Recommended schemas:

```text
workspace.napa_5k_bronze
workspace.napa_5k_silver
workspace.napa_5k_silver_reject

workspace.napa_50k_bronze
workspace.napa_50k_silver
workspace.napa_50k_silver_reject

workspace.napa_250k_bronze
workspace.napa_250k_silver
workspace.napa_250k_silver_reject

workspace.napa_ops
```

Gold schemas may follow the same release-specific pattern.

## 63.3 Recommended Execution Sequence

For instructor validation, run:

```text
napa_5k
   ↓ success
napa_50k
   ↓ success
napa_250k
```

The 5K release acts as a fast development and smoke-test environment. The 50K release tests scaling and engineering controls. The 250K release proves the final reference implementation.

## 63.4 Cross-Release Consistency Checks

The implementation shall verify:

- identical source inventory;
- compatible source schemas;
- identical Silver target inventory;
- identical Silver target schemas;
- identical rule IDs and severities;
- identical transformation versions;
- release-specific data kept in separate namespaces;
- approximate scale increases where expected.

Final analytical conclusions remain outside this specification and should use the 250K release.

---

# 64. Operations Layer

A single shared operations schema shall record all releases.

## 64.1 `pipeline_runs`

```sql
CREATE TABLE IF NOT EXISTS workspace.napa_ops.pipeline_runs (
    pipeline_run_id STRING NOT NULL,
    pipeline_name STRING NOT NULL,
    pipeline_version STRING NOT NULL,
    release_name STRING NOT NULL,
    processing_mode STRING NOT NULL,
    configuration_hash STRING NOT NULL,
    workflow_run_id STRING,
    status STRING NOT NULL,
    started_ts TIMESTAMP NOT NULL,
    completed_ts TIMESTAMP,
    duration_seconds DOUBLE,
    triggered_by STRING,
    error_class STRING,
    error_message STRING
)
USING DELTA;
```

Recommended statuses:

```text
STARTED
VALIDATING
BUILDING
PUBLISHING
SUCCEEDED
FAILED
```

## 64.2 `table_runs`

```sql
CREATE TABLE IF NOT EXISTS workspace.napa_ops.table_runs (
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    source_table STRING NOT NULL,
    target_table STRING NOT NULL,
    build_stage STRING NOT NULL,
    build_order INT NOT NULL,
    status STRING NOT NULL,
    started_ts TIMESTAMP NOT NULL,
    completed_ts TIMESTAMP,
    duration_seconds DOUBLE,
    source_row_count BIGINT,
    exact_duplicate_count BIGINT,
    business_key_duplicate_count BIGINT,
    accepted_row_count BIGINT,
    rejected_row_count BIGINT,
    warning_count BIGINT,
    published_row_count BIGINT,
    error_message STRING
)
USING DELTA;
```

## 64.3 `quality_results`

```sql
CREATE TABLE IF NOT EXISTS workspace.napa_ops.quality_results (
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    target_table STRING NOT NULL,
    rule_id STRING NOT NULL,
    rule_type STRING NOT NULL,
    severity STRING NOT NULL,
    evaluated_row_count BIGINT,
    failed_row_count BIGINT,
    failure_pct DOUBLE,
    threshold_value STRING,
    status STRING NOT NULL,
    sample_business_keys ARRAY<STRING>,
    evaluated_ts TIMESTAMP NOT NULL
)
USING DELTA;
```

## 64.4 `reconciliation_results`

```sql
CREATE TABLE IF NOT EXISTS workspace.napa_ops.reconciliation_results (
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    source_table STRING NOT NULL,
    target_table STRING NOT NULL,
    bronze_row_count BIGINT NOT NULL,
    exact_duplicate_count BIGINT NOT NULL,
    business_key_loser_count BIGINT NOT NULL,
    rejected_row_count BIGINT NOT NULL,
    accepted_row_count BIGINT NOT NULL,
    reconciliation_difference BIGINT NOT NULL,
    status STRING NOT NULL,
    evaluated_ts TIMESTAMP NOT NULL
)
USING DELTA;
```

## 64.5 `schema_snapshots`

```sql
CREATE TABLE IF NOT EXISTS workspace.napa_ops.schema_snapshots (
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    layer_name STRING NOT NULL,
    table_name STRING NOT NULL,
    column_name STRING NOT NULL,
    data_type STRING NOT NULL,
    nullable BOOLEAN,
    ordinal_position INT,
    schema_hash STRING NOT NULL,
    captured_ts TIMESTAMP NOT NULL
)
USING DELTA;
```

## 64.6 `run_messages`

```sql
CREATE TABLE IF NOT EXISTS workspace.napa_ops.run_messages (
    pipeline_run_id STRING NOT NULL,
    release_name STRING NOT NULL,
    target_table STRING,
    message_level STRING NOT NULL,
    message_code STRING NOT NULL,
    message_text STRING NOT NULL,
    created_ts TIMESTAMP NOT NULL
)
USING DELTA;
```

Operations tables are append-oriented audit evidence even though business Silver tables use full refresh.

---

# 65. Reject Table Standard

Each Silver target shall publish an exception table in the release-specific reject schema.

Example:

```text
workspace.napa_50k_silver_reject.players_exceptions
```

Required columns:

```text
source_table
target_table
source_business_key
reject_reason_code
reject_reason_detail
rule_id
rule_severity
source_record_json
_pipeline_run_id
_source_dataset
_load_ts
_record_hash
```

A record failing multiple rules may produce one reject row containing an array of rule failures or multiple rule-detail rows associated with one rejected record. The reconciliation framework must count each rejected source record once.

Recommended reason codes:

```text
MISSING_PRIMARY_KEY
DUPLICATE_EXACT_RECORD
DUPLICATE_BUSINESS_KEY
INVALID_DATA_TYPE
INVALID_DOMAIN_VALUE
INVALID_DATE
INVALID_DATE_RANGE
VALUE_OUT_OF_RANGE
ORPHAN_FOREIGN_KEY
MEMBERSHIP_PERIOD_OVERLAP
INVALID_TEAM_CARDINALITY
INVALID_MATCH_SIDE_CARDINALITY
INVALID_PARTICIPANT_CARDINALITY
PLAYER_ON_BOTH_MATCH_SIDES
GAME_SCORE_WINNER_MISMATCH
MATCH_WINNER_MISMATCH
BATCH_RECONCILIATION_MISMATCH
UNEXPECTED_SCHEMA
MISSING_REQUIRED_COLUMN
```

---

# 66. Testing Strategy

## 66.1 Unit Tests

Unit-test reusable logic independently from notebooks.

Required areas:

- YAML loading;
- deep merge;
- placeholder substitution;
- configuration validation;
- string standardization;
- domain normalization;
- safe type casting;
- deterministic hashes;
- duplicate ranking;
- date ordering;
- age calculation at a fixed as-of date;
- membership-current logic;
- score-margin and total-point derivations;
- rule result aggregation;
- reconciliation arithmetic.

## 66.2 Integration Tests

For each Silver builder:

1. create a temporary Bronze test table;
2. load valid and invalid sample records;
3. execute the builder;
4. verify accepted records;
5. verify rejected records and reason codes;
6. verify metadata columns;
7. verify deterministic rerun behavior;
8. remove test objects.

Each table must test at least:

- valid record;
- missing primary key;
- exact duplicate;
- conflicting duplicate business key;
- invalid domain;
- orphan foreign key where applicable;
- invalid date;
- invalid numeric range where applicable.

## 66.3 Cross-Table Tests

Required scenarios:

- club references a missing region;
- membership references a missing player;
- team has an unexpected active-member count;
- match has fewer or more than two sides;
- match side has an unexpected player count;
- player appears on both sides;
- game winner conflicts with scores;
- match winner conflicts with participating teams;
- batch count reconciliation fails.

## 66.4 Release Acceptance Tests

### 5K

- all thirteen sources detected;
- all configured Silver tables created;
- quality and reconciliation records produced;
- rerun produces identical business rows and hashes;
- no code changes required.

### 50K

- only release configuration changes;
- same Silver schemas;
- same rule definitions;
- successful scaling from 5K;
- performance recorded.

### 250K

- only release configuration changes;
- complete full-refresh execution;
- no driver-side collection of full datasets;
- all target tables and views query successfully;
- execution evidence written to operations tables.

## 66.5 Determinism Test

For two runs against unchanged Bronze tables and configuration:

- compare target row counts;
- compare primary-key sets;
- compare `_record_hash` values;
- compare an aggregate hash of sorted business columns;
- ignore run-specific metadata.

Differences constitute a failed determinism test.

---

# 67. Performance and Delta Guidance

## 67.1 General Principles

- use Spark DataFrames and Spark SQL;
- avoid pandas for pipeline-scale data;
- avoid `collect()` on large datasets;
- select required columns before joins;
- broadcast only measured small reference tables;
- cache only DataFrames reused enough to justify it;
- unpersist cached DataFrames;
- avoid unnecessary repartitioning;
- use configuration for performance settings.

## 67.2 Partitioning

Do not partition small Silver tables.

For large competition tables, begin with no explicit partitioning. Introduce partitioning only after measuring query and write behavior. Candidate columns may include release-independent temporal values such as match year, but high-cardinality or tiny partitions must be avoided.

## 67.3 `OPTIMIZE` and `VACUUM`

Both operations shall be disabled by default and configuration-controlled.

Do not disable Delta retention safeguards. The reference implementation does not require aggressive vacuuming.

## 67.4 Full-Refresh Publication

Preferred publication:

```python
(
    validated_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_fqn)
)
```

Where staging is enabled:

1. write the run-scoped staging table;
2. validate staging;
3. replace the final target;
4. remove staging after successful publication.

---

# 68. Security and Governance

Although the dataset is synthetic, apply professional controls:

- no credentials in code or YAML;
- no personal local paths;
- least-privilege schema permissions;
- source data read-only to transformation tasks;
- destructive writes restricted to configured target schemas;
- table and column comments where supported;
- pipeline code versioned in GitHub;
- configuration changes reviewed through Git;
- operations history retained;
- no hidden simulation parameters exposed in Silver;
- no roster recommendations generated by the engineering pipeline.

---

# 69. Runbook

## 69.1 Prerequisites

- repository synchronized to the intended branch;
- Databricks catalog and schemas available;
- all thirteen Parquet files uploaded for the selected release;
- Bronze tables created and validated;
- YAML files committed;
- Workflow configured for serverless compute only;
- reusable Python modules import successfully.

## 69.2 Run a Single Release

1. Open the NAPA Bronze-to-Silver Workflow.
2. Select **Run now with different parameters**.
3. Set `release_name` to `napa_5k`, `napa_50k`, or `napa_250k`.
4. Start the run.
5. Monitor task status.
6. Review the final summary task.
7. Query `pipeline_runs`, `table_runs`, and `reconciliation_results`.
8. Confirm the correct release-specific Silver schema was refreshed.

## 69.3 Investigate Failure

1. Identify the failed Workflow task.
2. Query the latest `pipeline_runs` record.
3. Query failed `table_runs`.
4. Review `run_messages`.
5. Review relevant `quality_results`.
6. Inspect the target exception table.
7. Correct configuration, code, or source problem.
8. Commit the correction.
9. Rerun the complete release build.

Do not manually patch Silver records.

## 69.4 Run All Releases

Recommended sequence:

```text
napa_5k → napa_50k → napa_250k
```

Do not proceed to a larger release while a smaller release has unresolved structural failures.

---

# 70. Final Definition of Done

The Bronze-to-Silver reference implementation is complete when all of the following are true.

## Architecture

- one shared codebase processes all three releases;
- one reusable Workflow definition is used;
- the Workflow is deployable on Databricks Free Edition serverless compute without cluster IDs or job-cluster settings;
- release selection is parameterized;
- separate Silver and reject schemas exist for each release;
- one shared operations schema captures all runs;
- processing is full refresh only.

## Configuration

- base, environment, source, table, domain, quality, and logging configurations exist;
- configuration validation is implemented;
- no environment-specific paths are duplicated in transformation code;
- configuration hash is recorded for every run.

## Source Control

- all thirteen authoritative source tables are configured;
- actual schemas have been inspected;
- source-contract differences are documented;
- source inventory validation is automated.

## Silver Tables

- every configured target has a documented grain;
- primary keys are unique;
- required foreign keys are validated;
- standardization and transformations are implemented;
- deterministic surrogate keys and record hashes are used;
- exception tables are created;
- full-refresh publication works;
- convenience views are created where enabled.

## Data Quality and Operations

- rule results are persisted;
- rejects contain explicit reason codes;
- table reconciliation balances;
- cross-table checks execute;
- operations tables are populated;
- failed runs are marked correctly;
- no successful status is recorded for incomplete builds.

## Testing

- unit tests pass;
- integration tests pass;
- cross-table tests pass;
- 5K acceptance run passes;
- 50K acceptance run passes;
- 250K acceptance run passes;
- determinism test passes.

## Documentation

- architecture documented;
- source-to-target lineage documented;
- Silver data dictionary completed;
- quality-rule catalog completed;
- runbook completed;
- assumptions and source deviations documented.

## Boundary

- no Gold scoring or recommendations are implemented in Silver;
- no hidden simulation parameters are included;
- no incremental logic is implemented.

---

# 71. Codex Implementation Instructions

## 71.1 Primary Prompt

Act as a Senior Data Platform Engineer specializing in Databricks, PySpark, Delta Lake, configuration-driven pipelines, and data-quality engineering.

Implement the NAPA Bronze-to-Silver reference architecture described in this specification.

The implementation is instructor reference code. It must be readable, deterministic, full-refresh, configuration-driven, tested, and maintainable.

## 71.2 Mandatory Discovery

Before coding:

1. inspect the repository;
2. identify existing conventions and utilities;
3. inventory the actual Bronze tables;
4. inspect all physical schemas;
5. profile representative values and null patterns;
6. compare physical fields to this specification;
7. identify assumptions requiring instructor review;
8. provide a concise implementation plan.

The physical data is authoritative. Do not invent missing columns.

## 71.3 Mandatory Design

Implement:

- configuration loader and validator;
- release-aware pipeline context;
- reusable standardization utilities;
- deterministic hash and key utilities;
- duplicate-resolution framework;
- configurable validation engine;
- reject writer;
- Delta full-refresh publisher;
- operations writers;
- table reconciliation;
- cross-table validation;
- table-specific transform modules;
- thin Workflow notebooks;
- automated tests;
- documentation updates.

## 71.4 Workflow Requirement

Use Databricks Workflow tasks for orchestration.

The Workflow accepts `release_name` and invokes the same code for:

```text
napa_5k
napa_50k
napa_250k
```

Do not build a separate maintained code branch or separate transformation implementation for each release.

Assume Databricks Free Edition is serverless-only when defining the Workflow or bundle. Do not propose classic compute, all-purpose clusters, or job-cluster-based execution for this repository.

## 71.5 Prohibited Implementation

Do not implement:

- incremental loading;
- Delta `MERGE`;
- CDC;
- streaming;
- Auto Loader;
- watermark logic;
- append-only Silver facts;
- random surrogate keys;
- non-deterministic row numbering;
- arbitrary `eval`;
- pandas-based pipeline transformations;
- hard-coded personal paths;
- silently dropped records;
- fabricated source fields;
- fabricated validation results;
- Gold analytics;
- Olympic roster selection.

## 71.6 Coding Standards

- use type hints;
- document public functions;
- use small focused functions;
- define explicit exceptions;
- preserve root errors;
- avoid bare `except`;
- avoid large driver collections;
- avoid repeated logic;
- centralize configuration;
- validate inputs defensively;
- use clear business names;
- write comments that explain decisions, not obvious syntax.

## 71.7 Required Completion Report

At completion, report:

- files created or changed;
- Bronze tables and schemas discovered;
- source-to-target mappings implemented;
- configuration files created;
- Workflow tasks created;
- Silver tables and views created;
- exception and operations tables created;
- tests executed and actual results;
- performance observations by release;
- assumptions requiring review;
- divergences from this specification;
- exact execution instructions.

Do not claim that Databricks execution or tests passed unless they were actually run.

---

# Appendix A — Authoritative Source-to-Silver Matrix

| Bronze Source | Silver Target | Grain | Core Parents |
|---|---|---|---|
| `regions` | `regions` | One geographic region | None |
| `clubs` | `clubs` | One club or facility | `regions` |
| `club_memberships` | `club_memberships` | One player-club membership period | `players`, `clubs` |
| `player_master` | `players` | One conformed player | `regions`, `monthly_batches` where referenced |
| `player_registrations` | `player_registrations` | One registration event | `players`, `monthly_batches`, `regions` where referenced |
| `player_assessment_history` | `player_assessment_history` | One assessment observation | `players`, `monthly_batches` |
| `teams` | `teams` | One doubles team | None |
| `team_memberships` | `team_memberships` | One player-team membership period | `players`, `teams` |
| `matches` | `matches` | One match | `regions`, `monthly_batches` |
| `match_teams` | `match_teams` | One side in a match | `matches` |
| `match_team_players` | `match_team_players` | One player on a match side | `match_teams`, `players` |
| `match_games` | `match_games` | One game in a match | `matches` |
| `monthly_batches` | `monthly_batches` | One processing or snapshot period | None |

The representative source schema shows `match_teams` as a match-side record with `match_id`, `team_number`, `team_score`, and `average_team_rating`. It does not necessarily contain a persistent `team_id`. The implementation must use the actual physical schema and must not require a `team_id` foreign key unless the delivered data includes it.

---

# Appendix B — Recommended Reviewer SQL

## Latest Pipeline Runs

```sql
SELECT *
FROM workspace.napa_ops.pipeline_runs
ORDER BY started_ts DESC
LIMIT 20;
```

## Table Results for a Run

```sql
SELECT *
FROM workspace.napa_ops.table_runs
WHERE pipeline_run_id = '<pipeline_run_id>'
ORDER BY build_order;
```

## Reconciliation Failures

```sql
SELECT *
FROM workspace.napa_ops.reconciliation_results
WHERE pipeline_run_id = '<pipeline_run_id>'
  AND status <> 'PASSED';
```

## Critical Quality Failures

```sql
SELECT *
FROM workspace.napa_ops.quality_results
WHERE pipeline_run_id = '<pipeline_run_id>'
  AND severity = 'CRITICAL'
  AND failed_row_count > 0;
```

## Match-Side Cardinality

```sql
SELECT match_id, COUNT(*) AS side_count
FROM workspace.napa_250k_silver.match_teams
GROUP BY match_id
HAVING COUNT(*) <> 2;
```

## Player Cardinality by Match Side

```sql
SELECT match_team_id, COUNT(*) AS player_count
FROM workspace.napa_250k_silver.match_team_players
GROUP BY match_team_id
HAVING COUNT(*) <> 2;
```

## Game Winner Consistency

```sql
SELECT *
FROM workspace.napa_250k_silver.match_games
WHERE (winning_team_number = 1 AND team_one_score <= team_two_score)
   OR (winning_team_number = 2 AND team_two_score <= team_one_score);
```

---

# Appendix C — Silver/Gold Boundary

The Silver layer answers:

> What standardized, validated operational facts and relationships does NAPA have?

The Gold layer answers:

> What do those facts imply for athlete evaluation, partnership performance, development potential, and Olympic roster decisions?

Maintain this boundary throughout the implementation.

---

# Appendix D — Final Architecture Summary

```text
One Git repository
One shared pipeline codebase
One Databricks Workflow definition
One shared configuration model
Three release environment configurations
Three Bronze schemas
Three Silver schemas
Three Silver exception schemas
One shared operations schema
Full-refresh execution only
Thirteen authoritative source tables
No Gold analytics in Silver
```
