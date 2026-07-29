# NAPA Databricks Raw Student Data Certification
## Specification and Codex Implementation Plan

**Version:** 1.0 Proposed  
**Status:** Instructor reference architecture specification  
**Target platform:** Databricks  
**Certification boundary:** Student-facing Raw layer only  
**Supported release sizes:** `5k`, `50k`, and `250k`

---

## 1. Document Purpose

This document specifies a parameterized certification process for the NAPA student datasets after they have been exported and landed in the Databricks Raw layer.

The certification process must determine whether the exact student-facing release is:

1. complete and readable;
2. structurally coherent;
3. internally consistent;
4. operationally plausible;
5. sufficiently rich for its assigned course milestone;
6. suitable for downstream student engineering and analytics; and
7. safe to release without relying on the instructor Raw-to-Bronze reference pipeline.

This specification is intended to be provided directly to Codex for implementation in the NAPA pipeline repository.

---

## 2. Executive Design Decision

The official student-data certification point shall be the Databricks Raw layer.

The certification workflow shall read the exact student-facing Parquet release from its configured Raw landing location. It may create temporary Spark views or DataFrames for query execution, but it shall not depend on Bronze, Silver, or Gold tables.

```text
PostgreSQL generation
        |
        v
PostgreSQL release certification
        |
        v
Student Parquet export
        |
        v
Databricks Raw landing
        |
        v
RAW STUDENT DATA CERTIFICATION
        |
        +---- REJECTED -> correct generator/export and rerun
        |
        +---- CERTIFIED -> Raw-to-Bronze reference pipeline
```

This boundary intentionally removes the instructor Raw-to-Bronze implementation as a variable in deciding whether the source dataset delivered to students is fit for use.

A later Raw-to-Bronze validation process may verify ingestion fidelity, but it is a separate concern and must not be used to make an unusable Raw dataset appear acceptable.

---

## 3. Relationship to the PostgreSQL Release Certification Framework

The PostgreSQL and Databricks certification processes answer related but distinct questions.

| Certification stage | Primary question |
| --- | --- |
| PostgreSQL release certification | Did the simulator generate the intended synthetic world? |
| Databricks Raw certification | Is the exact student-facing release suitable for its intended classroom use? |
| Raw-to-Bronze validation | Did the instructor reference pipeline preserve and ingest the certified Raw data correctly? |

The Databricks implementation should reuse the conceptual rule model, severity vocabulary, deterministic result patterns, report structure, and certification philosophy of the PostgreSQL framework.

It should not copy PostgreSQL SQL directly where Spark SQL or PySpark provides a clearer and more scalable implementation.

The two frameworks should share stable rule identifiers whenever they measure the same business concept.

Examples:

```text
PLAYER_STATUS_DISTRIBUTION
PLAYER_GENDER_DISTRIBUTION
MATCH_TYPE_DISTRIBUTION
RATING_BAND_DISTRIBUTION
TEAM_PARTNER_CONTINUITY
```

Raw-only rules should use a distinct prefix where useful:

```text
RAW_REQUIRED_FILE_PRESENT
RAW_SCHEMA_CONTRACT
RAW_SOURCE_COUNT_RECONCILIATION
RAW_ASSIGNMENT_CANDIDATE_DEPTH
```

---

## 4. Objectives

The Raw certification process shall:

- certify the exact files delivered to students;
- support `5k`, `50k`, and `250k` through configuration only;
- execute without changes to source code between release sizes;
- validate both technical integrity and educational fitness;
- identify release-blocking conditions before students receive the data;
- produce durable, reproducible certification evidence;
- preserve the Raw data without mutation;
- support comparison with PostgreSQL source certification results;
- support cross-scale and prior-release regression analysis;
- provide enough detail to diagnose whether a problem originated in generation, export, packaging, or Raw landing.

---

## 5. Non-Objectives

The Raw certification process shall not:

- transform Raw data into Bronze business tables;
- correct, filter, deduplicate, or quarantine student records;
- create Silver or Gold analytical entities;
- validate instructor Bronze, Silver, or Gold logic;
- expose hidden simulation parameters to students;
- write certification findings into student-facing source files;
- infer missing business data to make a release pass;
- certify student-created pipelines;
- replace the PostgreSQL generator certification process.

---

## 6. Certification Scope

### 6.1 Expected Raw data domains

The certification framework must support the current student-facing source domains:

| Domain | Expected source |
| --- | --- |
| Geography | `regions.parquet` |
| Clubs | `clubs.parquet` |
| Club memberships | `club_memberships.parquet` |
| Players | `player_master.parquet` |
| Player registrations | `player_registrations.parquet` |
| Player assessment history | `player_assessment_history.parquet` |
| Teams | `teams.parquet` |
| Team memberships | `team_memberships.parquet` |
| Matches | `matches.parquet` |
| Match sides | `match_teams.parquet` |
| Match participants | `match_team_players.parquet` |
| Games | `match_games.parquet` |
| Monthly batches | `monthly_batches.parquet` |

The expected file set must be configuration-driven. A future schema version may add, remove, or rename files without changing certification orchestration code.

### 6.2 Canonical source mode

The official certification run shall read the exact Parquet files in the configured Raw release path.

Recommended canonical path pattern:

```text
<raw_root>/<release_name>/
```

Example:

```text
/Volumes/workspace/napa_raw/napa_5k/
/Volumes/workspace/napa_raw/napa_50k/
/Volumes/workspace/napa_raw/napa_250k/
```

Alternative DBFS or workspace paths may be used through configuration.

Temporary views may be registered:

```text
raw_cert_regions
raw_cert_player_master
raw_cert_teams
raw_cert_matches
```

These views must be scoped to the certification run and must not replace the student files.

### 6.3 Optional registered-table mode

An optional `source_mode: raw_tables` may be supported when the Raw layer is represented by external or managed tables.

When this mode is used, the framework must still verify that the configured tables point to the intended release and must capture table location, format, and version metadata.

The official release process should prefer direct Parquet certification unless there is a deliberate architectural decision to define the student Raw layer as registered tables.

---

## 7. Parameterization Contract

### 7.1 Required release parameter

The workflow must accept one required release parameter:

```text
release_name
```

Accepted operator inputs:

```text
5k
50k
250k
napa_5k
napa_50k
napa_250k
```

All aliases must normalize internally to:

```text
napa_5k
napa_50k
napa_250k
```

Unknown values must fail before any data query is executed.

### 7.2 Release profiles

Each normalized release maps to an intended-use profile.

| Release | Intended use | Certification emphasis |
| --- | --- | --- |
| `napa_5k` | Development and Milestone 1 foundation | file completeness, schema, key integrity, basic population viability, sufficient data for exploration |
| `napa_50k` | Engineering validation and Milestone 2 | full integrity, scalable relationships, meaningful quality defects, early analytical viability |
| `napa_250k` | Production-scale final prototype and Milestone 3 | full assignment readiness, Olympic candidate depth, ranking viability, development analysis, tournament readiness |

A rule may apply to:

- all releases;
- only `50k` and `250k`; or
- only `250k`.

The rule registry must declare applicability explicitly.

### 7.3 Additional workflow parameters

Recommended parameters:

| Parameter | Required | Purpose |
| --- | --- | --- |
| `release_name` | yes | Selects `5k`, `50k`, or `250k`. |
| `config_path` | no | Overrides the default certification configuration path. |
| `raw_root` | no | Overrides the configured Raw landing root. |
| `source_mode` | no | `parquet` or `raw_tables`; default `parquet`. |
| `analysis_as_of_date` | no | Freezes current-status and recency calculations. |
| `certification_run_id` | no | Allows orchestration to supply a durable run identifier. |
| `baseline_id` | no | Selects an approved comparison baseline. |
| `source_snapshot_path` | no | PostgreSQL certification/export snapshot for reconciliation. |
| `query` | no | Repeatable named-query filter for targeted debugging. |
| `category` | no | Repeatable category filter. |
| `fail_on` | no | Minimum severity that fails the workflow; default `blocker`. |
| `publish_report` | no | Enables or disables report publication; default `true`. |
| `persist_results` | no | Enables or disables Delta result persistence; default `true`. |

### 7.4 Example invocation

```bash
python -m napa_pipeline.certification.raw \
  --release-name napa_50k \
  --config-path config/certification/raw_certification.yml \
  --analysis-as-of-date 2026-06-30
```

Databricks job parameter example:

```text
--release-name
{{job.parameters.release_name}}

--analysis-as-of-date
{{job.parameters.analysis_as_of_date}}
```

---

## 8. Configuration Design

### 8.1 Recommended files

```text
config/
  certification/
    raw_certification.yml
    raw_certification_thresholds.yml
    raw_certification_schema.yml
    raw_certification_profiles.yml
```

A single consolidated YAML file is acceptable if that matches the existing repository pattern.

### 8.2 Example configuration

```yaml
certification:
  version: 1
  audit_catalog: workspace
  audit_schema: napa_certification
  source_mode: parquet
  raw_root: /Volumes/workspace/napa_raw
  report_root: /Volumes/workspace/napa_certification/reports
  snapshot_root: /Volumes/workspace/napa_certification/snapshots
  fail_on: blocker

releases:
  napa_5k:
    aliases: [5k]
    dataset_size_label: 5K
    intended_use: development
    raw_path: ${certification.raw_root}/napa_5k
    expected_player_scale:
      minimum: 4500
      maximum: 5500

  napa_50k:
    aliases: [50k]
    dataset_size_label: 50K
    intended_use: engineering_validation
    raw_path: ${certification.raw_root}/napa_50k
    expected_player_scale:
      minimum: 45000
      maximum: 55000

  napa_250k:
    aliases: [250k]
    dataset_size_label: 250K
    intended_use: production
    raw_path: ${certification.raw_root}/napa_250k
    expected_player_scale:
      minimum: 225000
      maximum: 275000

profiles:
  common:
    max_orphan_rate: 0.0001
    max_duplicate_key_rate: 0.0
    minimum_active_player_rate_blocker: 0.60
    minimum_active_player_rate_warning: 0.80
    maximum_unknown_status_rate: 0.001
    maximum_missing_required_field_rate: 0.001

  napa_5k:
    minimum_viable_teams_per_country_division: 10
    minimum_recent_matches_per_candidate_team: 3
    minimum_assessment_periods_for_development_probe: 2

  napa_50k:
    minimum_viable_teams_per_country_division: 75
    minimum_recent_matches_per_candidate_team: 8
    minimum_assessment_periods_for_development_probe: 3

  napa_250k:
    minimum_viable_teams_per_country_division: 300
    minimum_recent_matches_per_candidate_team: 12
    minimum_assessment_periods_for_development_probe: 4
```

All numeric values in the example are provisional calibration defaults. Codex must place them in configuration rather than embedding them in query code.

### 8.3 Threshold sources

Threshold precedence should be:

1. explicit runtime override;
2. approved scale-specific certification profile;
3. approved source release manifest or frozen PostgreSQL snapshot;
4. common certification default;
5. no assessment, with an informational finding explaining that the metric was observed but not thresholded.

The process must never silently substitute a live mutable generator default for an approved release-specific target.

---

## 9. Release Context and Manifest

### 9.1 Recommended release manifest

Each Raw release should include or be accompanied by a machine-readable manifest.

Recommended fields:

```json
{
  "release_name": "napa_250k",
  "release_version": "2026.07.1",
  "schema_version": "3.0",
  "generated_at": "2026-07-01T14:00:00Z",
  "exported_at": "2026-07-01T16:00:00Z",
  "analysis_as_of_date": "2026-06-30",
  "generation_run_id": 64,
  "expected_files": [],
  "file_row_counts": {},
  "file_checksums": {},
  "source_certification_snapshot": "..."
}
```

### 9.2 Manifest behavior

- A missing manifest should be a warning for development runs while the framework is being introduced.
- It should become an error or blocker for production `250k` certification after rollout.
- Manifest counts and checksums must be compared to the actual landed files when provided.
- The certification report must identify whether the release was certified with or without source reconciliation evidence.

---

## 10. Certification Pillars

The Databricks Raw framework shall use the following certification pillars.

### 10.1 Pillar A — Release Inventory and Format

Purpose:

> Confirm that the expected student release is present, readable, correctly identified, and complete.

Checks include:

- required files present;
- no unexpected duplicate source domain;
- Parquet files readable;
- nonzero record count where expected;
- file path and release identity;
- manifest consistency;
- checksum consistency when available;
- no mixed release versions in one landing path;
- no hidden temporary or partial-export files treated as sources.

### 10.2 Pillar B — Schema and Structural Integrity

Purpose:

> Confirm that the Raw release preserves the expected physical contract and relationship structure.

Checks include:

- required columns present;
- compatible data types;
- primary-key uniqueness;
- required-field null rates;
- foreign-key resolution;
- match-side cardinality;
- team membership cardinality;
- match winner participation;
- game sequence and score integrity;
- batch references;
- valid persistent team identity.

### 10.3 Pillar C — Population and Lifecycle Fitness

Purpose:

> Confirm that the player population is usable for current, historical, and development analysis.

Checks include:

- player scale;
- active/inactive/injured/retired distribution;
- active-player percentage;
- country, gender, region, and age representation;
- registration timing;
- lifecycle status consistency;
- recent activity coverage;
- inactive-history dominance;
- sufficient active players by analytical cohort.

The condition in which approximately 90% of players are inactive must produce a release-blocking finding.

### 10.4 Pillar D — Team and Partnership Fitness

Purpose:

> Confirm that the dataset contains valid and analytically useful doubles partnerships.

Checks include:

- exactly one persistent team identity per unordered player pair;
- fixed `team_type`;
- no pair represented as both ad hoc and competitive;
- exactly two members per valid team period;
- country and division compatibility;
- valid active team populations;
- team lifecycle coherence;
- partnership age and continuity;
- sufficient partnership diversity;
- sufficient repeated-match evidence;
- viable candidate depth by country and division.

### 10.5 Pillar E — Competition and Evidence Fitness

Purpose:

> Confirm that players and teams have enough credible match and game evidence for analysis.

Checks include:

- match volume by month;
- matches per active player;
- matches per team;
- zero-match active players;
- repeat-opponent concentration;
- match-type distribution;
- day-of-week and weekend distribution;
- region concentration;
- exactly two match sides;
- participant resolution;
- game count and best-of-series coherence;
- score validity;
- margin diversity;
- upset and favorite-win behavior;
- sufficient recent evidence for selection candidates.

### 10.6 Pillar F — Ratings, Confidence, and Development Fitness

Purpose:

> Confirm that ratings, uncertainty, and longitudinal signals support defensible analysis.

Checks include:

- rating range and distribution;
- elite tail size;
- rating spread and compression;
- confidence range;
- confidence versus match count;
- volatility range;
- rating recency;
- assessment-history coverage;
- number of observation periods;
- improving-player population;
- emerging-player population;
- evidence that future-potential analysis is possible.

### 10.7 Pillar G — Assignment Pathway Readiness

Purpose:

> Execute simplified analytical probes that confirm students can complete the intended milestone work.

Required probes:

1. **National ranking probe**
2. **Olympic player-selection probe**
3. **Olympic team-selection probe**
4. **Partnership analysis probe**
5. **Development analysis viability probe**
6. **Tournament candidate probe**
7. **Data-quality learning probe**

The development-analysis probe must confirm that enough longitudinal history exists to support student-defined criteria, not define the correct development cohort for students.

The probes do not calculate the instructor’s final answer or expose hidden factors. They only verify that the required analytical pathway has a sufficiently large and differentiated population.

### 10.8 Pillar H — Source Reconciliation and Regression

Purpose:

> Detect export, packaging, Raw landing, and cross-release defects.

Checks include:

- source-to-Raw row-count reconciliation;
- source-to-Raw distribution reconciliation;
- expected file count;
- scale ratio comparisons;
- prior approved release drift;
- 5K-to-50K-to-250K structural consistency;
- unexpected loss of countries, divisions, statuses, or analytical cohorts;
- schema drift;
- release-to-release candidate-pool collapse.

---

## 11. Rule Model

Each certification rule shall be represented as metadata rather than embedded orchestration logic.

Recommended model:

```python
@dataclass(frozen=True)
class RawCertificationRule:
    rule_id: str
    name: str
    description: str
    pillar: str
    category: str
    applicable_releases: tuple[str, ...]
    required_sources: tuple[str, ...]
    execution_mode: str
    severity_on_failure: str
    threshold_keys: tuple[str, ...]
    student_use_cases: tuple[str, ...]
    remediation_owner: str
    query: str | Callable
    post_process: Callable | None = None
```

Recommended execution modes:

```text
spark_sql
pyspark
manifest
reconciliation
derived_probe
```

Each result should contain:

```text
certification_run_id
release_name
rule_id
pillar
category
status
severity
observed_value
expected_min
expected_max
numerator
denominator
affected_count
sample_records
message
business_impact
recommended_action
execution_started_at
execution_completed_at
```

---

## 12. Minimum Rule Catalog

The following rule catalog is the minimum implementation target. Codex may refine names after repository inspection, but it must preserve the business intent and stable identifiers.

### 12.1 Inventory and schema rules

| Rule ID | Applicability | Default severity | Purpose |
| --- | --- | --- | --- |
| `RAW_RELEASE_NAME_VALID` | all | blocker | Release parameter normalizes to an approved profile. |
| `RAW_PATH_EXISTS` | all | blocker | Configured release path exists. |
| `RAW_REQUIRED_FILES_PRESENT` | all | blocker | Every required source domain is present. |
| `RAW_UNEXPECTED_DUPLICATE_DOMAIN` | all | error | A source domain is not represented by multiple conflicting paths. |
| `RAW_PARQUET_READABLE` | all | blocker | Every required file can be read by Spark. |
| `RAW_MANIFEST_IDENTITY_MATCH` | all | error | Manifest release identity matches runtime selection. |
| `RAW_MANIFEST_ROW_COUNT_MATCH` | all | error | Manifest counts reconcile to actual counts. |
| `RAW_SCHEMA_REQUIRED_COLUMNS` | all | blocker | Required columns are present. |
| `RAW_SCHEMA_TYPE_COMPATIBILITY` | all | error | Types are compatible with the schema contract. |
| `RAW_SCHEMA_VERSION_SUPPORTED` | all | blocker | The release schema version is supported. |
| `RAW_NONEMPTY_EXPECTED_TABLES` | all | blocker | Required domains contain records. |
| `RAW_PLAYER_SCALE_RANGE` | all | blocker | Player count is appropriate for the selected release. |

### 12.2 Key and relationship rules

| Rule ID | Applicability | Default severity | Purpose |
| --- | --- | --- | --- |
| `RAW_PLAYER_KEY_UNIQUE` | all | blocker | `player_id` is unique and non-null. |
| `RAW_TEAM_KEY_UNIQUE` | all | blocker | `team_id` is unique and non-null. |
| `RAW_MATCH_KEY_UNIQUE` | all | blocker | `match_id` is unique and non-null. |
| `RAW_GAME_KEY_UNIQUE` | all | error | Game keys are unique. |
| `RAW_PLAYER_REGION_FK` | all | error | Player home region resolves. |
| `RAW_CLUB_REGION_FK` | all | error | Club region resolves. |
| `RAW_CLUB_MEMBERSHIP_FKS` | all | error | Club membership resolves to player and club. |
| `RAW_TEAM_MEMBERSHIP_FKS` | all | blocker | Team membership resolves to player and team. |
| `RAW_MATCH_REGION_FK` | all | error | Match region resolves. |
| `RAW_MATCH_BATCH_FK` | all | error | Match batch resolves. |
| `RAW_MATCH_TEAM_CARDINALITY` | all | blocker | Every match has exactly two sides. |
| `RAW_MATCH_TEAM_PLAYER_CARDINALITY` | all | blocker | Every match side has exactly two players. |
| `RAW_WINNER_PARTICIPATED` | all | blocker | Winning team participated in the match. |
| `RAW_MATCH_GAME_FK` | all | error | Every game resolves to a match. |
| `RAW_BATCH_SEQUENCE_VALID` | all | error | Batch sequence and dates are coherent. |

### 12.3 Player and lifecycle rules

| Rule ID | Applicability | Default severity | Purpose |
| --- | --- | --- | --- |
| `RAW_PLAYER_STATUS_DISTRIBUTION` | all | error | Status mix is plausible and near approved targets. |
| `RAW_ACTIVE_PLAYER_RATE` | all | blocker | Active population is sufficient for current analysis. |
| `RAW_PLAYER_GENDER_DISTRIBUTION` | all | error | Gender distribution supports all divisions. |
| `RAW_PLAYER_COUNTRY_DISTRIBUTION` | all | error | USA and Canada populations are present at useful scale. |
| `RAW_PLAYER_AGE_DISTRIBUTION` | all | warning | Age distribution is plausible and supports development analysis. |
| `RAW_REGISTRATION_DATE_VALID` | all | error | Registration dates are logically valid. |
| `RAW_ACTIVITY_AFTER_REGISTRATION` | all | error | Competitive activity does not predate registration. |
| `RAW_ACTIVE_PLAYER_RECENT_ACTIVITY` | 50k, 250k | error | Active players have meaningful recent evidence. |
| `RAW_INACTIVE_HISTORY_DOMINANCE` | all | blocker | Historical inactive records do not overwhelm usable current cohorts. |
| `RAW_ACTIVE_COHORT_BY_COUNTRY_GENDER` | all | blocker | Each country/gender cohort is sufficiently populated. |
| `RAW_PLAYER_NAME_DUPLICATE_CONCENTRATION` | all | warning | Synthetic identity duplication remains within tolerance. |

### 12.4 Team and partnership rules

| Rule ID | Applicability | Default severity | Purpose |
| --- | --- | --- | --- |
| `RAW_TEAM_PAIR_UNIQUENESS` | all | blocker | One unordered player pair maps to one team identity. |
| `RAW_TEAM_TYPE_FIXED` | all | blocker | Team type does not vary for the same team. |
| `RAW_TEAM_PAIR_TYPE_EXCLUSIVITY` | all | blocker | One pair is not both ad hoc and competitive. |
| `RAW_TEAM_EXACTLY_TWO_MEMBERS` | all | blocker | Valid teams contain two members. |
| `RAW_TEAM_MEMBER_DISTINCT` | all | blocker | A team does not contain the same player twice. |
| `RAW_TEAM_COUNTRY_ALIGNMENT` | all | error | Team country aligns with member eligibility rules. |
| `RAW_TEAM_DIVISION_ALIGNMENT` | all | error | Team division aligns with member gender composition. |
| `RAW_TEAM_LIFECYCLE_VALID` | all | error | Formation and dissolution dates are coherent. |
| `RAW_ACTIVE_TEAM_MEMBER_STATUS` | 50k, 250k | error | Candidate teams have usable member status. |
| `RAW_TEAM_PARTNERSHIP_AGE_DISTRIBUTION` | 50k, 250k | warning | New, established, and mature teams are represented. |
| `RAW_TEAM_MATCH_EVIDENCE_DISTRIBUTION` | 50k, 250k | error | Teams have varied but sufficient history. |
| `RAW_VIABLE_TEAM_DEPTH_BY_COUNTRY_DIVISION` | all | blocker | Candidate depth is sufficient for the release profile. |

### 12.5 Match and game rules

| Rule ID | Applicability | Default severity | Purpose |
| --- | --- | --- | --- |
| `RAW_MATCH_VOLUME_BY_BATCH` | all | error | Monthly match volume is stable and plausible. |
| `RAW_MATCH_TYPE_DISTRIBUTION` | all | warning | Match-type mix remains near approved targets. |
| `RAW_MATCH_DAY_DISTRIBUTION` | all | warning | Scheduling distribution is plausible. |
| `RAW_WEEKEND_MATCH_SHARE` | all | warning | Weekend concentration remains within bounds. |
| `RAW_MATCHES_PER_ACTIVE_PLAYER` | all | error | Active-player match exposure is sufficient. |
| `RAW_MATCHES_PER_TEAM` | all | error | Team match exposure is sufficient. |
| `RAW_ZERO_MATCH_ACTIVE_PLAYERS` | all | error | Active players without evidence remain within tolerance. |
| `RAW_REPEAT_OPPONENT_CONCENTRATION` | 50k, 250k | warning | Performance is not dominated by one opponent. |
| `RAW_REGION_MATCH_CONCENTRATION` | all | warning | Match activity is not implausibly concentrated. |
| `RAW_GAME_SCORE_NONNEGATIVE` | all | blocker | Game scores are nonnegative. |
| `RAW_GAME_WINNER_SCORE_ALIGNMENT` | all | blocker | Game winner matches recorded score. |
| `RAW_GAME_SEQUENCE_VALID` | all | error | Game numbers are complete and ordered. |
| `RAW_MATCH_WIN_BY_TWO_COHERENCE` | all | error | Win-by rules are reflected in final scores. |
| `RAW_GAME_MARGIN_DISTRIBUTION` | 50k, 250k | warning | Score margins provide useful competitive variation. |
| `RAW_UPSET_RATE_PLAUSIBILITY` | 50k, 250k | warning | Outcomes are neither deterministic nor random. |

### 12.6 Rating, confidence, and development rules

| Rule ID | Applicability | Default severity | Purpose |
| --- | --- | --- | --- |
| `RAW_RATING_RANGE_VALID` | all | blocker | Ratings fall in supported bounds. |
| `RAW_RATING_DISTRIBUTION` | all | error | Rating distribution is plausible. |
| `RAW_ELITE_PLAYER_DEPTH` | all | blocker | Elite candidate population is sufficiently large. |
| `RAW_ELITE_RATING_SEPARATION` | 50k, 250k | warning | The elite tail is not unhelpfully compressed. |
| `RAW_CONFIDENCE_RANGE_VALID` | all | error | Confidence values fall in supported bounds. |
| `RAW_CONFIDENCE_MATCH_COUNT_RELATIONSHIP` | 50k, 250k | warning | More evidence generally produces more confidence. |
| `RAW_VOLATILITY_RANGE_VALID` | all | error | Volatility values fall in supported bounds. |
| `RAW_RATING_DATE_RECENCY` | 50k, 250k | error | Current candidates have current ratings. |
| `RAW_ASSESSMENT_HISTORY_COVERAGE` | 50k, 250k | error | Longitudinal assessment evidence is sufficient. |
| `RAW_DEVELOPMENT_OBSERVATION_PERIODS` | 50k, 250k | error | Development candidates have multiple periods. |
| `RAW_IMPROVING_PLAYER_DEPTH` | 250k | blocker | A meaningful emerging-talent cohort exists. |

### 12.7 Assignment pathway rules

| Rule ID | Applicability | Default severity | Purpose |
| --- | --- | --- | --- |
| `RAW_RANKING_PROBE_VIABLE` | 50k, 250k | blocker | National rankings can be produced with adequate evidence. |
| `RAW_PLAYER_SELECTION_PROBE_VIABLE` | 250k | blocker | Olympic-caliber players can be differentiated. |
| `RAW_TEAM_SELECTION_PROBE_VIABLE` | 250k | blocker | Olympic team selection has adequate candidate depth. |
| `RAW_PARTNERSHIP_ANALYSIS_PROBE_VIABLE` | 50k, 250k | blocker | Partnership strength and continuity can be analyzed. |
| `RAW_DEVELOPMENT_PIPELINE_PROBE_VIABLE` | 250k | blocker | Future-potential analysis is feasible. |
| `RAW_TOURNAMENT_CANDIDATE_PROBE_VIABLE` | 250k | blocker | Valid team IDs exist for every required country/division. |
| `RAW_DATA_QUALITY_LEARNING_PROBE` | all | warning | The release contains realistic but manageable quality issues. |

### 12.8 Reconciliation and regression rules

| Rule ID | Applicability | Default severity | Purpose |
| --- | --- | --- | --- |
| `RAW_SOURCE_FILE_COUNT_RECONCILIATION` | all | error | Raw file counts match export/source evidence. |
| `RAW_SOURCE_PLAYER_STATUS_RECONCILIATION` | all | blocker | Status mix was preserved through export. |
| `RAW_SOURCE_TEAM_COUNT_RECONCILIATION` | all | error | Team population was preserved. |
| `RAW_SOURCE_MATCH_COUNT_RECONCILIATION` | all | error | Match population was preserved. |
| `RAW_SOURCE_RATING_RECONCILIATION` | all | error | Rating distribution was preserved. |
| `RAW_SCHEMA_CROSS_SCALE_CONSISTENCY` | all | blocker | 5K, 50K, and 250K use the same supported schema. |
| `RAW_DISTRIBUTION_PRIOR_RELEASE_DRIFT` | all | warning | Material unexplained release drift is identified. |
| `RAW_CANDIDATE_POOL_PRIOR_RELEASE_REGRESSION` | 50k, 250k | error | Candidate populations do not collapse unexpectedly. |

---

## 13. Assignment Pathway Probe Definitions

### 13.1 Ranking probe

The ranking probe shall calculate the number of players by country who satisfy configurable minimum evidence requirements:

```text
valid status
valid country
valid rating
minimum confidence
minimum match count
rating recency
```

It shall not rank the players or publish an instructor answer.

Pass condition:

- adequate candidate count for both countries;
- meaningful rating or performance differentiation;
- no country dominated by missing or stale evidence.

### 13.2 Olympic player-selection probe

The player-selection probe shall confirm that each country and required gender cohort contains:

- multiple elite candidates;
- credible alternates;
- sufficient recent evidence;
- confidence values adequate for comparative analysis.

### 13.3 Olympic team-selection probe

For each country and division:

```text
USA men's doubles
USA women's doubles
USA mixed doubles
Canada men's doubles
Canada women's doubles
Canada mixed doubles
```

the probe shall count teams satisfying configurable eligibility and evidence rules.

At minimum:

- team exists;
- team has exactly two distinct members;
- country and division are valid;
- member statuses are usable;
- team has minimum match history;
- team has recent activity;
- team and player ratings are available;
- team ID is usable for tournament submission.

### 13.4 Partnership analysis probe

The partnership probe shall verify the existence of:

- established partnerships;
- newer partnerships;
- players with more than one partnership;
- repeated match history;
- meaningful differences in partnership success;
- enough evidence to distinguish player strength from team effectiveness.

### 13.5 Development analysis viability probe

The development-analysis probe shall verify that the data contains enough longitudinal signal to support student-defined development analysis, including:

- appropriate age or experience indicators;
- more than one observation period;
- rating or assessment improvement;
- uncertainty information;
- enough evidence to avoid forcing development conclusions from noise alone.

### 13.6 Tournament candidate probe

The tournament probe shall confirm that valid pre-existing team IDs are available for all six country/division combinations.

It shall test only availability and validity. It shall not identify the instructor-preferred teams.

### 13.7 Data-quality learning probe

The dataset is intended to contain realistic data quality issues. This probe shall verify that:

- the release is not perfectly sterile unless intentionally configured;
- defects remain within manageable limits;
- defects do not destroy core assignment pathways;
- the number and distribution of defects are appropriate for the release profile.

---

## 14. Assessment and Certification Model

### 14.1 Rule statuses

```text
pass
warning
fail
not_applicable
not_assessed
execution_error
```

### 14.2 Severities

```text
info
warning
error
blocker
```

### 14.3 Certification decisions

| Decision | Meaning |
| --- | --- |
| `CERTIFIED` | No blocker or error findings; release is approved for intended use. |
| `CERTIFIED_WITH_WARNINGS` | No blocker findings; warnings are accepted and documented. |
| `REJECTED` | One or more blocker findings, or an approved error-count policy is exceeded. |
| `EXECUTION_FAILED` | The framework could not complete certification reliably. |

The report must also state intended use:

```text
CERTIFIED_FOR_DEVELOPMENT
CERTIFIED_FOR_ENGINEERING_VALIDATION
CERTIFIED_FOR_PRODUCTION_ANALYTICS
```

### 14.4 Hard-gate behavior

The following conditions must reject the release regardless of aggregate score:

- required source file missing;
- unsupported or materially incomplete schema;
- unreadable Parquet;
- player count inconsistent with selected release;
- duplicate critical primary keys above tolerance;
- material broken team or match relationships;
- approximately 90% inactive players or active rate below configured blocker threshold;
- insufficient country/gender populations;
- insufficient viable teams in any required country/division;
- invalid tournament team IDs or missing candidate pools in `250k`;
- source-to-Raw population collapse;
- certification execution error that prevents a reliable conclusion.

### 14.5 Scoring

A score may supplement, but never override, hard gates.

Recommended pillar weights:

| Pillar | Weight |
| --- | ---: |
| Inventory and format | 10 |
| Schema and structural integrity | 20 |
| Population and lifecycle fitness | 15 |
| Team and partnership fitness | 15 |
| Competition and evidence fitness | 15 |
| Ratings, confidence, and development | 10 |
| Assignment pathway readiness | 10 |
| Reconciliation and regression | 5 |

The report should clearly distinguish:

- **decision:** gate-based;
- **score:** diagnostic;
- **warnings:** review items.

---

## 15. Persistent Audit Data Model

Create a dedicated instructor-only certification schema.

Recommended objects:

```text
<audit_catalog>.<audit_schema>.raw_certification_runs
<audit_catalog>.<audit_schema>.raw_certification_rule_runs
<audit_catalog>.<audit_schema>.raw_certification_metrics
<audit_catalog>.<audit_schema>.raw_certification_findings
<audit_catalog>.<audit_schema>.raw_certification_artifacts
<audit_catalog>.<audit_schema>.raw_certification_baselines
```

### 15.1 `raw_certification_runs`

Recommended columns:

```text
certification_run_id
release_name
release_version
schema_version
intended_use
source_mode
raw_path
analysis_as_of_date
started_at
completed_at
status
certification_decision
overall_score
config_snapshot_json
source_snapshot_path
baseline_id
code_version
git_commit
error_message
```

### 15.2 `raw_certification_rule_runs`

```text
certification_run_id
rule_id
pillar
category
status
severity
started_at
completed_at
attempt_number
row_count_scanned
result_json
error_message
```

### 15.3 `raw_certification_metrics`

```text
certification_run_id
rule_id
metric_name
dimension_json
metric_value
metric_text
expected_min
expected_max
unit
```

### 15.4 `raw_certification_findings`

```text
certification_run_id
finding_id
rule_id
severity
title
message
business_impact
recommended_action
affected_count
sample_records_json
accepted_exception
exception_reason
```

### 15.5 `raw_certification_artifacts`

```text
certification_run_id
artifact_type
artifact_path
created_at
checksum
```

### 15.6 Retention

Certification records must be retained across reruns to support:

- release history;
- root-cause analysis;
- prior-release comparison;
- grading evidence;
- reproducibility.

A rerun must create a new `certification_run_id`; it must not overwrite historical results.

---

## 16. Snapshot and Report Outputs

Each completed run shall publish:

1. JSON snapshot;
2. Markdown certification report;
3. CSV finding extract;
4. optional HTML rendering;
5. durable Delta records.

Recommended path:

```text
<snapshot_root>/<release_name>/<certification_run_id>/certification.json
<report_root>/<release_name>/<certification_run_id>/certification_report.md
<report_root>/<release_name>/<certification_run_id>/findings.csv
```

### 16.1 JSON structure

```json
{
  "certification_run_id": "...",
  "release_name": "napa_250k",
  "intended_use": "production",
  "analysis_as_of_date": "2026-06-30",
  "source": {
    "mode": "parquet",
    "path": "...",
    "manifest": "..."
  },
  "decision": "REJECTED",
  "score": 71.4,
  "pillar_scores": {},
  "severity_counts": {},
  "results": [],
  "findings": [],
  "artifacts": {},
  "code_version": "..."
}
```

### 16.2 Markdown report structure

```text
1. Certification Decision
2. Release Identity
3. Executive Summary
4. Pillar Scorecard
5. Release-Blocking Findings
6. Assignment Pathway Readiness
7. Population and Candidate Depth
8. Structural and Relationship Findings
9. Source Reconciliation
10. Cross-Scale and Historical Regression
11. Warnings and Accepted Exceptions
12. Recommended Remediation
13. Execution and Reproducibility Metadata
14. Detailed Rule Results
```

### 16.3 Example release decision

```text
NAPA RAW STUDENT DATA CERTIFICATION

Release: napa_250k
Decision: REJECTED
Intended use: Production analytics and Milestone 3

Release-blocking findings:
- Active-player rate is 9.8%; minimum is 60%.
- Canada mixed-doubles viable team count is below threshold.
- Source status distribution does not reconcile to Raw.

Do not publish this release to students.
```

---

## 17. Workflow Design

Recommended Databricks workflow:

```text
00_resolve_release_context
01_inventory_and_manifest
02_schema_and_structural_integrity
03_population_and_lifecycle
04_team_and_partnership_fitness
05_competition_and_game_fitness
06_ratings_and_development
07_assignment_pathway_probes
08_source_reconciliation
09_regression_comparison
10_assess_and_publish
11_enforce_release_gate
```

### 17.1 Workflow behavior

- The workflow must accept `release_name` as a job parameter.
- Each task must receive the normalized release context.
- Rule-level results must be durable.
- A task rerun should skip already successful rules when safe.
- A failed rule query should not silently disappear.
- Technical execution failure must be distinct from a failed certification rule.
- Publication must occur even for rejected releases when sufficient results exist.
- The release-gate task must fail the Databricks job when the decision is `REJECTED` or `EXECUTION_FAILED`.

### 17.2 Task isolation

A single Python package with one entry point is preferred for logic reuse.

Databricks tasks may call category-specific entry points:

```bash
python -m napa_pipeline.certification.raw inventory
python -m napa_pipeline.certification.raw structural
python -m napa_pipeline.certification.raw population
python -m napa_pipeline.certification.raw assignment
python -m napa_pipeline.certification.raw publish
```

If the repository is notebook-centric, thin notebooks may call shared package functions. Business logic should not be duplicated across notebooks.

---

## 18. Recommended Repository Structure

Codex must first inspect the existing repository and adapt to its conventions. A reasonable target structure is:

```text
config/
  certification/
    raw_certification.yml
    raw_certification_thresholds.yml
    raw_certification_schema.yml

src/
  napa_pipeline/
    certification/
      __init__.py
      models.py
      config.py
      release_context.py
      registry.py
      runner.py
      assessment.py
      scoring.py
      persistence.py
      reporting.py
      reconciliation.py
      regression.py
      raw/
        __init__.py
        source_loader.py
        inventory_rules.py
        structural_rules.py
        population_rules.py
        team_rules.py
        match_rules.py
        rating_rules.py
        assignment_probes.py

notebooks/
  certification/
    00_raw_certification_driver.py
    01_raw_certification_report.py

resources/
  workflows/
    napa_raw_certification.job.yml

tests/
  certification/
    test_release_context.py
    test_source_loader.py
    test_rule_registry.py
    test_structural_rules.py
    test_population_rules.py
    test_team_rules.py
    test_match_rules.py
    test_assignment_probes.py
    test_assessment.py
    test_reporting.py
    test_reconciliation.py
    test_regression.py
```

Do not create parallel duplicate frameworks if the repository already contains shared validation, configuration, reporting, or workflow utilities.

---

## 19. Performance Requirements

The implementation must be suitable for the `250k` release and associated millions of team, match, participant, and game records.

Requirements:

- use Spark SQL or DataFrame aggregations;
- avoid driver-side collection of large datasets;
- collect only summary metrics and limited sample records;
- cache only reused intermediate DataFrames;
- unpersist cached data after each pillar;
- avoid repeated full scans where a shared aggregate can serve multiple rules;
- use broadcast joins only for demonstrably small reference tables;
- record task and rule runtimes;
- preserve deterministic ordering of sample output;
- limit bad-record samples through configuration;
- do not create permanent copies of all Raw data for certification.

Recommended helper views may include:

```text
cert_player_match_summary
cert_team_roster
cert_team_match_summary
cert_country_division_candidates
cert_player_longitudinal_summary
```

These should be temporary or written only to an instructor audit schema when performance requires reuse.

---

## 20. Security and Separation

- Certification tables and reports must be instructor-only.
- Hidden source certification details must not be written into the student Raw release.
- Reports may discuss observed student-facing metrics but must not reveal hidden generator parameters or tournament bias values.
- The workflow must use read-only access to Raw data.
- Only the audit schema and report/snapshot locations may be written.
- Release certification and student analytics must use separate schemas or access controls.

---

## 21. Failure Diagnosis Matrix

| PostgreSQL certification | Raw certification | Likely cause |
| --- | --- | --- |
| fail | not run | generator or simulation problem |
| pass | fail inventory/schema | export, packaging, transfer, or Raw landing problem |
| pass | fail distribution reconciliation | export filtering or snapshot-selection problem |
| pass | pass | student source release is certified |
| pass | pass; Bronze validation fails | Raw-to-Bronze reference pipeline problem |

The certification report should include this diagnostic framing when source certification evidence is available.

---

## 22. Testing Strategy

### 22.1 Unit tests

Test:

- release-name normalization;
- invalid release rejection;
- scale-specific profile selection;
- schema contract evaluation;
- metric assessment;
- severity classification;
- hard-gate logic;
- report rendering;
- snapshot serialization;
- source reconciliation tolerance;
- prior-release regression calculations.

### 22.2 Spark integration tests

Use small synthetic Parquet fixtures representing:

1. valid release;
2. missing file;
3. missing required column;
4. duplicate player key;
5. orphaned team membership;
6. invalid match winner;
7. invalid game score;
8. insufficient candidate depth;
9. extreme status imbalance;
10. schema drift.

### 22.3 Required regression fixture

Create a known-bad fixture where approximately 90% of players are inactive.

Expected behavior:

```text
RAW_ACTIVE_PLAYER_RATE = fail/blocker
RAW_INACTIVE_HISTORY_DOMINANCE = fail/blocker
relevant assignment probes = fail
certification decision = REJECTED
release-gate task = failed
```

### 22.4 Scale validation

The same executable code must run against:

```text
napa_5k
napa_50k
napa_250k
```

Only configuration, data path, and profile thresholds may vary.

### 22.5 Golden report tests

Maintain expected JSON and Markdown outputs for a small deterministic fixture. Normalize timestamps and run IDs before comparison.

### 22.6 Performance test

Run the complete certification workflow against `napa_250k` and record:

- total runtime;
- rule runtime;
- Spark stages;
- shuffle volume where observable;
- largest result collections;
- memory or cache pressure.

No acceptance runtime should be hard-coded until measured on the available Databricks environment.

---

## 23. Acceptance Criteria

The implementation is complete when:

1. one workflow accepts `5k`, `50k`, or `250k`;
2. aliases normalize correctly;
3. the exact configured Raw files are certified without Bronze dependency;
4. all required source domains are inventoried;
5. the schema contract is validated;
6. critical keys and relationships are tested;
7. population and lifecycle fitness are assessed;
8. team and partnership viability are assessed;
9. match, game, rating, confidence, and development evidence are assessed;
10. assignment pathway probes run by release profile;
11. the known 90%-inactive fixture is rejected;
12. a good deterministic fixture is certified;
13. results persist to the instructor audit schema;
14. JSON, Markdown, and CSV artifacts are produced;
15. the final gate fails rejected releases;
16. prior certification results remain queryable;
17. code is configuration-driven and contains no release-specific branching outside profile selection;
18. automated tests pass;
19. the `250k` run completes without driver-side data collection failures;
20. documentation explains how to execute, interpret, and troubleshoot certification.

---

# Part II — Codex Implementation Plan

## 24. Codex Operating Instructions

Codex shall implement this specification incrementally.

Before changing code, Codex must:

1. inspect the repository;
2. identify existing configuration, workflow, validation, audit, logging, report, and persistence patterns;
3. identify how `5k`, `50k`, and `250k` are currently parameterized;
4. identify the canonical Raw paths and physical schemas;
5. identify existing Databricks Asset Bundle or workflow definitions;
6. identify reusable tests and fixtures;
7. provide a concise change-impact map.

Codex must not begin by creating a parallel architecture without first identifying reusable repository capabilities.

---

## 25. Phase 0 — Repository Assessment

### Tasks

- Locate current Raw ingestion and path configuration.
- Locate current release-name normalization.
- Locate source schema definitions and manifests.
- Locate validation result tables or reporting utilities.
- Locate Databricks job and bundle files.
- Locate test fixtures and local Spark test setup.
- Identify whether Raw is stored as Parquet, Delta, external tables, or a combination.
- Identify the exact student release file names and current schema version.
- Determine how code version and Git commit are captured.

### Deliverable

Create:

```text
docs/implementation/raw_certification_repository_assessment.md
```

The assessment shall include:

- reusable components;
- missing components;
- files requiring modification;
- proposed new files;
- migration risks;
- open questions;
- implementation sequence.

Do not change production code in Phase 0.

---

## 26. Phase 1 — Configuration and Release Context

### Tasks

1. Implement release-name normalization.
2. Implement scale-profile loading.
3. Implement Raw path resolution.
4. Implement `analysis_as_of_date`.
5. Implement config snapshot capture.
6. Implement schema-version validation.
7. Add CLI argument parsing.
8. Add job-parameter support.
9. Add clear errors for invalid release values.

### Tests

- all six accepted aliases;
- invalid values;
- missing release profile;
- missing path configuration;
- profile override precedence;
- deterministic config snapshot.

### Exit criteria

A dry-run command prints the fully resolved release context without reading data.

---

## 27. Phase 2 — Source Loader and Inventory Certification

### Tasks

1. Implement Parquet source loader.
2. Implement optional Raw-table source adapter if required.
3. Discover expected files from configuration.
4. Capture:
   - path;
   - format;
   - file count;
   - row count;
   - schema;
   - size if available;
   - modification metadata if available.
5. Implement manifest loading.
6. Implement inventory and readability rules.
7. Register temporary views.
8. Persist rule results.

### Tests

- missing path;
- missing file;
- duplicate domain;
- corrupt/unreadable Parquet;
- empty domain;
- manifest mismatch;
- successful inventory.

### Exit criteria

The workflow can certify release inventory for all three release profiles.

---

## 28. Phase 3 — Structural and Relationship Rules

### Tasks

Implement the complete inventory, schema, key, and relationship rule sets.

Priority order:

1. schema required columns;
2. primary keys;
3. foreign keys;
4. team composition;
5. match-side cardinality;
6. participant cardinality;
7. winner integrity;
8. game integrity;
9. batch integrity;
10. persistent team identity invariants.

Use shared aggregates where possible.

### Tests

Create deterministic failures for every blocker rule.

### Exit criteria

A structurally invalid release is rejected before assignment-readiness probes run.

---

## 29. Phase 4 — Population, Team, Match, and Rating Fitness

### Tasks

Port applicable business concepts from the PostgreSQL certification framework into Spark-based Raw rules.

Implement:

- player distribution and lifecycle checks;
- active-player fitness;
- team depth and partnership checks;
- competition volume and evidence checks;
- rating and confidence checks;
- temporal and development checks.

Where a PostgreSQL rule ID already exists, preserve it or document the cross-environment mapping.

### Tests

- status target drift;
- 90%-inactive release;
- insufficient country/gender cohort;
- insufficient viable team depth;
- zero-match active-player excess;
- rating compression;
- insufficient assessment history.

### Exit criteria

The Raw framework identifies analytically unusable but structurally valid releases.

---

## 30. Phase 5 — Assignment Pathway Probes

### Tasks

Implement all seven probes using configurable readiness definitions.

Requirements:

- no hidden-factor output;
- no final ranking or roster answer;
- no hard-coded athlete or team IDs;
- country/division results must be dimensional;
- candidate samples must be limited where a probe actually returns samples;
- scale-profile thresholds must control pass/fail.

### Tests

- viable 5K development release;
- viable 50K engineering release;
- viable 250K production release;
- missing Canada mixed candidate depth;
- insufficient development-analysis viability;
- invalid tournament team availability.

### Exit criteria

The report clearly states which assignment pathways are viable or blocked.

---

## 31. Phase 6 — Source Reconciliation and Regression

### Tasks

1. Define the source certification handoff format.
2. Load PostgreSQL/export snapshot metrics.
3. Map shared metric IDs.
4. Compare counts and percentages with configurable tolerance.
5. Compare schema across 5K, 50K, and 250K.
6. Compare against prior approved Raw certification.
7. Detect candidate-pool regression.
8. Distinguish:
   - source issue;
   - export issue;
   - Raw landing issue;
   - unexplained drift.

### Tests

- exact reconciliation;
- acceptable rounding drift;
- filtered inactive-status defect;
- dropped team rows;
- schema drift;
- missing source snapshot.

### Exit criteria

A PostgreSQL-pass/Raw-fail condition produces actionable export or landing diagnostics.

---

## 32. Phase 7 — Assessment, Persistence, and Reporting

### Tasks

1. Implement rule status and severity model.
2. Implement hard gates.
3. Implement pillar scores.
4. Implement final certification decision.
5. Persist run, rule, metric, finding, and artifact records.
6. Generate JSON snapshot.
7. Generate Markdown report.
8. Generate CSV finding extract.
9. Include Git commit and config snapshot.
10. Publish reports even when the release is rejected.

### Tests

- certified release;
- certified with warnings;
- rejected release;
- execution failure;
- hard gate overrides high score;
- historical run preservation;
- deterministic report content.

### Exit criteria

Every completed run has durable evidence and an unambiguous decision.

---

## 33. Phase 8 — Databricks Workflow and Release Gate

### Tasks

1. Add or update Databricks workflow definition.
2. Add job parameters.
3. Implement task dependencies.
4. Implement durable run context.
5. Implement rerun/resume behavior.
6. Implement final release-gate task.
7. Expose report paths in workflow output.
8. Document UI and CLI execution.

### Requirements

- one workflow definition;
- no separate 5K, 50K, and 250K code paths;
- job parameter controls release;
- rejected certification fails the job;
- report remains available after failure;
- code deploys through the repository's existing bundle or Git process.

### Exit criteria

An operator can select `5k`, `50k`, or `250k` and execute the same workflow end to end.

---

## 34. Phase 9 — Calibration and Rollout

### Tasks

1. Run against known-good candidate releases.
2. Run against known-bad historical releases.
3. Calibrate thresholds.
4. Mark each threshold as:
   - approved;
   - provisional;
   - observational.
5. Establish approved baselines.
6. Document accepted exceptions.
7. Define production release procedure.
8. Define rollback and recertification procedure.

### Required calibration cases

- the known 90%-inactive release;
- a release with correct active population;
- a release with broken team identities;
- a release with insufficient Canada mixed-team depth;
- a release with source-to-Raw row loss;
- a full valid `250k` release.

### Exit criteria

The instructor can rely on the framework as a formal student-data release gate.

---

## 35. Codex Completion Report

At completion, Codex must produce:

```text
docs/implementation/raw_certification_implementation_summary.md
```

It must contain:

- files added;
- files modified;
- architectural decisions;
- rule count by pillar;
- parameters;
- workflow name;
- audit tables;
- report locations;
- tests added;
- test results;
- performance observations;
- deviations from this specification;
- unresolved risks;
- commands to run `5k`, `50k`, and `250k`.

---

## 36. Required Execution Examples

### 5K

```bash
python -m napa_pipeline.certification.raw \
  --release-name 5k \
  --analysis-as-of-date 2026-06-30
```

Expected intended-use label:

```text
CERTIFIED_FOR_DEVELOPMENT
```

### 50K

```bash
python -m napa_pipeline.certification.raw \
  --release-name 50k \
  --analysis-as-of-date 2026-06-30
```

Expected intended-use label:

```text
CERTIFIED_FOR_ENGINEERING_VALIDATION
```

### 250K

```bash
python -m napa_pipeline.certification.raw \
  --release-name 250k \
  --analysis-as-of-date 2026-06-30
```

Expected intended-use label:

```text
CERTIFIED_FOR_PRODUCTION_ANALYTICS
```

---

## 37. Final Design Principle

The certification framework must answer:

> Can this exact Raw release support the engineering, governance, analytics, Olympic selection, partnership analysis, development analysis, and tournament-candidate work assigned to students at this release scale?

A technically readable release is not sufficient.

A statistically realistic release is not sufficient.

A release is certified only when it is both trustworthy and fit for its assigned student use.
