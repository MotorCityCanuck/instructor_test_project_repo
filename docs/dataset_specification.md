# Dataset Specification

**Purpose:** This document describes the NAPA dataset structure, release strategy, expected files, conceptual relationships, and usage expectations for the template repository. It is intended to orient student teams before implementation; teams must still validate actual file availability, field names, data types, keys, row counts, and relationships during their own ingestion and profiling work.

## Dataset Release Strategy

- `napa_5k` supports Milestone 1 foundational readiness and early ingestion validation.
- `napa_50k` supports Milestone 2 engineering readiness and broader data quality evaluation.
- `napa_250k` supports Milestone 3 final prototype execution and recommendation evidence.

## Dataset Storage Policy

- Do not commit source data files to GitHub.
- Store datasets locally or in approved Databricks locations.
- Document the actual storage pattern used by the team in the runbook.

## Expected Source Files

| Source file | Grain | Primary key | Analytical use |
|---|---|---|---|
| regions.parquet | One record per geographic region | id | Geographic context for athletes, clubs, and matches. |
| clubs.parquet | One record per club or facility | id | Club ecosystem, facility, and development context. |
| club_memberships.parquet | One record per player-club membership period | id | Athlete affiliation and development pathway analysis. |
| player_master.parquet | One current or snapshot record per athlete | player_id | Athlete profile, rating, status, confidence, and development context. |
| player_registrations.parquet | One record per player registration event | id | Participation growth, onboarding, and athlete lifecycle analysis. |
| player_assessment_history.parquet | One record per athlete assessment or development signal | id | Longitudinal development and future potential analysis. |
| teams.parquet | One record per doubles partnership/team | id | Partnership, country/category, lifecycle, and tournament candidate analysis. |
| team_memberships.parquet | One record per player-team membership period | id | Valid team composition and partnership continuity analysis. |
| matches.parquet | One record per match | id | Match-level results, competition context, and temporal performance analysis. |
| match_teams.parquet | One record per side/team in a match | id | Team-level match performance and matchup context. |
| match_team_players.parquet | One record per player participating on a match team | id | Player participation and rating-at-match evidence. |
| match_games.parquet | One record per game within a match | id | Game-level scoring, consistency, dominance, and close-game analysis. |
| monthly_batches.parquet | One record per processing or snapshot period | id | Temporal tracking, batch reconciliation, and release-period analysis. |

## Dataset Domains

- Geography and regions
- Clubs and athlete development context
- Athlete profiles and registrations
- Athlete assessments and longitudinal signals
- Teams and memberships
- Matches, participating teams, participating players, and games
- Monthly batch or release tracking

## Conceptual Relationship Paths

- Region to club to athlete development context
- Athlete to club membership history
- Athlete to team membership history
- Team to match participation
- Match to participating teams and players
- Match to game-level results

## Suggested Configuration Pattern

- Use the dataset configuration file to declare the active dataset.
- Keep dataset storage paths environment-specific and outside committed local config files.
- Align run identifiers with milestone evidence and quality reporting.

## Notes for 5K, 50K, and 250K Execution

- Expect the smaller dataset to be used for workflow proving and schema familiarization.
- Expect the mid-sized dataset to reveal additional engineering and data quality challenges.
- Expect the largest dataset to require careful attention to performance, reproducibility, and evidence capture.

## Student Validation Responsibilities

> The delivered files are the source of truth. Teams must validate actual file availability, field names, data types, row counts, keys, and relationships during ingestion.

- Confirm which files are present in each dataset release.
- Profile schemas rather than assuming consistency.
- Check row counts and missing-file conditions explicitly.
- Document any deviations between expectation and observed data.
