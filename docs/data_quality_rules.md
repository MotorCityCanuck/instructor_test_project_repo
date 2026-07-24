# Data Quality Rules

**Purpose:** This document defines the team’s planned data quality checks and severity levels for the NAPA Olympic Analytics Platform. The template provides a starter rule catalog only; teams must refine, implement, validate, and justify their own checks based on observed data and analytical needs.

## Rule Design Notes

- Define severity levels in a way the team can defend.
- Keep rule intent connected to business impact.
- Distinguish between rules that block analysis and rules that merely reduce confidence.

| Rule ID | Rule area | Rule description | Severity | Layer | Expected action | Business impact | Team owner |
|---|---|---|---|---|---|---|---|
| DQ-001 | Athlete identity | Confirm athlete identifiers are usable for joins and analysis. |  |  |  |  |  |
| DQ-002 | Athlete status | Confirm roster candidates have usable status and geography fields. |  |  |  |  |  |
| DQ-003 | Rating validity | Confirm rating-related values are interpretable and within expected ranges. |  |  |  |  |  |
| DQ-004 | Team composition | Confirm tournament candidate teams exist and have valid membership evidence. |  |  |  |  |  |
| DQ-005 | Team membership integrity | Confirm player-team relationships resolve correctly. |  |  |  |  |  |
| DQ-006 | Match structure | Confirm matches have the expected structure for analysis. |  |  |  |  |  |
| DQ-007 | Winner integrity | Confirm winners align with participating teams. |  |  |  |  |  |
| DQ-008 | Game integrity | Confirm game scores and winners are logically usable. |  |  |  |  |  |
| DQ-009 | Referential integrity | Confirm keys resolve across major entities. |  |  |  |  |  |
| DQ-010 | Duplicate detection | Identify possible duplicate records. |  |  |  |  |  |
| DQ-011 | Temporal consistency | Confirm key dates progress logically. |  |  |  |  |  |

## Schema 1.5 Team Identity Emphasis

- `DQ-004` should explicitly separate team identity class from analytical team category:
  - `teams.team_type` should be validated as an identity-class field such as `COMPETITIVE` or `AD_HOC`.
  - `teams.team_division` should be validated as the analytical category field used for men's, women's, mixed, and open doubles.
- `DQ-006` should confirm `match_teams.team_id` is populated for accepted match sides and resolves to a valid `teams` record.
- `DQ-007` should confirm `matches.winning_team_id` resolves to one of the same match's `match_teams.team_id` values rather than a `match_teams.id` row identifier.
- `DQ-009` should include cross-table checks for `match_teams.team_id -> teams.id` and `matches.winning_team_id -> same-match match_teams.team_id`.
- `DQ-010` should include duplicate active team-pair detection so the same active player pairing does not appear as multiple concurrent structured teams without review.
