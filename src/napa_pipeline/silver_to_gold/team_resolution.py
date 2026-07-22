"""Persistent-team resolution helpers for the Silver-to-Gold pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


DIRECT_VALID_TEAM_ID = "DIRECT_VALID_TEAM_ID"
ACTIVE_MEMBERSHIP_PAIR = "ACTIVE_MEMBERSHIP_PAIR"
UNIQUE_HISTORICAL_PAIR = "UNIQUE_HISTORICAL_PAIR"
UNRESOLVED = "UNRESOLVED"
AMBIGUOUS = "AMBIGUOUS"
RESOLVED = "RESOLVED"

DIRECT_RESOLUTION_CONFIDENCE = 1.0
ACTIVE_PAIR_RESOLUTION_CONFIDENCE = 0.9
HISTORICAL_PAIR_RESOLUTION_CONFIDENCE = 0.6
UNRESOLVED_CONFIDENCE = 0.0


@dataclass(frozen=True)
class ResolvedMatchTeamsResult:
    """Resolved match-side rows and summary counts for one Phase 4 build."""

    rows: tuple[dict[str, Any], ...]
    direct_resolution_count: int
    active_pair_resolution_count: int
    historical_pair_resolution_count: int
    ambiguous_count: int
    unresolved_count: int
    persistent_team_resolution_pct: float


def build_resolved_match_teams(
    match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_team_players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> ResolvedMatchTeamsResult:
    """Resolve persistent teams for historical match sides without fabricating team IDs."""
    teams_by_id = {
        str(row["team_id"]): row
        for row in teams_rows
        if row.get("team_id") not in (None, "")
    }
    player_ids_by_match_team = _group_player_ids_by_match_team(match_team_players_rows)
    active_pair_index = _build_active_pair_index(team_memberships_rows, teams_by_id)
    historical_pair_index = _build_historical_pair_index(team_memberships_rows)

    resolved_rows: list[dict[str, Any]] = []
    direct_resolution_count = 0
    active_pair_resolution_count = 0
    historical_pair_resolution_count = 0
    ambiguous_count = 0
    unresolved_count = 0

    for match_team_row in match_teams_rows:
        resolved_row = _resolve_match_team_row(
            match_team_row,
            teams_by_id=teams_by_id,
            player_ids_by_match_team=player_ids_by_match_team,
            active_pair_index=active_pair_index,
            historical_pair_index=historical_pair_index,
        )
        resolved_rows.append(resolved_row)

        method = resolved_row["team_resolution_method"]
        status = resolved_row["team_resolution_status"]
        if method == DIRECT_VALID_TEAM_ID:
            direct_resolution_count += 1
        elif method == ACTIVE_MEMBERSHIP_PAIR:
            active_pair_resolution_count += 1
        elif method == UNIQUE_HISTORICAL_PAIR:
            historical_pair_resolution_count += 1
        elif status == AMBIGUOUS:
            ambiguous_count += 1
        else:
            unresolved_count += 1

    resolved_count = (
        direct_resolution_count
        + active_pair_resolution_count
        + historical_pair_resolution_count
    )
    persistent_team_resolution_pct = (
        (resolved_count / len(resolved_rows)) * 100.0 if resolved_rows else 0.0
    )

    return ResolvedMatchTeamsResult(
        rows=tuple(resolved_rows),
        direct_resolution_count=direct_resolution_count,
        active_pair_resolution_count=active_pair_resolution_count,
        historical_pair_resolution_count=historical_pair_resolution_count,
        ambiguous_count=ambiguous_count,
        unresolved_count=unresolved_count,
        persistent_team_resolution_pct=persistent_team_resolution_pct,
    )


def _resolve_match_team_row(
    match_team_row: dict[str, Any],
    *,
    teams_by_id: dict[str, dict[str, Any]],
    player_ids_by_match_team: dict[str, list[str]],
    active_pair_index: dict[tuple[str, str], list[tuple[str, date | None, date | None]]],
    historical_pair_index: dict[tuple[str, str], list[str]],
) -> dict[str, Any]:
    match_team_id = str(match_team_row["match_team_id"])
    match_id = str(match_team_row["match_id"])
    team_number = match_team_row.get("team_number")
    direct_team_id = _normalize_optional_string(match_team_row.get("team_id"))
    match_date = _parse_date_value(match_team_row.get("match_date"))
    player_ids = sorted(set(player_ids_by_match_team.get(match_team_id, [])))
    pair_key = _canonical_pair_key(player_ids)
    pair_tuple = _canonical_pair_tuple(player_ids)

    if direct_team_id is not None:
        direct_team = teams_by_id.get(direct_team_id)
        if direct_team is not None and _team_is_valid_for_match_date(direct_team, match_date):
            return _build_resolved_row(
                match_id=match_id,
                match_team_id=match_team_id,
                team_number=team_number,
                match_date=match_date,
                canonical_player_pair_key=pair_key,
                resolved_team_id=direct_team_id,
                team_resolution_method=DIRECT_VALID_TEAM_ID,
                team_resolution_status=RESOLVED,
                team_resolution_confidence=DIRECT_RESOLUTION_CONFIDENCE,
                candidate_attribution_allowed_flag=_is_candidate_attributable_team(direct_team),
            )

    if pair_tuple is None or match_date is None:
        return _build_resolved_row(
            match_id=match_id,
            match_team_id=match_team_id,
            team_number=team_number,
            match_date=match_date,
            canonical_player_pair_key=pair_key,
            resolved_team_id=None,
            team_resolution_method=UNRESOLVED,
            team_resolution_status=UNRESOLVED,
            team_resolution_confidence=UNRESOLVED_CONFIDENCE,
            candidate_attribution_allowed_flag=False,
        )

    active_candidates = _resolve_active_pair_candidates(
        pair_tuple,
        match_date=match_date,
        active_pair_index=active_pair_index,
        teams_by_id=teams_by_id,
    )
    if len(active_candidates) == 1:
        resolved_team_id = active_candidates[0]
        return _build_resolved_row(
            match_id=match_id,
            match_team_id=match_team_id,
            team_number=team_number,
            match_date=match_date,
            canonical_player_pair_key=pair_key,
            resolved_team_id=resolved_team_id,
            team_resolution_method=ACTIVE_MEMBERSHIP_PAIR,
            team_resolution_status=RESOLVED,
            team_resolution_confidence=ACTIVE_PAIR_RESOLUTION_CONFIDENCE,
            candidate_attribution_allowed_flag=_is_candidate_attributable_team(
                teams_by_id[resolved_team_id]
            ),
        )
    if len(active_candidates) > 1:
        return _build_resolved_row(
            match_id=match_id,
            match_team_id=match_team_id,
            team_number=team_number,
            match_date=match_date,
            canonical_player_pair_key=pair_key,
            resolved_team_id=None,
            team_resolution_method=AMBIGUOUS,
            team_resolution_status=AMBIGUOUS,
            team_resolution_confidence=UNRESOLVED_CONFIDENCE,
            candidate_attribution_allowed_flag=False,
        )

    historical_candidates = historical_pair_index.get(pair_tuple, [])
    if len(historical_candidates) == 1:
        resolved_team_id = historical_candidates[0]
        return _build_resolved_row(
            match_id=match_id,
            match_team_id=match_team_id,
            team_number=team_number,
            match_date=match_date,
            canonical_player_pair_key=pair_key,
            resolved_team_id=resolved_team_id,
            team_resolution_method=UNIQUE_HISTORICAL_PAIR,
            team_resolution_status=RESOLVED,
            team_resolution_confidence=HISTORICAL_PAIR_RESOLUTION_CONFIDENCE,
            candidate_attribution_allowed_flag=_is_candidate_attributable_team(
                teams_by_id.get(resolved_team_id, {})
            ),
        )
    if len(historical_candidates) > 1:
        return _build_resolved_row(
            match_id=match_id,
            match_team_id=match_team_id,
            team_number=team_number,
            match_date=match_date,
            canonical_player_pair_key=pair_key,
            resolved_team_id=None,
            team_resolution_method=AMBIGUOUS,
            team_resolution_status=AMBIGUOUS,
            team_resolution_confidence=UNRESOLVED_CONFIDENCE,
            candidate_attribution_allowed_flag=False,
        )

    return _build_resolved_row(
        match_id=match_id,
        match_team_id=match_team_id,
        team_number=team_number,
        match_date=match_date,
        canonical_player_pair_key=pair_key,
        resolved_team_id=None,
        team_resolution_method=UNRESOLVED,
        team_resolution_status=UNRESOLVED,
        team_resolution_confidence=UNRESOLVED_CONFIDENCE,
        candidate_attribution_allowed_flag=False,
    )


def _build_resolved_row(
    *,
    match_id: str,
    match_team_id: str,
    team_number: Any,
    match_date: date | None,
    canonical_player_pair_key: str | None,
    resolved_team_id: str | None,
    team_resolution_method: str,
    team_resolution_status: str,
    team_resolution_confidence: float,
    candidate_attribution_allowed_flag: bool,
) -> dict[str, Any]:
    return {
        "match_id": match_id,
        "match_team_id": match_team_id,
        "team_number": team_number,
        "match_date": match_date,
        "canonical_player_pair_key": canonical_player_pair_key,
        "resolved_team_id": resolved_team_id,
        "team_resolution_method": team_resolution_method,
        "team_resolution_status": team_resolution_status,
        "team_resolution_confidence": team_resolution_confidence,
        "candidate_attribution_allowed_flag": candidate_attribution_allowed_flag,
    }


def _group_player_ids_by_match_team(
    match_team_players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]
) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for row in match_team_players_rows:
        match_team_id = _normalize_optional_string(row.get("match_team_id"))
        player_id = _normalize_optional_string(row.get("player_id"))
        if match_team_id is None or player_id is None:
            continue
        grouped.setdefault(match_team_id, []).append(player_id)
    return grouped


def _build_active_pair_index(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    teams_by_id: dict[str, dict[str, Any]],
) -> dict[tuple[str, str], list[tuple[str, date | None, date | None]]]:
    team_memberships = _group_memberships_by_team(team_memberships_rows)
    pair_index: dict[tuple[str, str], list[tuple[str, date | None, date | None]]] = {}

    for team_id, memberships in team_memberships.items():
        if team_id not in teams_by_id:
            continue
        unique_memberships = _dedupe_memberships(memberships)
        for first_index, first_membership in enumerate(unique_memberships):
            for second_membership in unique_memberships[first_index + 1 :]:
                first_player = _normalize_optional_string(first_membership.get("player_id"))
                second_player = _normalize_optional_string(second_membership.get("player_id"))
                if first_player is None or second_player is None or first_player == second_player:
                    continue
                overlap_start, overlap_end = _membership_overlap_window(
                    first_membership,
                    second_membership,
                )
                if overlap_start is None and overlap_end is None and (
                    _parse_date_value(first_membership.get("membership_start_date")) is not None
                    or _parse_date_value(second_membership.get("membership_start_date")) is not None
                ):
                    continue
                pair_index.setdefault(
                    _sorted_pair(first_player, second_player),
                    [],
                ).append((team_id, overlap_start, overlap_end))
    return pair_index


def _build_historical_pair_index(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[tuple[str, str], list[str]]:
    team_memberships = _group_memberships_by_team(team_memberships_rows)
    pair_index: dict[tuple[str, str], set[str]] = {}

    for team_id, memberships in team_memberships.items():
        unique_memberships = _dedupe_memberships(memberships)
        for first_index, first_membership in enumerate(unique_memberships):
            for second_membership in unique_memberships[first_index + 1 :]:
                first_player = _normalize_optional_string(first_membership.get("player_id"))
                second_player = _normalize_optional_string(second_membership.get("player_id"))
                if first_player is None or second_player is None or first_player == second_player:
                    continue
                overlap_start, overlap_end = _membership_overlap_window(
                    first_membership,
                    second_membership,
                )
                if overlap_start is None and overlap_end is None and (
                    _parse_date_value(first_membership.get("membership_start_date")) is not None
                    or _parse_date_value(second_membership.get("membership_start_date")) is not None
                ):
                    continue
                pair_index.setdefault(_sorted_pair(first_player, second_player), set()).add(team_id)

    return {
        pair_key: sorted(team_ids)
        for pair_key, team_ids in pair_index.items()
    }


def _resolve_active_pair_candidates(
    pair_key: tuple[str, str],
    *,
    match_date: date,
    active_pair_index: dict[tuple[str, str], list[tuple[str, date | None, date | None]]],
    teams_by_id: dict[str, dict[str, Any]],
) -> list[str]:
    candidates: set[str] = set()
    for team_id, overlap_start, overlap_end in active_pair_index.get(pair_key, []):
        if overlap_start is not None and match_date < overlap_start:
            continue
        if overlap_end is not None and match_date > overlap_end:
            continue
        team_row = teams_by_id.get(team_id)
        if team_row is None or not _team_is_valid_for_match_date(team_row, match_date):
            continue
        candidates.add(team_id)
    return sorted(candidates)


def _group_memberships_by_team(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in team_memberships_rows:
        team_id = _normalize_optional_string(row.get("team_id"))
        if team_id is None:
            continue
        grouped.setdefault(team_id, []).append(row)
    return grouped


def _dedupe_memberships(memberships: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str | None, date | None, date | None]] = set()
    for membership in memberships:
        signature = (
            _normalize_optional_string(membership.get("player_id")),
            _parse_date_value(membership.get("membership_start_date")),
            _parse_date_value(membership.get("membership_end_date")),
        )
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(membership)
    return deduped


def _membership_overlap_window(
    first_membership: dict[str, Any],
    second_membership: dict[str, Any],
) -> tuple[date | None, date | None]:
    first_start = _parse_date_value(first_membership.get("membership_start_date"))
    first_end = _parse_date_value(first_membership.get("membership_end_date"))
    second_start = _parse_date_value(second_membership.get("membership_start_date"))
    second_end = _parse_date_value(second_membership.get("membership_end_date"))

    overlap_start = _max_date(first_start, second_start)
    overlap_end = _min_date(first_end, second_end)
    if overlap_start is not None and overlap_end is not None and overlap_start > overlap_end:
        return None, None
    return overlap_start, overlap_end


def _canonical_pair_key(player_ids: list[str]) -> str | None:
    if len(player_ids) != 2:
        return None
    return f"{player_ids[0]}:{player_ids[1]}"


def _canonical_pair_tuple(player_ids: list[str]) -> tuple[str, str] | None:
    if len(player_ids) != 2:
        return None
    return player_ids[0], player_ids[1]


def _sorted_pair(first_player: str, second_player: str) -> tuple[str, str]:
    return tuple(sorted((first_player, second_player)))  # type: ignore[return-value]


def _normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_date_value(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _team_is_valid_for_match_date(team_row: dict[str, Any], match_date: date | None) -> bool:
    if match_date is None:
        return True
    formation_date = _parse_date_value(team_row.get("formation_date"))
    dissolution_date = _parse_date_value(team_row.get("dissolution_date"))
    if formation_date is not None and match_date < formation_date:
        return False
    if dissolution_date is not None and match_date > dissolution_date:
        return False
    return True


def _is_candidate_attributable_team(team_row: dict[str, Any]) -> bool:
    if not team_row:
        return False
    active_flag = team_row.get("active_flag")
    if isinstance(active_flag, bool):
        return active_flag
    status = _normalize_optional_string(team_row.get("team_status"))
    if status is None:
        return False
    return status.upper() == "ACTIVE"


def _max_date(first_value: date | None, second_value: date | None) -> date | None:
    if first_value is None:
        return second_value
    if second_value is None:
        return first_value
    return max(first_value, second_value)


def _min_date(first_value: date | None, second_value: date | None) -> date | None:
    if first_value is None:
        return second_value
    if second_value is None:
        return first_value
    return min(first_value, second_value)
