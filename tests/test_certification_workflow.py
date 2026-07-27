"""Tests for Raw certification workflow-support helpers."""

from napa_pipeline.certification.workflow import (
    evaluate_release_gate,
    resolve_certification_run_id,
)


def test_resolve_certification_run_id_uses_supplied_value_when_present() -> None:
    assert resolve_certification_run_id("run-123") == "run-123"


def test_resolve_certification_run_id_generates_value_when_blank() -> None:
    generated = resolve_certification_run_id(" ")
    assert generated
    assert generated != " "


def test_release_gate_fails_rejected_release() -> None:
    result = evaluate_release_gate(
        certification_decision="REJECTED",
        fail_on="blocker",
        severity_counts={"warning": 0, "error": 1, "blocker": 1},
    )

    assert result.should_fail is True
    assert "rejected" in result.message.lower()


def test_release_gate_can_fail_on_warnings() -> None:
    result = evaluate_release_gate(
        certification_decision="CERTIFIED_WITH_WARNINGS",
        fail_on="warning",
        severity_counts={"warning": 2, "error": 0, "blocker": 0},
    )

    assert result.should_fail is True
    assert "fail_on=warning" in result.message


def test_release_gate_can_be_disabled() -> None:
    result = evaluate_release_gate(
        certification_decision="CERTIFIED_WITH_WARNINGS",
        fail_on="never",
        severity_counts={"warning": 2, "error": 0, "blocker": 0},
    )

    assert result.should_fail is False
