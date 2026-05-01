import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

import dce


def _workspace_dir(name: str) -> Path:
    base = Path("tests/.tmp") / name
    if base.exists():
        for path in sorted(base.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        if base.exists():
            base.rmdir()
    return base


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run_status(monkeypatch, root: Path, section_id: str) -> str:
    monkeypatch.chdir(root)
    monkeypatch.setattr(sys, "argv", ["dce.py", "status", section_id])
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        dce.main()
    return buffer.getvalue()


def test_status_validation_failure_does_not_report_nothing_pending(monkeypatch):
    root = _workspace_dir("dce_status_validation_failure")
    project_root = root / "defiant-sky"
    _write_json(
        project_root / ".dce" / "approvals" / "data-model.approval.json",
        {"approval_status": "superseded", "selected_mode": "no_changes"},
    )
    _write_json(
        project_root / ".dce" / "preflight" / "data-model.stale_check.json",
        {"stale_status": "stale_valid", "stale_detected": False},
    )
    _write_json(
        project_root / ".dce" / "preflight" / "data-model.preflight.json",
        {"preflight_status": "preflight_pass"},
    )
    _write_json(
        project_root / ".dce" / "execution" / "gate" / "data-model.execution_gate.json",
        {"gate_status": "gate_pass"},
    )
    _write_json(
        project_root / ".dce" / "preflight" / "data-model.alignment.json",
        {"alignment_status": "alignment_pass"},
    )
    _write_json(
        project_root / ".dce" / "execution" / "data-model.execution.json",
        {"execution_status": "execution_completed_no_changes", "run_outcome_class": "validation_failure"},
    )

    output = _run_status(monkeypatch, root, "data-model")

    assert "outcome: validation_failure" in output
    assert "problem: validation_failure" in output
    assert "nothing pending" not in output


def test_status_validation_failure_uses_new_next_action(monkeypatch):
    root = _workspace_dir("dce_status_validation_next_action")
    project_root = root / "defiant-sky"
    _write_json(
        project_root / ".dce" / "approvals" / "data-model.approval.json",
        {"approval_status": "superseded", "selected_mode": "no_changes"},
    )
    _write_json(
        project_root / ".dce" / "preflight" / "data-model.stale_check.json",
        {"stale_status": "stale_valid", "stale_detected": False},
    )
    _write_json(
        project_root / ".dce" / "execution" / "data-model.execution.json",
        {"execution_status": "execution_completed_no_changes", "run_outcome_class": "validation_failure"},
    )

    output = _run_status(monkeypatch, root, "data-model")

    assert "inspect output validation failure and correct the section input or generator" in output


def test_status_completed_no_changes_success_still_reports_nothing_pending(monkeypatch):
    root = _workspace_dir("dce_status_success_no_changes")
    project_root = root / "defiant-sky"
    _write_json(
        project_root / ".dce" / "approvals" / "data-model.approval.json",
        {"approval_status": "superseded", "selected_mode": "no_changes"},
    )
    _write_json(
        project_root / ".dce" / "preflight" / "data-model.stale_check.json",
        {"stale_status": "stale_valid", "stale_detected": False},
    )
    _write_json(
        project_root / ".dce" / "preflight" / "data-model.preflight.json",
        {"preflight_status": "preflight_pass"},
    )
    _write_json(
        project_root / ".dce" / "execution" / "gate" / "data-model.execution_gate.json",
        {"gate_status": "gate_pass"},
    )
    _write_json(
        project_root / ".dce" / "preflight" / "data-model.alignment.json",
        {"alignment_status": "alignment_pass"},
    )
    _write_json(
        project_root / ".dce" / "execution" / "data-model.execution.json",
        {"execution_status": "execution_completed_no_changes", "run_outcome_class": "partial_skipped_identical"},
    )

    output = _run_status(monkeypatch, root, "data-model")

    assert "outcome: partial_skipped_identical" in output
    assert "Next Action:" in output
    assert "nothing pending" in output


def test_status_stale_invalidated_behavior_is_unchanged(monkeypatch):
    root = _workspace_dir("dce_status_stale_invalidated")
    project_root = root / "defiant-sky"
    _write_json(
        project_root / ".dce" / "approvals" / "data-model.approval.json",
        {"approval_status": "approved", "selected_mode": "create_only"},
    )
    _write_json(
        project_root / ".dce" / "preflight" / "data-model.stale_check.json",
        {"stale_status": "stale_invalidated", "stale_detected": True},
    )
    _write_json(
        project_root / ".dce" / "execution" / "data-model.execution.json",
        {"execution_status": "execution_completed", "run_outcome_class": "success_create_only"},
    )

    output = _run_status(monkeypatch, root, "data-model")

    assert "regenerate preview and review, then re-approve" in output
