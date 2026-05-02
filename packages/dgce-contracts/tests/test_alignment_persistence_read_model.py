import json
from pathlib import Path

import pytest

from packages.dgce_contracts.alignment_artifacts import (
    alignment_record_artifact_path,
    build_alignment_record_read_model_v1,
    load_alignment_record_read_model_v1,
    persist_alignment_record_v1,
)
from packages.dgce_contracts.alignment_builder import build_alignment_record_v1, validate_alignment_record_v1


TIMESTAMP = "2026-05-02T20:00:00Z"
INPUT_FP = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
APPROVAL_FP = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
PREVIEW_FP = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"


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


def _target(target: str, *, reference: str | None = None, structure: dict | None = None) -> dict:
    payload = {
        "target": target,
        "reference": reference or f"artifact://{target}",
    }
    if structure is not None:
        payload["structure"] = structure
    return payload


def _alignment_record(**kwargs) -> dict:
    defaults = {
        "alignment_id": "alignment.persistence.test.001",
        "timestamp": TIMESTAMP,
        "input_fingerprint": INPUT_FP,
        "approval_fingerprint": APPROVAL_FP,
        "preview_fingerprint": PREVIEW_FP,
        "approved_design_expectations": [
            _target("api/mission.py", structure={"kind": "api", "version": 1}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
        "preview_proposed_targets": [
            _target("api/mission.py", structure={"kind": "api", "version": 1}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
        "current_observed_targets": [
            _target("api/mission.py", structure={"kind": "api", "version": 1}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
    }
    defaults.update(kwargs)
    return build_alignment_record_v1(**defaults)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_persists_valid_alignment_record_to_expected_dce_path():
    workspace_path = _workspace_dir("alignment_persistence_valid")
    record = _alignment_record()

    persisted = persist_alignment_record_v1(record, workspace_path=workspace_path, section_id="mission-board")

    expected_path = workspace_path / ".dce" / "execution" / "alignment" / "mission-board.alignment.json"
    assert persisted.artifact_path == ".dce/execution/alignment/mission-board.alignment.json"
    assert alignment_record_artifact_path(workspace_path, "mission-board") == expected_path.resolve()
    assert expected_path.exists()
    assert expected_path.read_text(encoding="utf-8").endswith("\n")
    assert _read_json(expected_path) == record


def test_refuses_to_persist_invalid_alignment_record():
    workspace_path = _workspace_dir("alignment_persistence_invalid")
    record = _alignment_record()
    record["raw_provider_text"] = "not allowed"

    with pytest.raises(ValueError, match="alignment_record invalid"):
        persist_alignment_record_v1(record, workspace_path=workspace_path, section_id="mission-board")

    assert not (workspace_path / ".dce").exists()


def test_persisted_artifact_validates_against_alignment_record_schema():
    workspace_path = _workspace_dir("alignment_persistence_schema_valid")
    persisted = persist_alignment_record_v1(
        _alignment_record(),
        workspace_path=workspace_path,
        section_id="mission-board",
    )

    payload = _read_json(workspace_path / persisted.artifact_path)

    assert validate_alignment_record_v1(payload) is True


def test_read_model_projection_is_correct_for_aligned_record():
    workspace_path = _workspace_dir("alignment_read_model_aligned")
    record = _alignment_record()
    persisted = persist_alignment_record_v1(record, workspace_path=workspace_path, section_id="mission-board")

    projection = load_alignment_record_read_model_v1(workspace_path, "mission-board")

    assert projection == persisted.read_model
    assert projection == {
        "section_id": "mission-board",
        "alignment_id": record["alignment_id"],
        "alignment_result": "aligned",
        "drift_detected": False,
        "execution_permitted": True,
        "blocking_issues_count": 0,
        "informational_issues_count": 0,
        "primary_reason": "Approved expectations align with preview and observed targets.",
        "drift_codes": [],
        "evidence_sources": ["approval", "preview", "runtime_state"],
        "enrichment_status": "not_used",
        "code_graph_used": False,
        "resolver_used": False,
    }


def test_read_model_projection_is_correct_for_misaligned_record():
    workspace_path = _workspace_dir("alignment_read_model_misaligned")
    record = _alignment_record(
        preview_proposed_targets=[
            _target("api/mission.py", structure={"kind": "api", "version": 2}),
            _target("debug/extra.py", structure={"kind": "debug", "version": 1}),
        ],
        current_observed_targets=[
            _target("api/mission.py", structure={"kind": "api", "version": 2}),
        ],
    )

    persist_alignment_record_v1(record, workspace_path=workspace_path, section_id="mission-board")
    projection = load_alignment_record_read_model_v1(workspace_path, "mission-board")

    assert projection["section_id"] == "mission-board"
    assert projection["alignment_id"] == record["alignment_id"]
    assert projection["alignment_result"] == "misaligned"
    assert projection["drift_detected"] is True
    assert projection["execution_permitted"] is False
    assert projection["blocking_issues_count"] == 2
    assert projection["informational_issues_count"] == 1
    assert projection["primary_reason"] == "Approved expected artifact is missing from preview and observed targets."
    assert projection["enrichment_status"] == "not_used"
    assert projection["code_graph_used"] is False
    assert projection["resolver_used"] is False


def test_read_model_includes_drift_codes_and_evidence_sources():
    record = _alignment_record(
        preview_proposed_targets=[
            _target("api/mission.py", structure={"kind": "api", "version": 2}),
            _target("debug/extra.py", structure={"kind": "debug", "version": 1}),
        ],
        current_observed_targets=[
            _target("api/mission.py", structure={"kind": "api", "version": 2}),
        ],
    )

    projection = build_alignment_record_read_model_v1("mission-board", record)

    assert projection["drift_codes"] == [
        "missing_expected_artifact",
        "structure_mismatch",
        "unexpected_artifact",
    ]
    assert projection["evidence_sources"] == ["approval", "preview", "runtime_state"]


def test_persistence_creates_no_lifecycle_execution_or_stage8_artifacts():
    workspace_path = _workspace_dir("alignment_persistence_no_execution")

    persist_alignment_record_v1(_alignment_record(), workspace_path=workspace_path, section_id="mission-board")
    load_alignment_record_read_model_v1(workspace_path, "mission-board")

    assert (workspace_path / ".dce" / "execution" / "alignment" / "mission-board.alignment.json").exists()
    assert not (workspace_path / ".dce" / "execution" / "mission-board.execution.json").exists()
    assert not (workspace_path / ".dce" / "execution" / "gate").exists()
    assert not (workspace_path / ".dce" / "execution" / "simulation").exists()
    assert not (workspace_path / ".dce" / "execution" / "stage8").exists()
    assert not (workspace_path / ".dce" / "outputs").exists()
    assert not (workspace_path / ".dce" / "output").exists()
