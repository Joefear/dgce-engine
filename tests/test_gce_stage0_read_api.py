import inspect
import json
from pathlib import Path

from fastapi.testclient import TestClient

from apps.aether_api.main import create_app
from aether.dgce import (
    DGCESection,
    get_artifact_manifest,
    get_gce_stage0_artifact,
    list_gce_stage0_artifacts,
    persist_stage0_input,
    run_section_with_workspace,
)
import aether.dgce.decompose as dgce_decompose
import aether.dgce.read_api as dgce_read_api
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import ExecutionResult


def _metadata(source_id: str = "read-model-gdd-v1") -> dict:
    return {
        "project_id": "read-model-project",
        "project_name": "Read Model Project",
        "owner": "Design Authority",
        "source_id": source_id,
        "created_at": "2026-04-27T00:00:00Z",
        "updated_at": "2026-04-27T00:00:00Z",
    }


def _sections() -> list[dict]:
    return [
        {
            "section_id": "project_identity",
            "title": "Project Identity",
            "classification": "durable",
            "authorship": "human",
            "required": True,
            "content": {"purpose": "Define a bounded feature."},
        },
        {
            "section_id": "runtime_state",
            "title": "Runtime State",
            "classification": "volatile",
            "authorship": "injected",
            "required": False,
            "content": {"placeholder": "stage_0_state"},
        },
    ]


def _formal_gdd_input(source_id: str = "read-model-gdd-v1") -> dict:
    return {
        "contract_name": "GCEIngestionCore",
        "contract_version": "gce.ingestion.core.v1",
        "input_path": "formal_gdd",
        "metadata": _metadata(source_id),
        "document": {
            "session_objective": "Generate a bounded mission board feature.",
            "sections": _sections(),
        },
        "ambiguities": [],
    }


def _structured_intent_input(source_id: str = "read-model-structured-v1") -> dict:
    return {
        "contract_name": "GCEIngestionCore",
        "contract_version": "gce.ingestion.core.v1",
        "input_path": "structured_intent",
        "metadata": _metadata(source_id),
        "intent": {
            "session_objective": "Generate a bounded mission board feature.",
            "sections": _sections(),
        },
        "ambiguities": [],
    }


def _ambiguous_input() -> dict:
    payload = _formal_gdd_input("read-model-ambiguous-v1")
    payload["ambiguities"] = [
        {
            "field_path": "document.sections[0].content.scope",
            "question": "Which scope is authoritative?",
            "blocking": True,
        }
    ]
    return payload


def _software_section() -> DGCESection:
    return DGCESection(
        section_type="game_system",
        title="Mission Board",
        description="A modular mission board that assembles contracts and tracks progression.",
        requirements=["support mission templates"],
        constraints=["keep save format stable"],
    )


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


def _stub_executor_result(content: str) -> ExecutionResult:
    return ExecutionResult(
        output="Summary output",
        status=ArtifactStatus.EXPERIMENTAL,
        executor="stub",
        metadata={
            "real_model_called": False,
            "model_backend": "stub",
            "model_name": None,
            "estimated_tokens": len(content) / 4,
            "estimated_cost": (len(content) / 4) * 0.000002,
            "inference_avoided": False,
            "backend_used": "stub",
            "worth_running": True,
        },
    )


def test_valid_gce_persisted_artifact_appears_in_list_and_detail_read_model():
    project_root = _workspace_dir("gce_stage0_read_valid")
    persisted = persist_stage0_input(project_root, _formal_gdd_input())
    artifact_name = Path(persisted.artifact_path).name

    index = list_gce_stage0_artifacts(project_root)
    detail = get_gce_stage0_artifact(project_root, artifact_name)

    assert index["artifact_type"] == "gce_stage0_artifact_index"
    assert index["artifact_count"] == 1
    assert index["artifacts"] == [detail]
    assert detail["read_model_type"] == "gce_stage0_artifact_read_model"
    assert detail["artifact_type"] == "stage0_input_package"
    assert detail["contract_name"] == "DGCEStage0InputPackage"
    assert detail["contract_version"] == "dgce.stage0.input_package.v1"
    assert detail["input_path"] == "formal_gdd"
    assert detail["reason_code"] is None
    assert detail["stage_1_release"] == {"blocked": False, "reason_code": None}
    assert detail["artifact_fingerprint"] == persisted.artifact["artifact_fingerprint"]
    assert detail["normalized_session_intent_summary"] == {
        "contract_name": "GCESessionIntent",
        "contract_version": "gce.session_intent.v1",
        "source_input_path": "formal_gdd",
        "session_objective": "Generate a bounded mission board feature.",
        "metadata": {
            "project_id": "read-model-project",
            "project_name": "Read Model Project",
            "source_id": "read-model-gdd-v1",
        },
        "section_count": 2,
        "section_ids": ["project_identity", "runtime_state"],
        "section_classifications": {
            "project_identity": {
                "authorship": "human",
                "classification": "durable",
                "required": True,
            },
            "runtime_state": {
                "authorship": "injected",
                "classification": "volatile",
                "required": False,
            },
        },
    }
    assert detail["clarification_request_summary"] is None


def test_gce_detail_read_verifies_artifact_fingerprint_fail_closed():
    project_root = _workspace_dir("gce_stage0_read_invalid_fingerprint")
    persisted = persist_stage0_input(project_root, _formal_gdd_input())
    artifact_path = project_root / persisted.artifact_path
    artifact_name = artifact_path.name
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    payload["input_path"] = "structured_intent"
    artifact_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    detail = get_gce_stage0_artifact(project_root, artifact_name)

    assert detail["artifact_type"] == "gce_stage0_artifact_read_error"
    assert detail["reason_code"] == "artifact_fingerprint_invalid"
    assert detail["artifact_fingerprint"] == persisted.artifact["artifact_fingerprint"]
    assert detail["stage_1_release"] == {
        "blocked": True,
        "reason_code": "artifact_fingerprint_invalid",
    }


def test_invalid_gce_persisted_artifact_appears_as_blocked_in_read_model():
    project_root = _workspace_dir("gce_stage0_read_validation_failed")
    payload = _structured_intent_input()
    del payload["metadata"]["owner"]
    persisted = persist_stage0_input(project_root, payload)

    detail = get_gce_stage0_artifact(project_root, Path(persisted.artifact_path).name)

    assert detail["artifact_type"] == "stage0_input_package"
    assert detail["input_path"] == "structured_intent"
    assert detail["reason_code"] == "validation_failed"
    assert detail["stage_1_release"] == {
        "blocked": True,
        "reason_code": "validation_failed",
    }
    assert detail["normalized_session_intent_summary"] is None
    assert detail["clarification_request_summary"] is None


def test_clarification_artifact_exposes_summary_without_release():
    project_root = _workspace_dir("gce_stage0_read_clarification")
    persisted = persist_stage0_input(project_root, _ambiguous_input())

    detail = get_gce_stage0_artifact(project_root, Path(persisted.artifact_path).name)

    assert detail["reason_code"] == "clarification_required"
    assert detail["stage_1_release"] == {
        "blocked": True,
        "reason_code": "clarification_required",
    }
    assert detail["normalized_session_intent_summary"] is None
    assert detail["clarification_request_summary"] == {
        "artifact_type": "clarification_request",
        "contract_name": "GCEClarificationRequest",
        "contract_version": "gce.clarification_request.v1",
        "source_input_path": "formal_gdd",
        "reason_code": "unresolved_intent",
        "stage_1_release_blocked": True,
        "question_count": 1,
        "questions": [
            {
                "id": "clarification-001",
                "field_path": "document.sections[0].content.scope",
                "question": "Which scope is authoritative?",
                "blocking": True,
            }
        ],
        "clarification_request_fingerprint": detail["clarification_request_summary"][
            "clarification_request_fingerprint"
        ],
    }


def test_malformed_gce_artifact_detail_read_fails_closed():
    project_root = _workspace_dir("gce_stage0_read_malformed")
    persist_stage0_input(project_root, _formal_gdd_input())
    artifact_path = project_root / ".dce" / "input" / "gce" / "broken.formal-gdd.stage0.json"
    artifact_path.write_text("{not valid json", encoding="utf-8")

    detail = get_gce_stage0_artifact(project_root, artifact_path.name)

    assert detail["artifact_type"] == "gce_stage0_artifact_read_error"
    assert detail["reason_code"] == "artifact_malformed"
    assert detail["stage_1_release"] == {
        "blocked": True,
        "reason_code": "artifact_malformed",
    }


def test_missing_gce_artifact_detail_read_fails_closed():
    project_root = _workspace_dir("gce_stage0_read_missing")
    persist_stage0_input(project_root, _formal_gdd_input())

    detail = get_gce_stage0_artifact(project_root, "missing.formal-gdd.stage0.json")

    assert detail["artifact_type"] == "gce_stage0_artifact_read_error"
    assert detail["reason_code"] == "artifact_missing"
    assert detail["stage_1_release"] == {
        "blocked": True,
        "reason_code": "artifact_missing",
    }


def test_gce_read_api_does_not_change_existing_software_read_behavior(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    project_root = _workspace_dir("gce_stage0_read_software_unchanged")
    run_section_with_workspace(_software_section(), project_root)
    expected_manifest = json.loads((project_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"))

    assert get_artifact_manifest(project_root) == expected_manifest
    assert list_gce_stage0_artifacts(project_root) == {
        "artifact_type": "gce_stage0_artifact_index",
        "adapter": "gce",
        "contract_name": "GCEStage0ReadModel",
        "contract_version": "gce.stage0.read_model.v1",
        "artifact_count": 0,
        "artifacts": [],
    }
    assert get_artifact_manifest(project_root) == expected_manifest


def test_gce_stage0_http_read_routes_are_read_only():
    project_root = _workspace_dir("gce_stage0_read_http")
    persisted = persist_stage0_input(project_root, _formal_gdd_input())
    artifact_name = Path(persisted.artifact_path).name
    artifact_path = project_root / persisted.artifact_path
    before = artifact_path.read_bytes()
    client = TestClient(create_app())

    index_response = client.get("/v1/dgce/gce/stage0-artifacts", params={"workspace_path": str(project_root)})
    detail_response = client.get(
        f"/v1/dgce/gce/stage0-artifacts/{artifact_name}",
        params={"workspace_path": str(project_root)},
    )

    assert index_response.status_code == 200
    assert detail_response.status_code == 200
    assert index_response.json()["artifacts"] == [detail_response.json()]
    assert detail_response.json()["artifact_fingerprint"] == persisted.artifact["artifact_fingerprint"]
    assert artifact_path.read_bytes() == before


def test_gce_stage0_read_api_does_not_change_stage75_lifecycle_order():
    assert dgce_decompose.DGCE_LIFECYCLE_ORDER == [
        "preview",
        "review",
        "approval",
        "preflight",
        "gate",
        "alignment",
        "execution",
        "outputs",
    ]


def test_gce_stage0_read_api_introduces_no_code_graph_dependency():
    source = inspect.getsource(dgce_read_api).lower()

    assert "code_graph" not in source
    assert "dcg" not in source
