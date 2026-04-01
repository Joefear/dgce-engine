import json
from pathlib import Path

import pytest
from aether.dgce import (
    DGCESection,
    get_artifact_manifest,
    get_consumer_contract,
    get_dashboard,
    get_export_contract,
    get_lifecycle_trace,
    get_workspace_index,
    list_available_artifacts,
    run_section_with_workspace,
)
from aether.dgce import read_api as dgce_read_api
from aether.dgce.path_utils import resolve_workspace_path
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import ExecutionResult


def _section() -> DGCESection:
    return DGCESection(
        section_type="game_system",
        title="Mission Board",
        description="A modular mission board that assembles contracts and tracks player progression.",
        requirements=["support mission templates", "track progression state"],
        constraints=["keep save format stable", "support mod extension points"],
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


def _build_workspace(monkeypatch, name: str) -> Path:
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir(name)

    def fake_run(self, executor_name, content):
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

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root)
    return project_root


def test_read_api_returns_expected_validated_artifacts(monkeypatch):
    project_root = _build_workspace(monkeypatch, "dgce_read_api_success")

    assert get_dashboard(project_root) == json.loads((project_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"))
    assert get_workspace_index(project_root) == json.loads((project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))
    assert get_lifecycle_trace(project_root) == json.loads((project_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8"))
    assert get_consumer_contract(project_root) == json.loads((project_root / ".dce" / "consumer_contract.json").read_text(encoding="utf-8"))
    assert get_export_contract(project_root) == json.loads((project_root / ".dce" / "export_contract.json").read_text(encoding="utf-8"))
    assert get_artifact_manifest(project_root) == json.loads((project_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"))
    assert list_available_artifacts(project_root) == get_artifact_manifest(project_root)


def test_read_api_repeated_calls_are_identical_and_read_only(monkeypatch):
    project_root = _build_workspace(monkeypatch, "dgce_read_api_repeat")
    artifact_paths = [
        project_root / ".dce" / "dashboard.json",
        project_root / ".dce" / "workspace_index.json",
        project_root / ".dce" / "lifecycle_trace.json",
        project_root / ".dce" / "consumer_contract.json",
        project_root / ".dce" / "export_contract.json",
        project_root / ".dce" / "artifact_manifest.json",
    ]
    before_bytes = {path: path.read_bytes() for path in artifact_paths}

    first_dashboard = get_dashboard(project_root)
    second_dashboard = get_dashboard(project_root)
    assert first_dashboard == second_dashboard

    first_dashboard["artifact_type"] = "mutated"
    assert get_dashboard(project_root)["artifact_type"] == "dashboard"
    assert before_bytes == {path: path.read_bytes() for path in artifact_paths}


def test_read_api_raises_for_invalid_artifact(monkeypatch):
    project_root = _build_workspace(monkeypatch, "dgce_read_api_invalid")
    dashboard_path = project_root / ".dce" / "dashboard.json"
    invalid_payload = json.loads(dashboard_path.read_text(encoding="utf-8"))
    invalid_payload.pop("sections")
    dashboard_path.write_text(json.dumps(invalid_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ValueError):
        get_dashboard(project_root)


def test_read_api_has_no_write_side_effects(monkeypatch):
    project_root = _build_workspace(monkeypatch, "dgce_read_api_no_writes")
    artifact_paths = [
        project_root / ".dce" / "dashboard.json",
        project_root / ".dce" / "workspace_index.json",
        project_root / ".dce" / "lifecycle_trace.json",
        project_root / ".dce" / "consumer_contract.json",
        project_root / ".dce" / "export_contract.json",
        project_root / ".dce" / "artifact_manifest.json",
    ]
    before = {path: path.read_bytes() for path in artifact_paths}

    dgce_read_api.get_dashboard(project_root)
    dgce_read_api.get_workspace_index(project_root)
    dgce_read_api.get_lifecycle_trace(project_root)
    dgce_read_api.get_consumer_contract(project_root)
    dgce_read_api.get_export_contract(project_root)
    dgce_read_api.get_artifact_manifest(project_root)
    dgce_read_api.list_available_artifacts(project_root)

    assert before == {path: path.read_bytes() for path in artifact_paths}


def test_resolve_workspace_path_accepts_safe_relative_path(monkeypatch):
    project_root = _build_workspace(monkeypatch, "dgce_read_api_relative")
    relative_path = project_root.resolve().relative_to(Path.cwd().resolve())

    assert resolve_workspace_path(str(relative_path)) == project_root.resolve()
    assert get_dashboard(str(relative_path)) == json.loads((project_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"))


def test_resolve_workspace_path_rejects_relative_escape():
    with pytest.raises(ValueError, match="must remain within the current working directory"):
        resolve_workspace_path("..")


def test_read_api_raises_for_workspace_without_dce_directory():
    workspace_path = _workspace_dir("dgce_read_api_missing_dce")
    workspace_path.mkdir(parents=True, exist_ok=True)

    with pytest.raises(ValueError, match="must contain a \\.dce directory"):
        get_dashboard(workspace_path)


def test_read_api_raises_for_workspace_path_that_is_a_file():
    base = _workspace_dir("dgce_read_api_file_path")
    base.mkdir(parents=True, exist_ok=True)
    file_path = base / "workspace.txt"
    file_path.write_text("x", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a directory"):
        get_dashboard(file_path)


def test_read_api_raises_for_nonexistent_workspace_path():
    with pytest.raises(FileNotFoundError, match="Workspace path does not exist"):
        get_dashboard("tests/.tmp/does-not-exist")
