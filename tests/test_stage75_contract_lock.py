import json
import subprocess
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from apps.aether_api.main import create_app
import aether.dgce.decompose as dgce_decompose
from aether.dgce import (
    DGCESection,
    SectionApprovalInput,
    SectionSimulationInput,
    execute_reserved_simulation_gate,
    record_section_approval,
    record_section_simulation,
    run_section_with_workspace,
)
from aether.dgce.file_plan import FilePlan
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


def _infra_file_plan(path: str = "deploy/docker-compose.yaml") -> FilePlan:
    return FilePlan(
        project_name="DGCE",
        files=[
            {
                "path": path,
                "purpose": "Deployment manifest",
                "source": "expected_targets",
            }
        ],
    )


def _stub_executor_result(content: str) -> ExecutionResult:
    return ExecutionResult(
        output="Summary output",
        status=ArtifactStatus.EXPERIMENTAL,
        content=content,
    )


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_simulation_artifact(project_root: Path, section_id: str = "mission-board") -> dict:
    return _read_json(project_root / ".dce" / "execution" / "simulation" / f"{section_id}.simulation.json")


def _read_workspace_projection(project_root: Path, surface: str, section_id: str = "mission-board") -> dict:
    surface_paths = {
        "review_index": project_root / ".dce" / "reviews" / "index.json",
        "workspace_summary": project_root / ".dce" / "workspace_summary.json",
        "workspace_index": project_root / ".dce" / "workspace_index.json",
        "dashboard": project_root / ".dce" / "dashboard.json",
    }
    payload = _read_json(surface_paths[surface])
    return next(entry for entry in payload["sections"] if entry["section_id"] == section_id)["section_summary"]["simulation"]


def _read_api_projection(client: TestClient, project_root: Path, surface: str, section_id: str = "mission-board") -> dict:
    response_paths = {
        "api_summary": f"/v1/dgce/sections/{section_id}/summary",
        "api_overview": f"/v1/dgce/sections/{section_id}/overview",
        "api_dashboard": f"/v1/dgce/sections/{section_id}/dashboard",
    }
    response = client.get(response_paths[surface], params={"workspace_path": str(project_root.resolve())})
    assert response.status_code == 200
    return response.json()["simulation"]


def _all_simulation_projections(project_root: Path, section_id: str = "mission-board") -> dict[str, dict]:
    client = TestClient(create_app())
    projections = {
        "review_index": _read_workspace_projection(project_root, "review_index", section_id),
        "workspace_summary": _read_workspace_projection(project_root, "workspace_summary", section_id),
        "workspace_index": _read_workspace_projection(project_root, "workspace_index", section_id),
        "dashboard": _read_workspace_projection(project_root, "dashboard", section_id),
        "api_summary": _read_api_projection(client, project_root, "api_summary", section_id),
        "api_overview": _read_api_projection(client, project_root, "api_overview", section_id),
        "api_dashboard": _read_api_projection(client, project_root, "api_dashboard", section_id),
    }
    return projections


def _assert_simulation_projection_invariants(projection: dict) -> None:
    assert projection["findings_count"] == len(projection["finding_codes"])
    assert projection["provider_execution_summary"]
    assert projection["provider_execution_state"] in {
        "not_run",
        "executed",
        "forced_override",
        "unavailable",
        "timeout",
        "input_invalid",
        "artifact_invalid",
    }
    if projection["simulation_stage_applicable"] is False:
        assert projection["simulation_status"] is None
        assert projection["simulation_triggered"] is False
        assert projection["provider_execution_state"] == "not_run"
        assert projection["applicable_providers"] == []
        assert projection["selected_provider"] is None
    if projection["simulation_status"] == "skipped":
        assert projection["simulation_stage_applicable"] is True
        assert projection["simulation_triggered"] is False
        assert projection["findings_count"] == 0
        assert projection["finding_codes"] == []
        assert projection["provider_execution_state"] == "not_run"
        assert projection["provider_resolution"] is None
        assert projection["selected_provider"] is None
        assert projection["applicable_providers"] == []
    if projection["simulation_status"] in {"pass", "fail", "indeterminate"}:
        assert projection["simulation_triggered"] is True
    if projection["provider_resolution"] in {"explicit", "inferred"}:
        assert projection["selected_provider"] == projection["simulation_provider"]
        assert projection["simulation_provider"] in projection["applicable_providers"]
    if projection["provider_resolution"] == "forced_override":
        assert projection["selected_provider"] == projection["simulation_provider"]
    if projection["advisory_provider"] is None:
        assert projection["provider_execution_state"] in {
            "not_run",
            "executed",
            "forced_override",
            "unavailable",
            "timeout",
            "input_invalid",
            "artifact_invalid",
        }


def _assert_all_projections_equal(project_root: Path, section_id: str = "mission-board") -> dict:
    projections = _all_simulation_projections(project_root, section_id)
    first_projection = next(iter(projections.values()))
    for projection in projections.values():
        assert projection == first_projection
        _assert_simulation_projection_invariants(projection)
    return first_projection


def _assert_findings_normalized(findings: list[dict], *, authoritative_provider: str, advisory_provider: str | None) -> None:
    seen_findings: set[tuple[str, str, str, str]] = set()
    for finding in findings:
        assert set(finding.keys()) <= {"code", "summary", "target", "provider"}
        assert isinstance(finding["code"], str) and finding["code"]
        assert isinstance(finding["summary"], str) and finding["summary"]
        if "target" in finding and finding["target"] is not None:
            assert isinstance(finding["target"], str) and finding["target"]
        if advisory_provider is None:
            assert finding.get("provider") in {None, authoritative_provider}
        else:
            assert finding.get("provider") in {authoritative_provider, advisory_provider}
        finding_key = (
            str(finding.get("target") or ""),
            str(finding.get("provider") or ""),
            str(finding.get("code") or ""),
            str(finding.get("summary") or ""),
        )
        assert finding_key not in seen_findings
        seen_findings.add(finding_key)


def _patch_stub_executor(monkeypatch) -> None:
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)


def _record_basic_approval(project_root: Path, *, selected_mode: str = "create_only") -> None:
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode=selected_mode,
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )


def test_stage75_contract_lock_authoritative_only_baseline(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("dgce_stage75_contract_lock_authoritative_only")
    prepared_file_plan = _infra_file_plan()

    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )
    _record_basic_approval(project_root, selected_mode="create_only")

    run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        simulation_triggered=True,
        simulation_provider="infra_dry_run",
        simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_artifact = _read_simulation_artifact(project_root)
    assert simulation_artifact["provider_name"] == "infra_dry_run"
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": "infra_dry_run",
        "advisory_provider": None,
        "composition_mode": "authoritative_only",
    }
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": ["infra_dry_run"],
        "resolution": "explicit",
        "selected_provider": "infra_dry_run",
    }
    assert simulation_artifact["advisory_execution"] == {
        "state": "not_run",
        "summary": "simulation not executed",
        "target": None,
    }
    assert simulation_artifact["findings"] == []
    projection = _assert_all_projections_equal(project_root)
    assert projection["simulation_status"] == "pass"
    assert projection["simulation_provider"] == "infra_dry_run"
    assert projection["selected_provider"] == "infra_dry_run"
    assert projection["provider_resolution"] == "explicit"


def test_stage75_contract_lock_advisory_findings_are_normalized_attributed_and_deduped(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("dgce_stage75_contract_lock_advisory_findings")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("version: '3'\nservices: {}\n", encoding="utf-8")

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr="services.app.image must be a string\nservices.app.image must be a string\n",
        )

    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        allow_safe_modify=True,
        prepared_file_plan=prepared_file_plan,
    )
    _record_basic_approval(project_root, selected_mode="safe_modify")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = _read_simulation_artifact(project_root)
    assert simulation_gate["provider_name"] == "infra_dry_run"
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": "infra_dry_run",
        "advisory_provider": "external_dry_run",
        "composition_mode": "authoritative_plus_advisory",
    }
    assert simulation_artifact["advisory_execution"] == {
        "state": "executed",
        "summary": "docker compose config executed with blocking findings",
        "target": "deploy/docker-compose.yaml",
    }
    assert simulation_artifact["findings"] == [
        {
            "code": "external_compose_type_mismatch",
            "provider": "external_dry_run",
            "summary": "Docker Compose validation reported a field with an invalid type.",
            "target": "deploy/docker-compose.yaml",
        },
        {
            "code": "infra_modify_candidate",
            "provider": "infra_dry_run",
            "summary": "Infrastructure dry-run detected a modify candidate.",
            "target": "deploy/docker-compose.yaml",
        },
    ]
    _assert_findings_normalized(
        simulation_artifact["findings"],
        authoritative_provider="infra_dry_run",
        advisory_provider="external_dry_run",
    )
    projection = _assert_all_projections_equal(project_root)
    assert projection["simulation_status"] == "fail"
    assert projection["simulation_provider"] == "infra_dry_run"
    assert projection["advisory_provider"] == "external_dry_run"
    assert projection["finding_codes"] == ["external_compose_type_mismatch", "infra_modify_candidate"]


def test_stage75_contract_lock_advisory_failure_is_fail_safe(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("dgce_stage75_contract_lock_advisory_failure")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("version: '3'\nservices: {}\n", encoding="utf-8")

    def fake_subprocess_run(command, **_kwargs):
        raise subprocess.TimeoutExpired(command, timeout=dgce_decompose._EXTERNAL_DRY_RUN_TIMEOUT_SECONDS)

    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        allow_safe_modify=True,
        prepared_file_plan=prepared_file_plan,
    )
    _record_basic_approval(project_root, selected_mode="safe_modify")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = _read_simulation_artifact(project_root)
    assert simulation_gate["provider_name"] == "infra_dry_run"
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["reason_code"] == "simulation_fail"
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": "infra_dry_run",
        "advisory_provider": "external_dry_run",
        "composition_mode": "authoritative_plus_advisory",
    }
    assert simulation_artifact["advisory_execution"] == {
        "state": "timeout",
        "summary": "external command timed out",
        "target": "deploy/docker-compose.yaml",
    }
    assert simulation_artifact["findings"] == [
        {
            "code": "infra_modify_candidate",
            "provider": "infra_dry_run",
            "summary": "Infrastructure dry-run detected a modify candidate.",
            "target": "deploy/docker-compose.yaml",
        }
    ]
    _assert_findings_normalized(
        simulation_artifact["findings"],
        authoritative_provider="infra_dry_run",
        advisory_provider="external_dry_run",
    )
    projection = _assert_all_projections_equal(project_root)
    assert projection["simulation_status"] == "fail"
    assert projection["advisory_provider"] == "external_dry_run"


def test_stage75_contract_lock_forced_override_invariants():
    project_root = _workspace_dir("dgce_stage75_contract_lock_forced_override")
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    _record_basic_approval(project_root, selected_mode="create_only")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="infra_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = _read_simulation_artifact(project_root)
    assert simulation_gate["provider_name"] == "infra_dry_run"
    assert simulation_gate["provider_selection_source"] == "explicit"
    assert simulation_gate["provider_selection_reason"] == "forced_override"
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": "infra_dry_run",
        "advisory_provider": None,
        "composition_mode": "authoritative_only",
    }
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": [],
        "resolution": "forced_override",
        "selected_provider": "infra_dry_run",
    }
    projection = _assert_all_projections_equal(project_root)
    assert projection["simulation_status"] == "indeterminate"
    assert projection["provider_execution_state"] == "forced_override"
    assert projection["selected_provider"] == "infra_dry_run"
    assert projection["simulation_provider"] == "infra_dry_run"


def test_stage75_contract_lock_multi_provider_conflict_is_fail_closed(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("dgce_stage75_contract_lock_conflict")
    prepared_file_plan = _infra_file_plan()

    monkeypatch.setattr(dgce_decompose, "_SIMULATION_PROVIDER_PRECEDENCE", ())
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )
    _record_basic_approval(project_root, selected_mode="create_only")
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status="pass",
            provider_name="workspace_artifact",
            provider_selection_reason="seeded_workspace_artifact",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = _read_simulation_artifact(project_root)
    assert simulation_gate["provider_resolution_status"] == "unresolved"
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "simulation_provider_conflict"
    assert simulation_artifact["provider_name"] is None
    assert simulation_artifact["provider_execution_state"] == "not_run"
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": None,
        "advisory_provider": None,
        "composition_mode": "authoritative_only",
    }
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": ["infra_dry_run", "workspace_artifact"],
        "resolution": "conflict",
        "selected_provider": None,
    }
    projection = _assert_all_projections_equal(project_root)
    assert projection["simulation_status"] == "indeterminate"
    assert projection["provider_resolution"] == "conflict"
    assert projection["selected_provider"] is None


def test_stage75_contract_lock_unresolved_selection_is_fail_closed(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("dgce_stage75_contract_lock_unresolved")
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    _record_basic_approval(project_root, selected_mode="create_only")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = _read_simulation_artifact(project_root)
    assert simulation_gate["provider_resolution_status"] == "unresolved"
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "simulation_provider_unresolved"
    assert simulation_artifact["provider_execution_state"] == "not_run"
    assert simulation_artifact["provider_name"] is None
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": [],
        "resolution": "unresolved",
        "selected_provider": None,
    }
    projection = _assert_all_projections_equal(project_root)
    assert projection["simulation_status"] == "indeterminate"
    assert projection["provider_resolution"] == "unresolved"
    assert projection["selected_provider"] is None


def test_stage75_contract_lock_workspace_infra_external_interaction_is_deterministic(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("dgce_stage75_contract_lock_workspace_infra_external")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("version: '3'\nservices: {}\n", encoding="utf-8")

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="services.app Additional property typo is not allowed\n")

    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        allow_safe_modify=True,
        prepared_file_plan=prepared_file_plan,
    )
    _record_basic_approval(project_root, selected_mode="safe_modify")
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status="fail",
            findings=["approved write set violates deterministic safe modify boundary"],
            provider_name="workspace_artifact",
            provider_selection_reason="seeded_workspace_artifact",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = _read_simulation_artifact(project_root)
    assert simulation_gate["provider_name"] == "workspace_artifact"
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": "workspace_artifact",
        "advisory_provider": "infra_dry_run",
        "composition_mode": "authoritative_plus_advisory",
    }
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": ["external_dry_run", "infra_dry_run", "workspace_artifact"],
        "resolution": "inferred",
        "selected_provider": "workspace_artifact",
    }
    assert simulation_artifact["advisory_execution"] == {
        "state": "executed",
        "summary": "infra dry-run executed with blocking findings",
        "target": None,
    }
    _assert_findings_normalized(
        simulation_artifact["findings"],
        authoritative_provider="workspace_artifact",
        advisory_provider="infra_dry_run",
    )
    projection = _assert_all_projections_equal(project_root)
    assert projection["simulation_status"] == "fail"
    assert projection["simulation_provider"] == "workspace_artifact"
    assert projection["advisory_provider"] == "infra_dry_run"


def test_stage75_contract_lock_trigger_false_projects_skipped_everywhere():
    project_root = _workspace_dir("dgce_stage75_contract_lock_skipped")
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    _record_basic_approval(project_root, selected_mode="create_only")

    execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=False,
            simulation_provider="infra_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    projection = _assert_all_projections_equal(project_root)
    assert projection == {
        "applicable_providers": [],
        "advisory_provider": None,
        "findings_count": 0,
        "finding_codes": [],
        "provider_execution_state": "not_run",
        "provider_execution_summary": "simulation not executed",
        "provider_execution_target": None,
        "provider_selection_source": "not_applicable",
        "provider_resolution": None,
        "reason_code": None,
        "reason_summary": None,
        "selected_provider": None,
        "simulation_provider": "infra_dry_run",
        "simulation_stage_applicable": True,
        "simulation_status": "skipped",
        "simulation_triggered": False,
        "trigger_reason_codes": [],
        "trigger_reason_summary": None,
    }


@pytest.mark.parametrize(
    ("case_name", "builder", "expected_state", "expected_summary"),
    [
        (
            "external_unavailable",
            "external_unavailable",
            "unavailable",
            "external command unavailable",
        ),
        (
            "forced_override",
            "forced_override",
            "forced_override",
            "infra dry-run forced override applied",
        ),
        (
            "unresolved",
            "unresolved",
            "not_run",
            "simulation not executed",
        ),
    ],
)
def test_stage75_contract_lock_execution_trace_matrix(monkeypatch, case_name, builder, expected_state, expected_summary):
    if builder in {"external_unavailable", "unresolved"}:
        _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir(f"dgce_stage75_contract_lock_execution_trace_{case_name}")

    if builder == "external_unavailable":
        prepared_file_plan = _infra_file_plan()
        compose_path = project_root / "deploy" / "docker-compose.yaml"
        compose_path.parent.mkdir(parents=True, exist_ok=True)
        compose_path.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")

        def fake_subprocess_run(command, **_kwargs):
            raise FileNotFoundError("docker not found")

        monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
        run_section_with_workspace(
            _section(),
            project_root,
            incremental_mode="incremental_v2_2",
            prepared_file_plan=prepared_file_plan,
        )
        _record_basic_approval(project_root, selected_mode="create_only")
        execute_reserved_simulation_gate(
            project_root,
            "mission-board",
            require_preflight_pass=True,
            simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
                simulation_triggered=True,
                simulation_provider="external_dry_run",
                simulation_trigger_timestamp="2026-03-26T00:00:00Z",
            ),
        )
    elif builder == "forced_override":
        run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
        _record_basic_approval(project_root, selected_mode="create_only")
        execute_reserved_simulation_gate(
            project_root,
            "mission-board",
            require_preflight_pass=True,
            simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
                simulation_triggered=True,
                simulation_provider="infra_dry_run",
                simulation_trigger_timestamp="2026-03-26T00:00:00Z",
            ),
        )
    else:
        run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
        _record_basic_approval(project_root, selected_mode="create_only")
        execute_reserved_simulation_gate(
            project_root,
            "mission-board",
            require_preflight_pass=True,
            simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
                simulation_triggered=True,
                simulation_trigger_timestamp="2026-03-26T00:00:00Z",
            ),
        )

    simulation_artifact = _read_simulation_artifact(project_root)
    assert simulation_artifact["provider_execution_state"] == expected_state
    assert simulation_artifact["provider_execution_summary"] == expected_summary
    projection = _assert_all_projections_equal(project_root)
    assert projection["provider_execution_state"] == expected_state
    assert projection["provider_execution_summary"] == expected_summary


def test_stage75_contract_lock_lifecycle_order_is_unchanged():
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
