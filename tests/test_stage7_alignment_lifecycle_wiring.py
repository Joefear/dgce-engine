import json
from pathlib import Path

from aether.dgce import (
    DGCESection,
    SectionAlignmentInput,
    SectionApprovalInput,
    SectionExecutionGateInput,
    SectionPreflightInput,
    build_write_transparency,
    load_change_plan,
    record_section_alignment,
    record_section_approval,
    record_section_execution_gate,
    run_section_with_workspace,
)
from aether.dgce.decompose import _stage7_alignment_lifecycle_view
from aether.dgce.file_plan import FilePlan
from aether.dgce.read_api import get_stage7_alignment_read_model
from packages.dgce_contracts.alignment_builder import build_alignment_record_v1, validate_alignment_record_v1
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import ExecutionResult


LEGACY_ALIGNMENT_KEYS = [
    "alignment_blocked",
    "alignment_fingerprint",
    "alignment_reason",
    "alignment_status",
    "alignment_timestamp",
    "artifact_fingerprint",
    "artifact_type",
    "code_graph_used",
    "contract_version",
    "created_written_count",
    "drift_findings",
    "effective_execution_mode",
    "generated_by",
    "intent_alignment",
    "justification_alignment",
    "modify_written_count",
    "require_preflight_pass",
    "schema_version",
    "scope_alignment",
    "section_id",
    "strategy_alignment",
    "written_file_count",
]


STAGE7_READ_MODEL_KEYS = [
    "alignment_id",
    "alignment_result",
    "blocking_issues_count",
    "code_graph_used",
    "drift_codes",
    "drift_detected",
    "enrichment_status",
    "evidence_sources",
    "execution_permitted",
    "informational_issues_count",
    "primary_reason",
    "resolver_used",
    "section_id",
]


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


def _infra_file_plan(*, include_extra: bool = False) -> FilePlan:
    files = [
        {
            "path": "deploy/docker-compose.yaml",
            "purpose": "Deployment manifest",
            "source": "expected_targets",
        }
    ]
    if include_extra:
        files.append(
            {
                "path": "deploy/extra-compose.yaml",
                "purpose": "Unexpected deployment manifest",
                "source": "expected_targets",
            }
        )
    return FilePlan(project_name="DGCE", files=files)


def _stub_executor_result(content: str) -> ExecutionResult:
    return ExecutionResult(
        output="Summary output",
        status=ArtifactStatus.EXPERIMENTAL,
        content=content,
    )


def _patch_stub_executor(monkeypatch) -> None:
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _approve_preview(project_root: Path, *, plan: FilePlan) -> None:
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=plan,
    )
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="create_only",
            approval_timestamp="2026-05-02T22:30:00Z",
        ),
    )


def _record_alignment_direct(project_root: Path, *, plan: FilePlan) -> dict:
    record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-05-02T22:30:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-05-02T22:30:00Z"),
    )
    change_plan = load_change_plan(project_root / ".dce" / "plans" / "mission-board.change_plan.json")
    _, write_transparency = build_write_transparency(plan, change_plan, project_root)
    return record_section_alignment(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        alignment=SectionAlignmentInput(alignment_timestamp="2026-05-02T22:30:00Z"),
        file_plan=plan,
        change_plan=change_plan,
        write_transparency=write_transparency,
    )


def _alignment_target(path: str, *, operation: str) -> dict:
    return {
        "target": path,
        "reference": f"test:{path}",
        "structure": {
            "operation": operation,
        },
    }


def _alignment_record_from_targets(*, approved: list[dict], preview: list[dict], observed: list[dict]) -> dict:
    return build_alignment_record_v1(
        alignment_id="stage7:test:compat",
        timestamp="2026-05-02T22:30:00Z",
        input_fingerprint="a" * 64,
        approval_fingerprint="b" * 64,
        preview_fingerprint="c" * 64,
        approved_design_expectations=approved,
        preview_proposed_targets=preview,
        current_observed_targets=observed,
    )


def _legacy_alignment_view_template(
    *,
    alignment_blocked: bool = False,
    alignment_status: str = "aligned",
    alignment_reason: str = "aligned",
    drift_findings: list[str] | None = None,
) -> dict:
    alignment_record = {
        "status": alignment_status,
        "findings": [],
        "expected": [],
        "actual": [],
    }
    return {
        "alignment_blocked": alignment_blocked,
        "alignment_fingerprint": "legacy-alignment-fingerprint",
        "alignment_reason": alignment_reason,
        "alignment_status": alignment_status,
        "alignment_timestamp": "2026-05-02T22:30:00Z",
        "artifact_fingerprint": "legacy-artifact-fingerprint",
        "artifact_type": "alignment_record",
        "code_graph_used": False,
        "contract_version": "dgce.alignment_record.v1",
        "created_written_count": 0,
        "drift_findings": list(drift_findings or []),
        "effective_execution_mode": "create_only",
        "generated_by": "dgce",
        "intent_alignment": dict(alignment_record),
        "justification_alignment": dict(alignment_record),
        "modify_written_count": 0,
        "require_preflight_pass": True,
        "schema_version": "dgce.artifact.v1",
        "scope_alignment": dict(alignment_record),
        "section_id": "mission-board",
        "strategy_alignment": dict(alignment_record),
        "written_file_count": 0,
    }


def test_informational_v1_structure_mismatch_does_not_block_legacy_view():
    alignment_record = _alignment_record_from_targets(
        approved=[_alignment_target("deploy/docker-compose.yaml", operation="create")],
        preview=[_alignment_target("deploy/docker-compose.yaml", operation="modify")],
        observed=[_alignment_target("deploy/docker-compose.yaml", operation="modify")],
    )

    legacy_view = _stage7_alignment_lifecycle_view(
        alignment_record,
        section_id="mission-board",
        require_preflight_pass=True,
        alignment_input=SectionAlignmentInput(alignment_timestamp="2026-05-02T22:30:00Z"),
        legacy_alignment_view=_legacy_alignment_view_template(),
    )

    assert validate_alignment_record_v1(alignment_record) is True
    assert alignment_record["execution_permitted"] is True
    assert alignment_record["drift_items"] == [
        {
            "code": "structure_mismatch",
            "summary": "Comparable structured metadata differs from approved expectations.",
            "target": "deploy/docker-compose.yaml",
            "severity": "informational",
        }
    ]
    assert sorted(legacy_view.keys()) == LEGACY_ALIGNMENT_KEYS
    assert legacy_view["alignment_status"] == "aligned"
    assert legacy_view["alignment_blocked"] is False
    assert legacy_view["alignment_reason"] == "aligned"
    assert legacy_view["drift_findings"] == []


def test_v1_blocking_drift_falls_back_into_legacy_blocking_view():
    alignment_record = _alignment_record_from_targets(
        approved=[_alignment_target("deploy/docker-compose.yaml", operation="create")],
        preview=[],
        observed=[],
    )

    legacy_view = _stage7_alignment_lifecycle_view(
        alignment_record,
        section_id="mission-board",
        require_preflight_pass=True,
        alignment_input=SectionAlignmentInput(alignment_timestamp="2026-05-02T22:30:00Z"),
        legacy_alignment_view=_legacy_alignment_view_template(),
    )

    assert validate_alignment_record_v1(alignment_record) is True
    assert alignment_record["execution_permitted"] is False
    assert sorted(legacy_view.keys()) == LEGACY_ALIGNMENT_KEYS
    assert legacy_view["alignment_status"] == "misaligned"
    assert legacy_view["alignment_blocked"] is True
    assert legacy_view["drift_findings"] == ["missing_expected_artifact"]
    assert legacy_view["alignment_reason"] == alignment_record["alignment_summary"]["primary_reason"]


def test_record_section_alignment_preserves_legacy_view_and_persists_v1_artifact(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("stage7_lifecycle_legacy_view_and_v1_artifact")
    plan = _infra_file_plan()
    _approve_preview(project_root, plan=plan)

    legacy_view = _record_alignment_direct(project_root, plan=plan)
    alignment = _read_json(project_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json")

    assert sorted(legacy_view.keys()) == LEGACY_ALIGNMENT_KEYS
    assert validate_alignment_record_v1(alignment) is True
    assert legacy_view["artifact_type"] == "alignment_record"
    assert legacy_view["alignment_status"] == alignment["alignment_result"]
    assert legacy_view["alignment_blocked"] is (alignment["execution_permitted"] is False)
    assert legacy_view["alignment_reason"] == "aligned"
    assert alignment["alignment_summary"]["primary_reason"] == "Approved expectations align with preview and observed targets."
    assert legacy_view["alignment_timestamp"] == alignment["timestamp"]
    assert legacy_view["code_graph_used"] is alignment["alignment_enrichment"]["code_graph_used"]
    assert legacy_view["drift_findings"] == [item["code"] for item in alignment["drift_items"]]
    assert legacy_view["scope_alignment"]["status"] == "aligned"
    assert legacy_view["intent_alignment"]["status"] == "aligned"
    assert "alignment_status" not in alignment
    assert "scope_alignment" not in alignment


def test_aligned_stage7_artifact_is_produced_after_stage6_pass(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("stage7_lifecycle_aligned_artifact")
    plan = _infra_file_plan()
    _approve_preview(project_root, plan=plan)

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-05-02T22:30:00Z",
        preflight_validation_timestamp="2026-05-02T22:30:00Z",
        alignment_timestamp="2026-05-02T22:30:00Z",
        execution_timestamp="2026-05-02T22:30:00Z",
        prepared_file_plan=plan,
    )
    alignment_path = project_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json"
    alignment = _read_json(alignment_path)

    assert validate_alignment_record_v1(alignment) is True
    assert alignment["alignment_result"] == "aligned"
    assert alignment["execution_permitted"] is True
    assert result.run_outcome_class != "blocked_alignment"
    assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists()


def test_aligned_stage7_allows_existing_downstream_flow_to_continue(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("stage7_lifecycle_aligned_downstream")
    plan = _infra_file_plan()
    _approve_preview(project_root, plan=plan)

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-05-02T22:30:00Z",
        preflight_validation_timestamp="2026-05-02T22:30:00Z",
        alignment_timestamp="2026-05-02T22:30:00Z",
        simulation_triggered=True,
        simulation_provider="infra_dry_run",
        simulation_trigger_timestamp="2026-05-02T22:30:00Z",
        execution_timestamp="2026-05-02T22:30:00Z",
        prepared_file_plan=plan,
    )

    assert result.run_outcome_class != "blocked_alignment"
    assert (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation_trigger.json").exists()
    assert (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").exists()
    assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists()


def test_misaligned_stage7_artifact_is_produced_and_blocks_before_stage75_and_stage8(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("stage7_lifecycle_misaligned_blocks")
    approved_plan = _infra_file_plan()
    drifted_plan = _infra_file_plan(include_extra=True)
    _approve_preview(project_root, plan=approved_plan)

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-05-02T22:30:00Z",
        preflight_validation_timestamp="2026-05-02T22:30:00Z",
        alignment_timestamp="2026-05-02T22:30:00Z",
        simulation_triggered=True,
        simulation_provider="infra_dry_run",
        simulation_trigger_timestamp="2026-05-02T22:30:00Z",
        execution_timestamp="2026-05-02T22:30:00Z",
        prepared_file_plan=drifted_plan,
    )
    alignment = _read_json(project_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json")

    assert validate_alignment_record_v1(alignment) is True
    assert alignment["alignment_result"] == "misaligned"
    assert alignment["execution_permitted"] is False
    assert [item["code"] for item in alignment["drift_items"]] == ["unexpected_artifact"]
    assert result.run_outcome_class == "blocked_alignment"
    assert result.execution_outcome["status"] == "blocked"
    assert result.execution_outcome["execution_status"] == "not_run_alignment_blocked"
    assert not (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation_trigger.json").exists()
    assert not (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").exists()
    assert not (project_root / ".dce" / "execution" / "mission-board.execution.json").exists()
    assert not (project_root / ".dce" / "outputs" / "mission-board.json").exists()


def test_stage7_block_is_not_labeled_guardrail_policy_or_simulation_failure(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("stage7_lifecycle_block_label")
    _approve_preview(project_root, plan=_infra_file_plan())

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-05-02T22:30:00Z",
        preflight_validation_timestamp="2026-05-02T22:30:00Z",
        alignment_timestamp="2026-05-02T22:30:00Z",
        simulation_triggered=True,
        simulation_provider="infra_dry_run",
        simulation_trigger_timestamp="2026-05-02T22:30:00Z",
        execution_timestamp="2026-05-02T22:30:00Z",
        prepared_file_plan=_infra_file_plan(include_extra=True),
    )
    serialized_outcome = json.dumps(result.execution_outcome, sort_keys=True)

    assert result.run_outcome_class == "blocked_alignment"
    assert "guardrail" not in serialized_outcome.lower()
    assert "policy" not in serialized_outcome.lower()
    assert "simulation" not in serialized_outcome.lower()


def test_stage7_read_api_reads_lifecycle_produced_alignment_artifact(monkeypatch):
    _patch_stub_executor(monkeypatch)
    project_root = _workspace_dir("stage7_lifecycle_read_api")
    plan = _infra_file_plan()
    _approve_preview(project_root, plan=plan)

    run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-05-02T22:30:00Z",
        preflight_validation_timestamp="2026-05-02T22:30:00Z",
        alignment_timestamp="2026-05-02T22:30:00Z",
        execution_timestamp="2026-05-02T22:30:00Z",
        prepared_file_plan=plan,
    )

    read_model = get_stage7_alignment_read_model(project_root, "mission-board")

    assert sorted(read_model.keys()) == STAGE7_READ_MODEL_KEYS
    assert read_model["section_id"] == "mission-board"
    assert read_model["alignment_result"] == "aligned"
    assert read_model["execution_permitted"] is True
    assert read_model["drift_codes"] == []
    assert "timestamp" not in read_model
    assert "drift_items" not in read_model
    assert "evidence" not in read_model
