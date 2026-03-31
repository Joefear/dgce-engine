import io
import json
from pathlib import Path

from rich.console import Console

from aether.dgce import DGCESection, run_section_with_workspace
from aether.dgce.inspector import main
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import ExecutionResult


def _section(title: str = "Mission Board") -> DGCESection:
    return DGCESection(
        section_type="game_system",
        title=title,
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


def _write_section_output(project_root: Path, section_id: str, payload: dict) -> None:
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stub_executor_output(content: str) -> str:
    lowered = content.lower()
    if "system breakdown" in lowered:
        return json.dumps(
            {
                "module_name": "mission_board",
                "purpose": "coordinate mission generation",
                "subcomponents": ["templates", "tracker"],
                "dependencies": ["save_state"],
                "implementation_order": ["templates", "tracker"],
            }
        )
    if "data model" in lowered:
        return json.dumps(
            {
                "entities": ["Mission"],
                "fields": ["id", "state"],
                "relationships": ["mission->player"],
                "validation_rules": ["id required"],
            }
        )
    if "api surface" in lowered:
        return json.dumps(
            {
                "interfaces": ["MissionBoardService"],
                "methods": ["create_mission"],
                "inputs": ["template_id"],
                "outputs": ["mission_id"],
                "error_cases": ["template_missing"],
            }
        )
    return "Summary output"


def _stub_executor_result(content: str) -> ExecutionResult:
    return ExecutionResult(
        output=_stub_executor_output(content),
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


def test_inspector_loads_valid_workspace_and_prints_output(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_inspector_valid")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root)

    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["inspect", str(project_root)], console=console)
    output = stream.getvalue()

    assert exit_code == 0
    assert "DGCE Workspace Inspector" in output
    assert "Workspace Summary" in output
    assert "mission-board" in output
    assert "success_create_only" in output
    assert "Latest run mode" not in output
    assert "Latest run outcome class" not in output


def test_inspector_section_flag_focuses_one_section(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_inspector_section")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section("Mission Board"), project_root)
    run_section_with_workspace(_section("Alpha Section"), project_root)

    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["inspect", str(project_root), "--section", "alpha-section"], console=console)
    output = stream.getvalue()

    assert exit_code == 0
    assert "Section Detail" in output
    assert "alpha-section" in output
    assert "mission-board" not in output


def test_inspector_handles_missing_workspace_cleanly():
    project_root = _workspace_dir("dgce_inspector_missing")
    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["inspect", str(project_root)], console=console)

    assert exit_code == 1
    assert "DGCE workspace not found" in stream.getvalue()


def test_inspector_handles_missing_workspace_summary_cleanly():
    project_root = _workspace_dir("dgce_inspector_missing_summary")
    (project_root / ".dce").mkdir(parents=True, exist_ok=True)
    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["inspect", str(project_root)], console=console)

    assert exit_code == 1
    assert "Required artifact missing" in stream.getvalue()
    assert "workspace_summary.json" in stream.getvalue()


def test_inspector_orders_non_success_sections_first(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_inspector_ordering")
    dce_root = project_root / ".dce"
    outputs_dir = dce_root / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    (dce_root / "workspace_summary.json").write_text(
        json.dumps(
            {
                "total_sections_seen": 2,
                "sections": [
                    {
                        "section_id": "zeta-section",
                        "latest_run_mode": "create_only",
                        "latest_run_outcome_class": "success_create_only",
                        "latest_status": "success",
                        "latest_validation_ok": True,
                        "latest_advisory_type": None,
                        "latest_advisory_explanation": None,
                        "latest_written_files_count": 1,
                        "latest_skipped_modify_count": 0,
                        "latest_skipped_ignore_count": 0,
                    },
                    {
                        "section_id": "alpha-section",
                        "latest_run_mode": "create_only",
                        "latest_run_outcome_class": "execution_error",
                        "latest_status": "error",
                        "latest_validation_ok": True,
                        "latest_advisory_type": "process_adjustment",
                        "latest_advisory_explanation": ["execution_error"],
                        "latest_written_files_count": 0,
                        "latest_skipped_modify_count": 0,
                        "latest_skipped_ignore_count": 0,
                    },
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (dce_root / "advisory_index.json").write_text(
        json.dumps(
            {
                "section_id": "alpha-section",
                "run_mode": "create_only",
                "run_outcome_class": "execution_error",
                "status": "error",
                "validation_ok": True,
                "advisory_type": "process_adjustment",
                "advisory_explanation": ["execution_error"],
                "written_files_count": 0,
                "skipped_modify_count": 0,
                "skipped_ignore_count": 0,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (dce_root / "ownership_index.json").write_text(
        json.dumps({"files": []}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["inspect", str(project_root)], console=console)
    output = stream.getvalue()

    assert exit_code == 0
    assert output.index("alpha-section") < output.index("zeta-section")


def test_inspector_skips_single_section_write_summary_cleanly_when_output_missing():
    project_root = _workspace_dir("dgce_inspector_missing_single_output")
    dce_root = project_root / ".dce"
    outputs_dir = dce_root / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    (dce_root / "workspace_summary.json").write_text(
        json.dumps(
            {
                "total_sections_seen": 1,
                "sections": [
                    {
                        "section_id": "mission-board",
                        "latest_run_mode": "create_only",
                        "latest_run_outcome_class": "success_create_only",
                        "latest_status": "success",
                        "latest_validation_ok": True,
                        "latest_advisory_type": None,
                        "latest_advisory_explanation": None,
                        "latest_written_files_count": 0,
                        "latest_skipped_modify_count": 0,
                        "latest_skipped_ignore_count": 0,
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (dce_root / "ownership_index.json").write_text(
        json.dumps({"files": []}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["inspect", str(project_root)], console=console)
    output = stream.getvalue()

    assert exit_code == 0
    assert "Write Summary" in output
    assert "Section output not found: mission-board" in output


def test_explain_run_reports_validation_failure():
    project_root = _workspace_dir("dgce_explain_validation_failure")
    _write_section_output(
        project_root,
        "mission-board",
        {
            "section_id": "mission-board",
            "run_mode": "create_only",
            "run_outcome_class": "validation_failure",
            "file_plan": {"project_name": "DGCE", "files": []},
            "execution_outcome": {
                "section_id": "mission-board",
                "stage": "WRITE",
                "status": "error",
                "validation_summary": {
                    "ok": False,
                    "error": "missing_keys",
                    "missing_keys": ["relationships", "validation_rules"],
                },
                "change_plan_summary": {"create_count": 0, "modify_count": 0, "ignore_count": 0},
                "execution_summary": {
                    "written_files_count": 3,
                    "skipped_modify_count": 0,
                    "skipped_ignore_count": 0,
                    "skipped_identical_count": 0,
                    "skipped_ownership_count": 0,
                    "skipped_exists_fallback_count": 0,
                },
            },
            "advisory": {
                "type": "policy_adjustment",
                "summary": "Review schema contract handling for mission-board",
                "explanation": ["validation_failed", "missing_required_keys"],
            },
            "write_transparency": {
                "write_decisions": [],
                "write_summary": {
                    "written_count": 3,
                    "modify_written_count": 0,
                    "diff_visible_count": 0,
                    "skipped_modify_count": 0,
                    "skipped_ignore_count": 0,
                    "skipped_identical_count": 0,
                    "skipped_ownership_count": 0,
                    "skipped_exists_fallback_count": 0,
                    "before_bytes_total": 0,
                    "after_bytes_total": 0,
                    "changed_lines_estimate_total": 0,
                    "bytes_written_total": 120,
                },
            },
        },
    )

    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["explain", str(project_root), "--section", "mission-board"], console=console)
    output = stream.getvalue()

    assert exit_code == 0
    assert "Section ID: mission-board" in output
    assert "Run outcome class: validation_failure" in output
    assert "Validation failed" in output
    assert "missing keys: relationships, validation_rules" in output
    assert "Advisory Summary" in output
    assert "policy_adjustment" in output


def test_explain_run_reports_ownership_blocked_modify():
    project_root = _workspace_dir("dgce_explain_ownership_blocked")
    _write_section_output(
        project_root,
        "mission-board",
        {
            "section_id": "mission-board",
            "run_mode": "safe_modify",
            "run_outcome_class": "partial_skipped_ownership",
            "file_plan": {"project_name": "DGCE", "files": []},
            "execution_outcome": {
                "section_id": "mission-board",
                "stage": "WRITE",
                "status": "partial",
                "validation_summary": {"ok": True, "error": None, "missing_keys": []},
                "change_plan_summary": {"create_count": 2, "modify_count": 1, "ignore_count": 0},
                "execution_summary": {
                    "written_files_count": 2,
                    "skipped_modify_count": 0,
                    "skipped_ignore_count": 0,
                    "skipped_identical_count": 0,
                    "skipped_ownership_count": 1,
                    "skipped_exists_fallback_count": 0,
                },
            },
            "advisory": None,
            "write_transparency": {
                "write_decisions": [{"path": "mission_board/service.py", "decision": "skipped", "reason": "ownership"}],
                "write_summary": {
                    "written_count": 2,
                    "modify_written_count": 0,
                    "diff_visible_count": 0,
                    "skipped_modify_count": 0,
                    "skipped_ignore_count": 0,
                    "skipped_identical_count": 0,
                    "skipped_ownership_count": 1,
                    "skipped_exists_fallback_count": 0,
                    "before_bytes_total": 0,
                    "after_bytes_total": 0,
                    "changed_lines_estimate_total": 0,
                    "bytes_written_total": 64,
                },
            },
        },
    )

    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["explain", str(project_root), "--section", "mission-board"], console=console)
    output = stream.getvalue()

    assert exit_code == 0
    assert "Run mode: safe_modify" in output
    assert "Safe Modify was enabled for this run." in output
    assert "blocked because they were not owned" in output


def test_explain_run_reports_identical_modify_skip():
    project_root = _workspace_dir("dgce_explain_identical_skip")
    _write_section_output(
        project_root,
        "mission-board",
        {
            "section_id": "mission-board",
            "run_mode": "safe_modify",
            "run_outcome_class": "partial_skipped_identical",
            "file_plan": {"project_name": "DGCE", "files": []},
            "execution_outcome": {
                "section_id": "mission-board",
                "stage": "WRITE",
                "status": "success",
                "validation_summary": {"ok": True, "error": None, "missing_keys": []},
                "change_plan_summary": {"create_count": 3, "modify_count": 1, "ignore_count": 0},
                "execution_summary": {
                    "written_files_count": 3,
                    "skipped_modify_count": 0,
                    "skipped_ignore_count": 0,
                    "skipped_identical_count": 1,
                    "skipped_ownership_count": 0,
                    "skipped_exists_fallback_count": 0,
                },
            },
            "advisory": None,
            "write_transparency": {
                "write_decisions": [{"path": "mission_board/service.py", "decision": "skipped", "reason": "identical"}],
                "write_summary": {
                    "written_count": 3,
                    "modify_written_count": 0,
                    "diff_visible_count": 0,
                    "skipped_modify_count": 0,
                    "skipped_ignore_count": 0,
                    "skipped_identical_count": 1,
                    "skipped_ownership_count": 0,
                    "skipped_exists_fallback_count": 0,
                    "before_bytes_total": 0,
                    "after_bytes_total": 0,
                    "changed_lines_estimate_total": 0,
                    "bytes_written_total": 96,
                },
            },
        },
    )

    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["explain", str(project_root), "--section", "mission-board"], console=console)
    output = stream.getvalue()

    assert exit_code == 0
    assert "partial_skipped_identical" in output
    assert "generated content was identical" in output


def test_explain_run_reports_create_only_success():
    project_root = _workspace_dir("dgce_explain_create_only_success")
    _write_section_output(
        project_root,
        "mission-board",
        {
            "section_id": "mission-board",
            "run_mode": "create_only",
            "run_outcome_class": "success_create_only",
            "file_plan": {"project_name": "DGCE", "files": []},
            "execution_outcome": {
                "section_id": "mission-board",
                "stage": "WRITE",
                "status": "success",
                "validation_summary": {"ok": True, "error": None, "missing_keys": []},
                "change_plan_summary": {"create_count": 3, "modify_count": 0, "ignore_count": 0},
                "execution_summary": {
                    "written_files_count": 3,
                    "skipped_modify_count": 0,
                    "skipped_ignore_count": 0,
                    "skipped_identical_count": 0,
                    "skipped_ownership_count": 0,
                    "skipped_exists_fallback_count": 0,
                },
            },
            "advisory": None,
            "write_transparency": {
                "write_decisions": [],
                "write_summary": {
                    "written_count": 3,
                    "modify_written_count": 0,
                    "diff_visible_count": 0,
                    "skipped_modify_count": 0,
                    "skipped_ignore_count": 0,
                    "skipped_identical_count": 0,
                    "skipped_ownership_count": 0,
                    "skipped_exists_fallback_count": 0,
                    "before_bytes_total": 0,
                    "after_bytes_total": 0,
                    "changed_lines_estimate_total": 0,
                    "bytes_written_total": 96,
                },
            },
        },
    )

    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["explain", str(project_root), "--section", "mission-board"], console=console)
    output = stream.getvalue()

    assert exit_code == 0
    assert "success_create_only" in output
    assert "Safe Modify was disabled for this run." in output
    assert "Run completed with success status." in output
    assert "3 file(s) were written." in output


def test_explain_run_reports_mixed_create_and_modify_writes_truthfully():
    project_root = _workspace_dir("dgce_explain_mixed_writes")
    _write_section_output(
        project_root,
        "mission-board",
        {
            "section_id": "mission-board",
            "run_mode": "safe_modify",
            "run_outcome_class": "success_safe_modify",
            "file_plan": {"project_name": "DGCE", "files": []},
            "execution_outcome": {
                "section_id": "mission-board",
                "stage": "WRITE",
                "status": "success",
                "validation_summary": {"ok": True, "error": None, "missing_keys": []},
                "change_plan_summary": {"create_count": 2, "modify_count": 1, "ignore_count": 0},
                "execution_summary": {
                    "written_files_count": 3,
                    "skipped_modify_count": 0,
                    "skipped_ignore_count": 0,
                    "skipped_identical_count": 0,
                    "skipped_ownership_count": 0,
                    "skipped_exists_fallback_count": 0,
                },
            },
            "advisory": None,
            "write_transparency": {
                "write_decisions": [
                    {"path": "mission_board/service.py", "decision": "written", "reason": "modify", "bytes_written": 40},
                    {"path": "mission_board/models.py", "decision": "written", "reason": "create", "bytes_written": 28},
                    {"path": "api/missionboardservice.py", "decision": "written", "reason": "create", "bytes_written": 32},
                ],
                "write_summary": {
                    "written_count": 3,
                    "modify_written_count": 1,
                    "diff_visible_count": 1,
                    "skipped_modify_count": 0,
                    "skipped_ignore_count": 0,
                    "skipped_identical_count": 0,
                    "skipped_ownership_count": 0,
                    "skipped_exists_fallback_count": 0,
                    "before_bytes_total": 24,
                    "after_bytes_total": 40,
                    "changed_lines_estimate_total": 3,
                    "bytes_written_total": 100,
                },
            },
        },
    )

    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["explain", str(project_root), "--section", "mission-board"], console=console)
    output = stream.getvalue()

    assert exit_code == 0
    assert "Safe Modify was enabled for this run." in output
    assert "3 file(s) were written: 2 create write(s) and 1 Safe Modify write(s)." in output


def test_explain_run_handles_missing_section_output_cleanly():
    project_root = _workspace_dir("dgce_explain_missing_section")
    stream = io.StringIO()
    console = Console(file=stream, force_terminal=False, color_system=None)

    exit_code = main(["explain", str(project_root), "--section", "missing-section"], console=console)

    assert exit_code == 1
    assert "Section output not found" in stream.getvalue()
