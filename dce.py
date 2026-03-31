import argparse
import json
from pathlib import Path
from aether.dgce.decompose import SectionApprovalInput, _collect_orchestrator_artifact_paths, run_dgce_section, record_section_approval


def _next_action_for_status(
    approval_status: str | None,
    stale_status: str | None,
    stale_detected: bool | None,
    preflight_status: str | None,
    gate_status: str | None,
    alignment_status: str | None,
    execution_status: str | None,
    run_outcome_class: str | None,
) -> str:
    """Derive the operator next action from already-persisted orchestrator artifacts."""
    if approval_status is None:
        return "approve section"
    if stale_status == "stale_invalidated" or stale_detected is True:
        return "regenerate preview and review, then re-approve"
    if preflight_status is not None and preflight_status != "preflight_pass":
        return "resolve preflight failure and re-approve"
    if gate_status is not None and gate_status != "gate_pass":
        return "resolve gate failure"
    if alignment_status is not None and alignment_status != "alignment_pass":
        return "resolve alignment mismatch and re-approve"
    if run_outcome_class == "validation_failure":
        return "inspect output validation failure and correct the section input or generator"
    if execution_status in {"execution_completed", "execution_completed_no_changes"}:
        return "nothing pending"
    if approval_status == "approved":
        return "run governed section"
    return "review section state"


def main():
    parser = argparse.ArgumentParser(prog="dce")
    subparsers = parser.add_subparsers(dest="command")

    # run-section
    run_parser = subparsers.add_parser("run-section")
    run_parser.add_argument("section_id")
    run_parser.add_argument("--governed", action="store_true")

    # approve
    approve_parser = subparsers.add_parser("approve")
    approve_parser.add_argument("section_id")
    approve_parser.add_argument("--mode", required=True)

    # status
    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("section_id")

    args = parser.parse_args()

    project_root = Path.cwd() / "defiant-sky"

    if args.command == "run-section":
        result = run_dgce_section(
            section_id=args.section_id,
            project_root=project_root,
            governed=args.governed,
        )
        print(result)

    elif args.command == "approve":
        record_section_approval(
            project_root=project_root,
            section_id=args.section_id,
            approval=SectionApprovalInput(
                approval_status="approved",
                selected_mode=args.mode,
            ),
        )
        print(f"Approved {args.section_id} with mode={args.mode}")

    elif args.command == "status":
        artifact_paths = _collect_orchestrator_artifact_paths(project_root, args.section_id)

        def load_artifact(artifact_path):
            if artifact_path is None:
                return None
            path = Path(project_root) / artifact_path
            if not path.exists():
                return None
            if path.suffix == ".json":
                return json.loads(path.read_text(encoding="utf-8"))
            return path.read_text(encoding="utf-8")

        approval = load_artifact(artifact_paths["approval_path"])
        stale_check = load_artifact(artifact_paths["stale_check_path"])
        preflight = load_artifact(artifact_paths["preflight_path"])
        gate = load_artifact(artifact_paths["execution_gate_path"])
        alignment = load_artifact(artifact_paths["alignment_path"])
        execution = load_artifact(artifact_paths["execution_path"])

        approval_status = approval.get("approval_status") if isinstance(approval, dict) else None
        stale_status = stale_check.get("stale_status") if isinstance(stale_check, dict) else None
        stale_detected = stale_check.get("stale_detected") if isinstance(stale_check, dict) else None
        preflight_status = preflight.get("preflight_status") if isinstance(preflight, dict) else None
        gate_status = gate.get("gate_status") if isinstance(gate, dict) else None
        alignment_status = alignment.get("alignment_status") if isinstance(alignment, dict) else None
        execution_status = execution.get("execution_status") if isinstance(execution, dict) else None
        run_outcome_class = execution.get("run_outcome_class") if isinstance(execution, dict) else None
        next_action = _next_action_for_status(
            approval_status=approval_status,
            stale_status=stale_status,
            stale_detected=stale_detected,
            preflight_status=preflight_status,
            gate_status=gate_status,
            alignment_status=alignment_status,
            execution_status=execution_status,
            run_outcome_class=run_outcome_class,
        )

        print(f"Section: {args.section_id}")
        print("")
        print("Approval:")
        print(f"  status: {approval_status}")
        print(f"  mode: {approval.get('selected_mode') if isinstance(approval, dict) else None}")
        print("")
        print("Stale Check:")
        print(f"  status: {stale_status}")
        print("")
        print("Preflight:")
        print(f"  status: {preflight_status}")
        print("")
        print("Gate:")
        print(f"  status: {gate_status}")
        print("")
        print("Alignment:")
        print(f"  status: {alignment_status}")
        print("")
        print("Execution:")
        print(f"  status: {execution_status}")
        print(f"  outcome: {run_outcome_class}")
        if run_outcome_class == "validation_failure":
            print("  problem: validation_failure")
        print("")
        print("Next Action:")
        print(f"  {next_action}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
