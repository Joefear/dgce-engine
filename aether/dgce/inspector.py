"""Read-only CLI inspector for persisted DGCE workspace artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Optional

from rich.console import Console
from rich.table import Table


def main(argv: Optional[list[str]] = None, console: Optional[Console] = None) -> int:
    """Run the DGCE workspace inspector CLI."""
    parser = argparse.ArgumentParser(prog="dgce", description="Inspect persisted DGCE workspace artifacts.")
    subparsers = parser.add_subparsers(dest="command")

    inspect_parser = subparsers.add_parser("inspect", help="Inspect a .dce workspace")
    inspect_parser.add_argument("project_root", help="Project root containing a .dce workspace")
    inspect_parser.add_argument("--section", dest="section_id", help="Focus on one section_id")

    explain_parser = subparsers.add_parser("explain", help="Explain one persisted section run")
    explain_parser.add_argument("project_root", help="Project root containing a .dce workspace")
    explain_parser.add_argument("--section", dest="section_id", required=True, help="Section output to explain")

    args = parser.parse_args(argv)
    output = console or Console()

    if args.command == "inspect":
        return inspect_workspace(Path(args.project_root), section_id=args.section_id, console=output)
    if args.command == "explain":
        return explain_run(Path(args.project_root), section_id=args.section_id, console=output)

    parser.print_help()
    return 1


def inspect_workspace(project_root: Path, *, section_id: str | None = None, console: Optional[Console] = None) -> int:
    """Inspect a persisted DGCE workspace and print a concise operator summary."""
    output = console or Console()
    workspace_root = project_root / ".dce"
    summary_path = workspace_root / "workspace_summary.json"
    advisory_index_path = workspace_root / "advisory_index.json"
    ownership_index_path = workspace_root / "ownership_index.json"

    if not workspace_root.exists():
        output.print(f"[red]DGCE workspace not found:[/red] {workspace_root}")
        return 1
    if not summary_path.exists():
        output.print(f"[red]Required artifact missing:[/red] {summary_path}")
        return 1

    workspace_summary = _load_json(summary_path)
    advisory_index = _load_json(advisory_index_path) if advisory_index_path.exists() else None
    ownership_index = _load_json(ownership_index_path) if ownership_index_path.exists() else {"files": []}
    sections = list(workspace_summary.get("sections", []))

    output.print(f"[bold]DGCE Workspace Inspector[/bold]")
    output.print(f"Project root: {project_root}")

    if section_id:
        return _print_section_focus(
            workspace_root,
            project_root,
            section_id=section_id,
            sections=sections,
            ownership_index=ownership_index,
            console=output,
        )

    _print_workspace_summary(sections, console=output)
    _print_sections_overview(sections, console=output)
    _print_advisory_summary(advisory_index, console=output)
    _print_ownership_summary(ownership_index, console=output)
    if len(sections) == 1:
        _print_single_section_write_summary(workspace_root, str(sections[0].get("section_id")), console=output)
    return 0


def explain_run(project_root: Path, *, section_id: str, console: Optional[Console] = None) -> int:
    """Explain one persisted DGCE section run from the section output artifact only."""
    output = console or Console()
    workspace_root = project_root / ".dce"
    outputs_path = workspace_root / "outputs" / f"{section_id}.json"

    if not outputs_path.exists():
        output.print(f"[red]Section output not found:[/red] {section_id}")
        return 1

    payload = _load_json(outputs_path)
    execution_outcome = payload.get("execution_outcome", {})
    advisory = payload.get("advisory")

    output.print("[bold]DGCE Explain Run[/bold]")
    output.print(f"Project root: {project_root}")
    output.print(f"Section ID: {payload.get('section_id') or section_id}")
    output.print(f"Run mode: {payload.get('run_mode')}")
    output.print(f"Run outcome class: {payload.get('run_outcome_class')}")
    output.print(f"Status: {execution_outcome.get('status')}")

    output.print("")
    output.print("[bold]Why[/bold]")
    for bullet in _build_explain_bullets(payload):
        output.print(f"- {bullet}")

    if isinstance(advisory, dict):
        output.print("")
        output.print("[bold]Advisory Summary[/bold]")
        output.print(f"Type: {advisory.get('type')}")
        output.print(f"Summary: {advisory.get('summary')}")

    output.print("")
    output.print("[bold]Key Write Decisions[/bold]")
    output.print(_build_write_decision_summary(payload))
    return 0


def _print_workspace_summary(sections: list[dict[str, Any]], *, console: Console) -> None:
    console.print("")
    console.print("[bold]Workspace Summary[/bold]")
    console.print(f"Total sections: {len(sections)}")


def _print_sections_overview(sections: list[dict[str, Any]], *, console: Console) -> None:
    console.print("")
    console.print("[bold]Per-Section Overview[/bold]")
    table = Table(show_header=True, header_style="bold")
    table.add_column("section_id")
    table.add_column("status")
    table.add_column("run_mode")
    table.add_column("run_outcome_class")

    for section in _sorted_sections(sections):
        table.add_row(
            str(section.get("section_id") or ""),
            str(section.get("latest_status") or ""),
            str(section.get("latest_run_mode") or ""),
            str(section.get("latest_run_outcome_class") or ""),
        )
    console.print(table)


def _print_advisory_summary(advisory_index: dict[str, Any] | None, *, console: Console) -> None:
    console.print("")
    console.print("[bold]Advisory Summary[/bold]")
    if not advisory_index:
        console.print("No advisory index found.")
        return
    console.print(f"Type: {advisory_index.get('advisory_type')}")
    explanation = advisory_index.get("advisory_explanation")
    console.print(f"Explanation: {', '.join(explanation) if isinstance(explanation, list) and explanation else 'n/a'}")


def _print_ownership_summary(ownership_index: dict[str, Any], *, console: Console) -> None:
    console.print("")
    console.print("[bold]Ownership Summary[/bold]")
    files = ownership_index.get("files", [])
    console.print(f"Owned files: {len(files)}")


def _print_single_section_write_summary(workspace_root: Path, section_id: str, *, console: Console) -> None:
    outputs_path = workspace_root / "outputs" / f"{section_id}.json"
    if not outputs_path.exists():
        console.print("")
        console.print("[bold]Write Summary[/bold]")
        console.print(f"Section output not found: {section_id}")
        return

    payload = _load_json(outputs_path)
    write_summary = payload.get("write_transparency", {}).get("write_summary", {})
    console.print("")
    console.print("[bold]Write Summary[/bold]")
    console.print(f"Written count: {write_summary.get('written_count', 0)}")
    console.print(f"Skipped modify: {write_summary.get('skipped_modify_count', 0)}")
    console.print(f"Skipped ignore: {write_summary.get('skipped_ignore_count', 0)}")
    console.print(f"Bytes written total: {write_summary.get('bytes_written_total', 0)}")


def _print_section_focus(
    workspace_root: Path,
    project_root: Path,
    *,
    section_id: str,
    sections: list[dict[str, Any]],
    ownership_index: dict[str, Any],
    console: Console,
) -> int:
    outputs_path = workspace_root / "outputs" / f"{section_id}.json"
    if not outputs_path.exists():
        console.print(f"[red]Section output not found:[/red] {section_id}")
        return 1

    payload = _load_json(outputs_path)
    section_summary = next((entry for entry in sections if entry.get("section_id") == section_id), None)

    console.print("")
    console.print("[bold]Section Detail[/bold]")
    console.print(f"Section: {section_id}")
    console.print(f"Project root: {project_root}")
    console.print(f"Run mode: {payload.get('run_mode')}")
    console.print(f"Run outcome class: {payload.get('run_outcome_class')}")
    console.print(f"Status: {payload.get('execution_outcome', {}).get('status')}")
    if section_summary:
        console.print(
            "Advisory: "
            f"{section_summary.get('latest_advisory_type') or 'n/a'}"
        )

    _print_single_section_write_summary(workspace_root, section_id, console=console)

    owned_files = [
        entry for entry in ownership_index.get("files", []) if entry.get("section_id") == section_id
    ]
    console.print("")
    console.print("[bold]Ownership Summary[/bold]")
    console.print(f"Owned files: {len(owned_files)}")
    return 0


def _sorted_sections(sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return sections sorted with non-success first, then deterministically by section_id."""
    return sorted(
        sections,
        key=lambda entry: (
            0 if str(entry.get("latest_status")) != "success" else 1,
            str(entry.get("section_id") or ""),
        ),
    )


def _load_json(path: Path) -> dict[str, Any]:
    """Load one persisted DGCE JSON artifact."""
    return json.loads(path.read_text(encoding="utf-8"))


def _build_explain_bullets(payload: dict[str, Any]) -> list[str]:
    """Build a deterministic short explanation from persisted section facts."""
    execution_outcome = payload.get("execution_outcome", {})
    validation = execution_outcome.get("validation_summary", {})
    execution = execution_outcome.get("execution_summary", {})
    advisory = payload.get("advisory")
    bullets: list[str] = []

    missing_keys = [str(key) for key in validation.get("missing_keys", [])]
    if validation.get("ok") is False:
        bullet = "Validation failed"
        if missing_keys:
            bullet += f"; missing keys: {', '.join(missing_keys)}"
        elif validation.get("error"):
            bullet += f"; error: {validation.get('error')}"
        bullets.append(bullet + ".")

    if execution_outcome.get("status") == "error" and validation.get("ok") is not False:
        bullets.append("Execution finished with an error status.")

    run_mode = str(payload.get("run_mode") or "")
    if run_mode == "safe_modify":
        bullets.append("Safe Modify was enabled for this run.")
    elif run_mode:
        bullets.append("Safe Modify was disabled for this run.")

    status = execution_outcome.get("status")
    if status and status != "error":
        bullets.append(f"Run completed with {status} status.")

    if int(execution.get("skipped_ownership_count", 0)) > 0:
        bullets.append(
            f"{int(execution.get('skipped_ownership_count', 0))} modify path(s) were blocked because they were not owned."
        )
    if int(execution.get("skipped_identical_count", 0)) > 0:
        bullets.append(
            f"{int(execution.get('skipped_identical_count', 0))} modify path(s) were skipped because the generated content was identical."
        )
    if int(execution.get("skipped_modify_count", 0)) > 0:
        bullets.append(
            f"{int(execution.get('skipped_modify_count', 0))} modify path(s) were skipped by controlled-write policy."
        )
    if int(execution.get("skipped_ignore_count", 0)) > 0:
        bullets.append(
            f"{int(execution.get('skipped_ignore_count', 0))} ignored path(s) were skipped."
        )
    if int(execution.get("skipped_exists_fallback_count", 0)) > 0:
        bullets.append(
            f"{int(execution.get('skipped_exists_fallback_count', 0))} existing unplanned path(s) were protected by exists_fallback."
        )
    write_bullet = _build_written_files_bullet(payload)
    if write_bullet:
        bullets.append(write_bullet)
    if isinstance(advisory, dict):
        explanation = advisory.get("explanation")
        if isinstance(explanation, list) and explanation:
            bullets.append(
                f"Advisory: {advisory.get('type')} ({', '.join(str(item) for item in explanation)})."
            )
        else:
            bullets.append(f"Advisory: {advisory.get('type')}.")

    return bullets[:6]


def _build_write_decision_summary(payload: dict[str, Any]) -> str:
    """Return a short deterministic summary of persisted write decision counts."""
    write_summary = payload.get("write_transparency", {}).get("write_summary", {})
    return (
        "written={written}, skipped_modify={skipped_modify}, skipped_ignore={skipped_ignore}, "
        "skipped_identical={skipped_identical}, skipped_ownership={skipped_ownership}, "
        "skipped_exists_fallback={skipped_exists_fallback}"
    ).format(
        written=int(write_summary.get("written_count", 0)),
        skipped_modify=int(write_summary.get("skipped_modify_count", 0)),
        skipped_ignore=int(write_summary.get("skipped_ignore_count", 0)),
        skipped_identical=int(write_summary.get("skipped_identical_count", 0)),
        skipped_ownership=int(write_summary.get("skipped_ownership_count", 0)),
        skipped_exists_fallback=int(write_summary.get("skipped_exists_fallback_count", 0)),
    )


def _build_written_files_bullet(payload: dict[str, Any]) -> str | None:
    """Return a truthful written-files explanation derived from persisted write decisions."""
    write_transparency = payload.get("write_transparency", {})
    write_decisions = write_transparency.get("write_decisions", [])
    written_entries = [
        entry for entry in write_decisions if entry.get("decision") == "written"
    ]
    execution = payload.get("execution_outcome", {}).get("execution_summary", {})
    written_files_count = int(execution.get("written_files_count", 0))

    if written_files_count <= 0:
        return None

    if not written_entries or len(written_entries) != written_files_count:
        return f"{written_files_count} file(s) were written."

    written_create_count = sum(1 for entry in written_entries if entry.get("reason") == "create")
    written_modify_count = sum(1 for entry in written_entries if entry.get("reason") == "modify")

    if written_create_count == written_files_count:
        return f"{written_files_count} file(s) were written because create paths were new."
    if written_modify_count == written_files_count:
        return f"{written_files_count} file(s) were written through Safe Modify."
    if written_create_count > 0 and written_modify_count > 0:
        return (
            f"{written_files_count} file(s) were written: {written_create_count} create write(s) "
            f"and {written_modify_count} Safe Modify write(s)."
        )
    return f"{written_files_count} file(s) were written."
