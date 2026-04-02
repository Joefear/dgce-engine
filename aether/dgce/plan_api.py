"""Read-only DGCE bundle planning helpers."""

from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import Any

from aether.dgce.decompose import _load_section_from_workspace_input
from aether.dgce.path_utils import resolve_workspace_path


def _validate_bundle_plan_input(section_ids: object) -> list[str]:
    if not isinstance(section_ids, list):
        raise ValueError("Bundle section_ids must be a list")
    if not section_ids:
        raise ValueError("Bundle requires at least one section_id")

    normalized: list[str] = []
    seen: set[str] = set()
    for entry in section_ids:
        if not isinstance(entry, str):
            raise ValueError("Bundle section_ids must be non-empty strings")
        section_id = entry.strip()
        if not section_id:
            raise ValueError("Bundle section_ids must be non-empty strings")
        if section_id in seen:
            raise ValueError("Bundle section_ids must be unique")
        seen.add(section_id)
        normalized.append(section_id)
    return normalized


def _load_section_dependencies(project_root: Path, section_id: str) -> list[str]:
    section = _load_section_from_workspace_input(project_root, section_id)
    dependencies = getattr(section, "dependencies", [])
    if not isinstance(dependencies, list):
        raise ValueError(f"Section dependencies must be a list: {section_id}")

    normalized: list[str] = []
    seen: set[str] = set()
    for entry in dependencies:
        if not isinstance(entry, str):
            raise ValueError(f"Section dependencies must contain only strings: {section_id}")
        dependency = entry.strip()
        if not dependency:
            raise ValueError(f"Section dependencies must contain only non-empty strings: {section_id}")
        if dependency in seen:
            continue
        seen.add(dependency)
        normalized.append(dependency)
    return normalized


def _topological_order(
    section_ids: list[str],
    dependencies_by_section: dict[str, list[str]],
) -> tuple[list[str], list[dict[str, str]], set[str]]:
    position = {section_id: index for index, section_id in enumerate(section_ids)}
    adjacency: dict[str, list[str]] = {section_id: [] for section_id in section_ids}
    indegree: dict[str, int] = {section_id: 0 for section_id in section_ids}
    edges: list[dict[str, str]] = []
    seen_edges: set[tuple[str, str]] = set()
    missing_dependencies: set[str] = set()

    for section_id in section_ids:
        for dependency in dependencies_by_section[section_id]:
            if dependency not in position:
                missing_dependencies.add(dependency)
                continue
            edge = (dependency, section_id)
            if edge in seen_edges:
                continue
            seen_edges.add(edge)
            adjacency[dependency].append(section_id)
            indegree[section_id] += 1
            edges.append({"from": dependency, "to": section_id})

    ready = deque(sorted((section_id for section_id in section_ids if indegree[section_id] == 0), key=position.__getitem__))
    ordered: list[str] = []
    while ready:
        current = ready.popleft()
        ordered.append(current)
        for neighbor in sorted(adjacency[current], key=position.__getitem__):
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                ready.append(neighbor)

    cyclic_nodes = {section_id for section_id in section_ids if indegree[section_id] > 0}
    edges.sort(key=lambda entry: (entry["from"], entry["to"]))
    return ordered, edges, cyclic_nodes


def _canonicalize_cycle(cycle: list[str], position: dict[str, int]) -> tuple[str, ...]:
    if not cycle:
        return tuple()
    rotations = [tuple(cycle[index:] + cycle[:index]) for index in range(len(cycle))]
    return min(rotations, key=lambda rotated: tuple(position[item] for item in rotated))


def _find_cycles(
    section_ids: list[str],
    dependencies_by_section: dict[str, list[str]],
    cyclic_nodes: set[str],
) -> list[list[str]]:
    if not cyclic_nodes:
        return []

    position = {section_id: index for index, section_id in enumerate(section_ids)}
    cycles: set[tuple[str, ...]] = set()

    def visit(node: str, stack: list[str], stack_set: set[str]) -> None:
        stack.append(node)
        stack_set.add(node)
        for dependency in dependencies_by_section[node]:
            if dependency not in cyclic_nodes:
                continue
            if dependency in stack_set:
                start = stack.index(dependency)
                cycles.add(_canonicalize_cycle(stack[start:], position))
                continue
            visit(dependency, stack, stack_set)
        stack.pop()
        stack_set.remove(node)

    for section_id in sorted(cyclic_nodes, key=position.__getitem__):
        visit(section_id, [], set())

    return [list(cycle) for cycle in sorted(cycles, key=lambda cycle: tuple(position[item] for item in cycle))]


def plan_section_bundle(
    workspace_path: str | Path,
    section_ids: object,
) -> tuple[dict[str, Any], int]:
    project_root = resolve_workspace_path(workspace_path)
    normalized_section_ids = _validate_bundle_plan_input(section_ids)
    dependencies_by_section = {
        section_id: _load_section_dependencies(project_root, section_id)
        for section_id in normalized_section_ids
    }
    ordered_section_ids, dependency_edges, cyclic_nodes = _topological_order(normalized_section_ids, dependencies_by_section)
    cycles_detected = _find_cycles(normalized_section_ids, dependencies_by_section, cyclic_nodes)

    missing_dependencies: list[str] = []
    seen_missing: set[str] = set()
    for section_id in normalized_section_ids:
        for dependency in dependencies_by_section[section_id]:
            if dependency in normalized_section_ids or dependency in seen_missing:
                continue
            seen_missing.add(dependency)
            missing_dependencies.append(dependency)

    plan_valid = not missing_dependencies and not cycles_detected
    result: dict[str, Any] = {
        "status": "ok" if plan_valid else "invalid",
        "plan_valid": plan_valid,
        "ordered_section_ids": ordered_section_ids if not cycles_detected else [],
        "input_section_ids": list(normalized_section_ids),
        "dependency_edges": dependency_edges,
        "cycles_detected": cycles_detected,
        "missing_dependencies": missing_dependencies,
    }
    if not plan_valid:
        if missing_dependencies and cycles_detected:
            result["detail"] = "Bundle plan contains missing dependencies and dependency cycles"
        elif missing_dependencies:
            result["detail"] = "Bundle plan contains missing dependencies"
        else:
            result["detail"] = "Bundle plan contains dependency cycles"
    return result, 200
