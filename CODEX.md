# DGCE

DGCE is the governed generation layer inside DCE for deterministic creation tasks routed through Aether under Guardrail authority.

## Locked Roles

- Guardrail = policy authority
- Aether = execution control
- Itera = advisory only
- DGCE = generation only

## Hard Rules

- Do not modify lifecycle stages or their order.
- Do not write files outside Execution-stage controlled write flow.
- Do not bypass Guardrail evaluation or decisions.
- Do not expand section scope during execution.
- Do not generate multiple files unless explicitly approved.

## Phase 4 Constraint

- single-function
- single-file
- validated output only

## Implementation Rules

- Make minimal changes only.
- Reuse the existing pipeline.
- Do not perform direct file writes from generation code.
- Always validate before write.
- Keep model execution separate from policy, lifecycle, and artifact persistence.
- Use versioned prompt templates only. Do not introduce silent prompt variants.
- Keep execution metadata audit-safe and bounded to fixed execution configuration.
