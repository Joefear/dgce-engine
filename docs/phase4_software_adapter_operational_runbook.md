# Phase 4 Software Adapter Operational Runbook

This runbook explains how to operate and triage the Phase 4 DGCE Software Generation Adapter locally. It documents current behavior only. It does not change runtime behavior, reopen GCE Stage 0, redesign Stage 7.5, add simulation engines, modify Code Graph or `dcg.facts.v1`, modify Guardrail, add external adapter families, or start Game Adapter Stage 2 work.

For the lifecycle/artifact contract, see `docs/dgce_stage0_to_stage9_lifecycle_contract.md`.

## Local Startup

The local FastAPI app is exposed as `apps.aether_api.main:app`. Tests use `apps.aether_api.main.create_app()` with `fastapi.testclient.TestClient`.

Typical local startup:

```powershell
python -m uvicorn apps.aether_api.main:app --reload
```

If `DGCE_API_KEY` is set, read endpoints under `/v1/dgce/...` require `X-API-Key`. Tests commonly clear `DGCE_API_KEY`, so local unauthenticated development is possible when that variable is unset. DGCE routes are rate-limited per client in the local app middleware.

Workspaces are ordinary project directories containing a `.dce/` control-plane folder. API payloads use:

```json
{"workspace_path": "path/to/workspace"}
```

## Lifecycle Sequence

### 1. Prepare

Endpoint:

```text
POST /v1/dgce/sections/{section_id}/prepare
```

Use prepare after preview/review/approval artifacts exist. Prepare validates section existence, artifact linkage, approval readiness, preflight readiness, Stage 6 gate readiness, and writes `.dce/plans/{section_id}.prepared_plan.json` when all checks pass.

Successful eligible response:

```json
{
  "status": "ok",
  "section_id": "mission-board",
  "eligible": true,
  "checks": {
    "section_exists": true,
    "artifacts_valid": true,
    "approval_ready": true,
    "preflight_ready": true,
    "gate_ready": true
  }
}
```

### 2. Review / Read

Read the operator surfaces before approval or execution:

```text
GET /v1/dgce/workspace-index?workspace_path=...
GET /v1/dgce/lifecycle-trace?workspace_path=...
GET /v1/dgce/dashboard?workspace_path=...
GET /v1/dgce/artifact-manifest?workspace_path=...
GET /v1/dgce/sections/{section_id}/summary?workspace_path=...
GET /v1/dgce/sections/{section_id}/overview?workspace_path=...
GET /v1/dgce/sections/{section_id}/dashboard?workspace_path=...
```

The Python SDK is read-only and wraps the read endpoints through `DGCEClient`.

### 3. Approve

Endpoint:

```text
POST /v1/dgce/sections/{section_id}/approve
```

Payload:

```json
{
  "workspace_path": "path/to/workspace",
  "approved_by": "operator",
  "notes": "approved for local run",
  "selected_mode": "create_only"
}
```

`selected_mode` may be omitted; the API uses the preview recommendation. Current allowed modes are `review_required`, `safe_modify`, `create_only`, and `no_changes`. Approval writes `.dce/approvals/{section_id}.approval.json` and refreshes preflight/gate artifacts.

### 4. Preflight / Gate

Preflight and gate are refreshed as part of approval and prepare flows. Operators normally inspect, rather than call, these artifacts directly.

Important files:

```text
.dce/preflight/{section_id}.preflight.json
.dce/preflight/{section_id}.stale_check.json
.dce/execution/gate/{section_id}.gate_input.json
.dce/execution/gate/{section_id}.execution_gate.json
```

The canonical Stage 6 execution gate path is:

```text
.dce/execution/gate/{section_id}.execution_gate.json
```

Stage 6 gate artifacts do not live under `.dce/preflight/`.

### 5. Execute

Endpoint:

```text
POST /v1/dgce/sections/{section_id}/execute
```

Payload:

```json
{"workspace_path": "path/to/workspace", "rerun": false}
```

Execution re-runs prepare validation with `persist_prepared_plan=false`, verifies prepared-plan binding and approval lineage, enforces rerun rules, then performs controlled writes. If prior execution artifacts exist, execution requires `rerun: true`; rerun still fails closed if safe-modify/ownership validation fails.

Bundle operations:

```text
POST /v1/dgce/sections/plan-bundle
POST /v1/dgce/sections/execute-bundle
GET /v1/dgce/bundles/{bundle_fingerprint}
GET /v1/dgce/bundles/{bundle_fingerprint}/verify
GET /v1/dgce/bundles/{bundle_fingerprint}/summary
GET /v1/dgce/bundles/{bundle_fingerprint}/overview
GET /v1/dgce/bundles/{bundle_fingerprint}/dashboard
```

### 6. Inspect Output

After execution, inspect:

```text
.dce/execution/{section_id}.execution.json
.dce/outputs/{section_id}.json
.dce/workspace_summary.json
.dce/lifecycle_trace.json
.dce/artifact_manifest.json
```

Useful endpoints:

```text
GET /v1/dgce/sections/{section_id}/provenance?workspace_path=...
GET /v1/dgce/sections/{section_id}/verify?workspace_path=...
GET /v1/dgce/sections/{section_id}/summary?workspace_path=...
GET /v1/dgce/sections/{section_id}/overview?workspace_path=...
GET /v1/dgce/sections/{section_id}/dashboard?workspace_path=...
```

## Key Surfaces

| Surface | Purpose |
| --- | --- |
| `/v1/dgce/sections/{section_id}/prepare` | Eligibility check and prepared-plan sealing. |
| `/v1/dgce/sections/{section_id}/approve` | Operator approval and selected execution mode. |
| `/v1/dgce/sections/{section_id}/execute` | Controlled single-section execution. |
| `/v1/dgce/refresh` | Refresh derived workspace artifacts without executing. |
| `/v1/dgce/workspace-index` | Workspace-level section index and artifact links. |
| `/v1/dgce/lifecycle-trace` | Ordered lifecycle stage visibility. |
| `/v1/dgce/dashboard` | Operator dashboard projection. |
| `/v1/dgce/artifact-manifest` | Available artifact inventory. |
| `/v1/dgce/consumer-contract` | Supported read-model fields for consumers. |
| `/v1/dgce/export-contract` | Export-facing version of the consumer contract. |
| `/v1/dgce/sections/{section_id}/provenance` | Approval, prepared-plan, execution, and rerun provenance. |
| `/v1/dgce/sections/{section_id}/verify` | Artifact-chain verification report. |
| `DGCEClient` | Read-only Python SDK for dashboard, workspace index, lifecycle trace, contracts, manifest, and GCE Stage 0 read models. |

## Interpreting Blocking States

### Eligible / Ineligible

`eligible: true` means all prepare checks are true and the prepared plan can be sealed. `eligible: false` means execution must not proceed. Check the `checks` object first:

- `section_exists: false`: section id is not present in workspace artifacts.
- `artifacts_valid: false`: manifest/index linkage is broken or required artifacts do not validate.
- `approval_ready: false`: approval is missing, rejected, non-executable, or fingerprint-invalid.
- `preflight_ready: false`: preflight is missing, stale, failed, or fingerprint-invalid.
- `gate_ready: false`: Stage 6 gate decision, gate input, stale-check, or persisted gate artifact is invalid.

### Gate Ready

`gate_ready: true` means persisted stale-check, gate-input, and execution-gate artifacts match recomputed facts and the Stage 6 decision permits downstream execution.

`gate_ready: false` is fail-closed. Inspect:

```text
.dce/preflight/{section_id}.stale_check.json
.dce/execution/gate/{section_id}.gate_input.json
.dce/execution/gate/{section_id}.execution_gate.json
```

### Guardrail Decision

`guardrail_decision` is exposed in the Stage 6 gate and read models.

- `ALLOW`: permits downstream only when the rest of the gate contract is valid.
- `BLOCK`: prevents execution.
- `MODIFY`: unsupported by current DGCE Stage 6 consumption and fails closed.
- Missing, malformed, unknown, or unsupported values fail closed.

### Stale Approval

Stale approval means the approval no longer matches current input, preview, or review fingerprints. Inspect `.dce/preflight/{section_id}.stale_check.json` for `stale_status`, `stale_detected`, and `stale_reason`. Safe next step is to review the changed preview/review and approve again if still intended.

### Alignment Drift

Alignment drift means the current write plan no longer matches approved scope or safe-modify/ownership constraints. Inspect `.dce/execution/alignment/{section_id}.alignment.json` for `alignment_status`, `alignment_blocked`, `alignment_reason`, and drift findings. Safe next step is to regenerate/review/approve the current plan rather than forcing execution.

### Stage 7.5

Stage 7.5 is locked and reserved between alignment and execution.

- `pass`: allows Stage 8.
- `fail`: blocks.
- `indeterminate`: blocks.
- `skipped` or not triggered: recorded through simulation projection, including `simulation_stage_applicable`.

Inspect:

```text
.dce/execution/simulation/{section_id}.simulation_trigger.json
.dce/execution/simulation/{section_id}.simulation.json
```

Simulation engines remain reserved; do not add or expect new engine behavior in Phase 4.

### Execution / Write-Scope Failures

Execution refuses when prepare is ineligible, the prepared-plan binding drifts, approval lineage drifts, prior execution exists without `rerun: true`, rerun validation is unsafe, Stage 6 is invalid, Stage 7 blocks, or Stage 7.5 blocks. Writes are limited by file plan, write transparency, selected mode, and ownership rules. `.dce/` is never a valid scaffold write target.

Inspect:

```text
.dce/plans/{section_id}.prepared_plan.json
.dce/execution/{section_id}.execution.json
.dce/outputs/{section_id}.json
.dce/ownership_index.json
```

## Canonical Artifact Paths

Operator shorthand and canonical paths:

| Shorthand | Canonical path |
| --- | --- |
| `.dce/input/` | `.dce/input/{section_id}.json`; GCE Stage 0 uses `.dce/input/gce/*.stage0.json`. |
| `.dce/preview/` | Preview artifacts live under `.dce/plans/{section_id}.preview.json`. |
| `.dce/review/` | Review artifacts live under `.dce/reviews/{section_id}.review.md` and `.dce/reviews/index.json`. |
| `.dce/approval/` | Approval artifacts live under `.dce/approvals/{section_id}.approval.json`. |
| `.dce/preflight/` | `.dce/preflight/{section_id}.preflight.json` and `.dce/preflight/{section_id}.stale_check.json`. |
| `.dce/execution/gate/` | `.dce/execution/gate/{section_id}.gate_input.json` and `.dce/execution/gate/{section_id}.execution_gate.json`. |
| `.dce/execution/alignment/` | `.dce/execution/alignment/{section_id}.alignment.json`. |
| `.dce/execution/simulation/` | `.dce/execution/simulation/{section_id}.simulation_trigger.json` and `.dce/execution/simulation/{section_id}.simulation.json`. |
| `.dce/execution/` | `.dce/execution/{section_id}.execution.json`, bundle manifests, and archived rerun execution artifacts. |
| `.dce/output/` | Output artifacts live under `.dce/outputs/{section_id}.json`. |

Read-model files:

```text
.dce/workspace_index.json
.dce/workspace_summary.json
.dce/lifecycle_trace.json
.dce/dashboard.json
.dce/artifact_manifest.json
.dce/consumer_contract.json
.dce/export_contract.json
.dce/consumer_contract_reference.md
.dce/export_contract_reference.md
```

## Troubleshooting

| Symptom | Likely cause | Inspect | Safe next step |
| --- | --- | --- | --- |
| Prepare returns `eligible: false` and `section_exists: false` | Wrong `section_id` or workspace path | `/v1/dgce/workspace-index`, `.dce/input/` | Use a section id from workspace index. |
| Prepare returns `approval_ready: false` | Missing/rejected/tampered approval or non-executable selected mode | `.dce/approvals/{section_id}.approval.json`, `/sections/{section_id}/summary` | Review current preview/review and approve again. |
| Prepare returns `preflight_ready: false` | Preflight missing, failed, or fingerprint-invalid | `.dce/preflight/{section_id}.preflight.json` | Refresh/approve, then prepare again. |
| Prepare returns `gate_ready: false` | Stale approval, invalid gate input fingerprint, BLOCK/MODIFY/unknown decision, or gate drift | `.dce/preflight/{section_id}.stale_check.json`, `.dce/execution/gate/*.json` | Fix stale artifacts by re-reviewing/re-approving; do not edit gate by hand. |
| `guardrail_decision: BLOCK` | Stage 6 failed closed | `.dce/execution/gate/{section_id}.execution_gate.json` | Read `reasons` and address stale/preflight/permission issue. |
| `guardrail_decision: MODIFY` | Unsupported Stage 6 consumption state | Gate artifact and prepare response | Treat as blocked; Phase 4 does not consume MODIFY. |
| Stale approval | Input/preview/review fingerprint changed after approval | `.dce/preflight/{section_id}.stale_check.json`, approval artifact | Re-run review and approve current artifacts. |
| Alignment blocked | Write plan drift, ownership miss, or safe-modify conflict | `.dce/execution/alignment/{section_id}.alignment.json`, `.dce/ownership_index.json` | Regenerate/review/approve current write plan. |
| Stage 7.5 fail or indeterminate | Simulation/provider evidence blocks or cannot prove safe state | `.dce/execution/simulation/*.json`, section summary simulation projection | Treat as blocked; do not bypass reserved seam. |
| Execute returns prior execution / rerun required | Existing execution/output artifacts | `.dce/execution/{section_id}.execution.json`, `.dce/outputs/{section_id}.json` | Use `rerun: true` only after confirming rerun is intended. |
| Rerun fails safe-modify validation | Existing files require ownership/safe-modify permission | `.dce/ownership_index.json`, execution error detail | Re-approve safe modify only when ownership and scope are correct. |
| Execute returns binding or lineage mismatch | Prepared plan no longer matches approval/current artifacts | `.dce/plans/{section_id}.prepared_plan.json`, `/sections/{section_id}/provenance` | Prepare again from current artifacts; approve again if lineage changed. |
| Read API returns 404 | Artifact not present yet | Artifact manifest or workspace path | Run/refresh lifecycle to produce requested surface. |
| Read API returns 400 | Persisted artifact failed schema/integrity validation | `/v1/dgce/artifact-manifest`, target JSON file | Inspect malformed artifact; regenerate through supported lifecycle path. |

## Boundaries

- GCE Stage 0 is complete and locked. Do not reopen it in Phase 4 operational work.
- Stage 7.5 is locked. Do not redesign the seam or lifecycle order.
- Simulation engines remain reserved until a later phase; Phase 4 records and consumes the reserved seam only.
- Code Graph is enrichment/fallback context only. Do not modify Code Graph or `dcg.facts.v1`.
- Guardrail remains authoritative; DGCE consumes the sealed Stage 6 decision contract and does not modify the Guardrail repo.
- Game Adapter Stage 2 is deferred. Do not start Game Adapter planning or add adapter families here.
- The current SDK is read-only. Lifecycle operations are HTTP/API helper surfaces, not SDK write methods.

## Production-Stable Readiness Checklist

- Operator can start the local API and identify the workspace path.
- Workspace index, lifecycle trace, dashboard, artifact manifest, consumer contract, and export contract read successfully.
- Current preview and review exist and match approval fingerprints.
- Approval is explicit and selected mode is understood.
- Prepare returns `eligible: true` with all checks true before execution.
- Stage 6 gate is at `.dce/execution/gate/{section_id}.execution_gate.json`, has `guardrail_decision: ALLOW`, and `gate_ready: true`.
- Stale-check reports current approval links.
- Alignment is not blocked.
- Stage 7.5 is pass or skipped/not-triggered according to the locked contract; fail and indeterminate are treated as blockers.
- Execute writes only approved paths and never `.dce/`.
- Execution/provenance/verify/read-model surfaces converge after execution.
- Any rerun has explicit `rerun: true` and passes safe-modify/ownership validation.
