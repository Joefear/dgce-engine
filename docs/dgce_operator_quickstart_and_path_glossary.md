# DGCE Operator Quickstart And Path Glossary

DGCE Phase 4 is the production-stable Software Generation Adapter lifecycle for controlled code generation. It turns a structured software section into deterministic preview/review artifacts, requires explicit approval, seals preflight and Guardrail gate state, checks alignment and the locked Stage 7.5 simulation seam, performs controlled writes, and publishes output/read-model artifacts for operators, SDK consumers, and audits.

For fuller contracts, see `docs/dgce_stage0_to_stage9_lifecycle_contract.md`, `docs/phase4_software_adapter_operational_runbook.md`, and `docs/dgce_sdk_lifecycle_scope.md`.

## Local Startup

The local FastAPI app is exposed as `apps.aether_api.main:app`.

```powershell
python -m uvicorn apps.aether_api.main:app --reload
```

If `DGCE_API_KEY` is set, read endpoints under `/v1/dgce/...` require the `X-API-Key` header. Workspaces are ordinary project directories with a `.dce/` control-plane folder.

## Common Test Commands

End-to-end Stage 0 through Stage 9 smoke:

```powershell
python -m pytest tests/test_dgce_end_to_end_smoke.py -q
```

Stage 7.5 lifecycle-order lock:

```powershell
python -m pytest tests/test_stage75_contract_lock.py::test_stage75_contract_lock_lifecycle_order_is_unchanged -q
```

Prepare/read/execute API smoke matrix:

```powershell
python -m pytest tests/test_dgce_prepare_api.py tests/test_dgce_read_api.py tests/test_dgce_read_api_http.py tests/test_dgce_execute_api.py -q
```

## Artifact Path Glossary

| Shorthand | Canonical path | What it means |
| --- | --- | --- |
| `.dce/input/` | `.dce/input/{section_id}.json` | Stage 1 section input. GCE Stage 0 packages live separately under `.dce/input/gce/*.stage0.json`. |
| `.dce/preview/` | `.dce/plans/{section_id}.preview.json` | Stage 2 deterministic preview/change intent. |
| `.dce/review/` | `.dce/reviews/{section_id}.review.md` and `.dce/reviews/index.json` | Stage 3 operator review bundle and review index. |
| `.dce/approval/` | `.dce/approvals/{section_id}.approval.json` | Stage 4 explicit approval and selected execution mode. |
| `.dce/preflight/` | `.dce/preflight/{section_id}.preflight.json` and `.dce/preflight/{section_id}.stale_check.json` | Stage 5 preflight and stale approval checks. |
| `.dce/execution/gate/` | `.dce/execution/gate/{section_id}.gate_input.json` and `.dce/execution/gate/{section_id}.execution_gate.json` | Stage 6 Guardrail decision-consumption artifacts. The execution gate path is canonical. |
| `.dce/execution/alignment/` | `.dce/execution/alignment/{section_id}.alignment.json` | Stage 7 approved-scope/write-plan drift detection. |
| `.dce/execution/simulation/` | `.dce/execution/simulation/{section_id}.simulation_trigger.json` and `.dce/execution/simulation/{section_id}.simulation.json` | Stage 7.5 locked reserved simulation seam artifacts. Simulation may be skipped/not triggered. |
| `.dce/execution/` | `.dce/execution/{section_id}.execution.json` | Stage 8 execution record, plus archives/bundle manifests where applicable. |
| `.dce/output/` | `.dce/outputs/{section_id}.json` | Stage 9 output record. The actual directory name is `.dce/outputs/`. |

Useful read-model files:

```text
.dce/workspace_index.json
.dce/workspace_summary.json
.dce/lifecycle_trace.json
.dce/dashboard.json
.dce/artifact_manifest.json
.dce/consumer_contract.json
.dce/export_contract.json
```

## Quick Troubleshooting

| Symptom | First place to inspect | Safe next step |
| --- | --- | --- |
| `gate_ready: false` | `.dce/preflight/{section_id}.stale_check.json`, `.dce/execution/gate/{section_id}.gate_input.json`, `.dce/execution/gate/{section_id}.execution_gate.json` | Treat as fail-closed. Re-review/re-approve current artifacts; do not hand-edit the gate. |
| Stale approval | `.dce/preflight/{section_id}.stale_check.json` and `.dce/approvals/{section_id}.approval.json` | Regenerate/review the current preview/review and approve again if still intended. |
| Alignment drift | `.dce/execution/alignment/{section_id}.alignment.json` and `.dce/ownership_index.json` | Regenerate/review/approve the current write plan; do not force execution past drift. |
| Stage 7.5 indeterminate | `.dce/execution/simulation/{section_id}.simulation_trigger.json`, `.dce/execution/simulation/{section_id}.simulation.json`, and section summary simulation projection | Treat as blocked. Stage 7.5 indeterminate does not permit Stage 8. |
| Execution blocked by write scope | `.dce/plans/{section_id}.prepared_plan.json`, `.dce/execution/{section_id}.execution.json`, `.dce/ownership_index.json` | Confirm selected mode, approved targets, ownership, and safe-modify status; approve the correct current scope before rerun. |

## Do Not Touch

- GCE Stage 0 is complete and locked. Do not reopen or redesign it.
- Stage 7.5 is locked. Do not change lifecycle order or seam semantics.
- Phase 5 simulation engines remain reserved. Do not implement engines in Phase 4 polish work.
- Game Adapter Stage 2 is deferred until intentionally opened by status and scope.
- Code Graph is enrichment/fallback context only. Do not modify Code Graph or `dcg.facts.v1`.
- Guardrail remains authoritative. DGCE consumes the sealed Stage 6 decision contract and does not modify the Guardrail repo.
- Do not add new external adapter families from this quickstart/glossary slice.
