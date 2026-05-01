# Phase 4 Software Generation Adapter Stability Audit

Date: 2026-05-01

Verdict: **Phase 4 cannot be called production-stable yet.**

This was a repo-level audit of the existing DGCE software-generation adapter. No runtime behavior, tests, schemas, Code Graph contracts, Guardrail repo files, Stage 7.5 design, GCE Stage 0 behavior, simulation engines, or Game Adapter planning were changed.

## Evidence Boundary

The requested authoritative source, `DGCE Master Handoff Document v1.3`, was not present in this checkout by filename or searchable text. This report therefore uses the user-provided priority and hard limits as the governing handoff summary, and cites only local repo evidence.

Repo inspected: `C:\Users\samcf\Desktop\Dev\Aether repo`

Primary code/docs inspected:

- `aether/dgce/decompose.py`
- `aether/dgce/incremental.py`
- `aether/dgce/prepare_api.py`
- `aether/dgce/approve_api.py`
- `aether/dgce/execute_api.py`
- `aether/dgce/read_api.py`
- `aether/dgce/read_api_http.py`
- `aether/dgce/sdk.py`
- `aether/dgce/code_graph_context.py`
- `aether/dgce/gce_ingestion.py`
- `docs/dgce_controlled_write_contract.md`
- `docs/dgce_workspace_artifacts.md`
- `docs/gce_stage0_ingestion.md`
- DGCE/GCE lock and API tests under `tests/`

## Test Results

Command:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'; python -m pytest -q -p no:cacheprovider --basetemp C:\tmp\dgce_phase4_audit_pytest tests/test_gce_stage0_boundary.py tests/test_gce_stage0_contract_lock.py tests/test_gce_stage0_fixtures.py tests/test_gce_stage0_read_api.py tests/test_dgce_prepare_api.py tests/test_dgce_approve_api.py tests/test_dgce_execute_api.py tests/test_stage75_contract_lock.py tests/test_model_execution_slice.py tests/test_dgce_read_api.py tests/test_dgce_read_api_http.py tests/test_dgce_sdk.py tests/test_dgce_refresh_api.py
```

Result: **4 failed, 361 passed in 309.37s**

Failures:

- `tests/test_dgce_approve_api.py::TestDGCEApproveAPI::test_default_approve_uses_preview_recommended_mode`
- `tests/test_dgce_approve_api.py::TestDGCEApproveAPI::test_override_selected_mode_changes_execution_permission_outcome`
- `tests/test_dgce_approve_api.py::TestDGCEApproveAPI::test_approve_recomputes_only_target_section_and_does_not_execute`
- `tests/test_dgce_execute_api.py::TestDGCEExecuteAPI::test_owned_bundle_rerun_audit_manifest_is_distinct_and_deterministic`

Command:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'; python -m pytest -q -p no:cacheprovider --basetemp C:\tmp\dgce_phase4_audit_pytest_incremental tests/test_dgce_incremental.py tests/test_dgce_loop.py tests/test_dgce_inspector.py
```

Result: **2 failed, 377 passed in 470.03s**

Failures:

- `tests/test_dgce_incremental.py::test_execute_reserved_simulation_gate_fail_closes_invalid_provider_response`
- `tests/test_dgce_incremental.py::test_external_dry_run_normalizers_emit_only_compact_contract_findings[dockerfile-findings1]`

An earlier attempt to include `tests/test_dgce_api.py` returned `file or directory not found`; that file does not exist in this checkout.

## Stability Assessment

### 1. End-to-End Lifecycle Coverage From Stage 0 Through Stage 9

Status: **Partial, not production-stable.**

Evidence:

- GCE Stage 0 ingestion and Stage 1 release are implemented separately in `aether/dgce/gce_ingestion.py` and documented in `docs/gce_stage0_ingestion.md`.
- The software-generation lifecycle order in `aether/dgce/decompose.py:221` is `preview`, `review`, `approval`, `preflight`, `gate`, `alignment`, `execution`, `outputs`.
- Stage 7.5 simulation is implemented as a reserved gate in `aether/dgce/decompose.py:7900`, but it is not represented in `DGCE_LIFECYCLE_ORDER`.
- Stage 9 read-model/dashboard/consumer surfaces are built in `aether/dgce/decompose.py:8112`, `aether/dgce/decompose.py:8177`, `aether/dgce/decompose.py:8282`, `aether/dgce/decompose.py:8379`, and `aether/dgce/decompose.py:8450`.

Gap: there is no single explicit Stage 0 through Stage 9 lifecycle contract in code or docs. The implementation is strong in pieces, but the trace names do not map cleanly to the requested stage numbering.

### 2. Stage 2 Preview Artifact Completeness and Determinism

Status: **Strong, with hardening needed.**

Evidence:

- Preview artifact generation is deterministic and sorted by path in `aether/dgce/incremental.py:269`.
- Preview summaries and recommended modes are derived in `aether/dgce/incremental.py:329`.
- Preview can include Code Graph planning context, but falls back conservatively when facts are absent, malformed, missing placement facts, or unmapped to preview targets in `aether/dgce/incremental.py:664`.
- Preview artifacts are fingerprinted when persisted in `aether/dgce/decompose.py:756`.

Gap: docs do not yet capture the modern preview contract, including Code Graph fallback fields and Stage 7 alignment dependencies.

### 3. Stage 3 Review Bundle and Stage 4 Approval Behavior

Status: **Mixed. Review is stable; approval tests are red.**

Evidence:

- Review markdown is rendered deterministically from preview artifacts in `aether/dgce/incremental.py:354` and written with review fingerprinting in `aether/dgce/decompose.py:758`.
- Approval API validates section existence, required current artifacts, selected mode, and section artifact validity in `aether/dgce/approve_api.py:22`.
- Approval artifacts capture input, preview, review fingerprints and execution permission via `_build_approval_artifact` in `aether/dgce/decompose.py:3796`.

Gap: three approval API tests still expect `.dce/preflight/{section_id}.execution_gate.json`, while the implemented and newer tested contract writes gates to `.dce/execution/gate/{section_id}.execution_gate.json` in `aether/dgce/decompose.py:1279`, `aether/dgce/prepare_api.py:42`, and `aether/dgce/refresh_api.py:30`. This is likely a stale-test/contract-location mismatch, but a red suite blocks production-stable status.

### 4. Stage 5 Stale Approval / Fingerprint-Chain Enforcement

Status: **Strong, with one rerun/audit failure.**

Evidence:

- Preflight, stale-check, and gate recomputation are explicit in `aether/dgce/prepare_api.py:421`.
- Prepared plans bind input, preview, review, approval, preflight, stale-check, and execution-gate fingerprints in `aether/dgce/prepare_api.py:91`.
- Prepared plan approval lineage is independently fingerprinted in `aether/dgce/prepare_api.py:119`.
- Execution revalidates prepared-plan binding and approval lineage before running in `aether/dgce/execute_api.py:1148`.
- Bundle and section provenance verification are implemented in `aether/dgce/execute_api.py:431` and `aether/dgce/execute_api.py:551`.

Gap: `tests/test_dgce_execute_api.py:4440` fails because the owned-bundle safe-modify rerun returns HTTP 400 instead of writing a distinct deterministic rerun audit manifest. That is a production-stability risk for reruns and audit trace continuity.

### 5. Stage 6 Guardrail Gate Coverage for ALLOW / BLOCK / MODIFY

Status: **Not production-stable.**

Evidence:

- Gate input is a factual artifact built in `aether/dgce/decompose.py:4368`.
- Target classifications are kept policy-free and factual in `aether/dgce/decompose.py:4466`.
- `_pass_gate_input_to_guardrail` currently returns the gate input unchanged in `aether/dgce/decompose.py:4586`.
- Execution gate pass/block is derived locally from preflight, stale, and approval permission in `aether/dgce/decompose.py:4266`.

Gap: this validates an immutable Guardrail handoff shape, but it does not appear to consume an actual Guardrail ALLOW/BLOCK/MODIFY decision artifact. Existing tests cover handoff immutability, not end-to-end Guardrail policy decision enforcement.

### 6. Stage 7 Alignment and Drift Detection Coverage

Status: **Strong, with docs lag.**

Evidence:

- Alignment is recorded in `aether/dgce/decompose.py:1284`.
- Alignment checks approved scope, intent, strategy, justification, and gate context in `aether/dgce/decompose.py:5162`.
- Drift findings are normalized and ordered in `aether/dgce/decompose.py:5765`.
- Execution blocks on alignment drift in `aether/dgce/decompose.py:815`.

Gap: the implementation is deeper than the docs. The public docs do not clearly describe the Stage 7 artifact contract or the precise drift reason taxonomy.

### 7. Stage 7.5 Locked Seam Regression Coverage

Status: **Strong coverage, but currently failing on two Stage 7.5-adjacent assertions.**

Evidence:

- Stage 7.5 has trigger and simulation record models in `aether/dgce/decompose.py:165` and `aether/dgce/decompose.py:174`.
- Trigger and simulation artifacts are built in `aether/dgce/decompose.py:6127` and `aether/dgce/decompose.py:6249`.
- Provider selection, execution, fail-closed behavior, projection consistency, and lifecycle order are covered heavily in `tests/test_stage75_contract_lock.py`.
- `tests/test_stage75_contract_lock.py` passed as part of the first command.

Gaps:

- `tests/test_dgce_incremental.py:8177` expects invalid provider responses to be reported as `invalid_provider_response`, but the current artifact reports `provider_exception`.
- `tests/test_dgce_incremental.py:10267` expects compact external Dockerfile findings not to include raw phrase leakage such as `unknown instruction`; the current normalized summary includes that phrase.

### 8. Stage 8 Execution Write-Scope Enforcement

Status: **Strong, with rerun instability.**

Evidence:

- File plans targeting `.dce` are rejected before writes in `aether/dgce/decompose.py:729`.
- Controlled-write decisions are centralized in `aether/dgce/incremental.py:549`.
- Safe Modify requires prior ownership and identical-content checks in `aether/dgce/incremental.py:564`.
- `write_file_plan` is still a filesystem backstop against unauthorized overwrites in `aether/dgce/file_writer.py:12`.
- Prepared file plans cannot exceed preview scope in `aether/dgce/prepare_api.py:188`.
- Execution uses prepared file plans after binding/lineage checks in `aether/dgce/execute_api.py:1148`.

Gap: safe-modify rerun audit coverage is red in `tests/test_dgce_execute_api.py:4440`, so write-scope enforcement is not yet production-stable for rerun flows.

### 9. Stage 9 Output / Read-Model / Dashboard / Consumer-Contract Consistency

Status: **Strong.**

Evidence:

- Workspace summary, lifecycle trace, workspace index, dashboard, artifact manifest, consumer contract, export contract, and references are built together in `aether/dgce/decompose.py:8832`.
- Manifest/contract/reference convergence assertions run before persistence in `aether/dgce/decompose.py:8846`.
- Read API validates locked artifact schemas in `aether/dgce/read_api.py:28`.
- HTTP read endpoints return the exact read model payloads without wrappers in `aether/dgce/read_api_http.py:67`.
- SDK methods map to read endpoints in `aether/dgce/sdk.py:38`.

Gap: SDK is read-only. If production readiness requires SDK-level prepare/approve/execute/bundle/provenance operations, that surface is not present.

### 10. Code Graph Integration and Fallback Behavior When Absent

Status: **Stable and bounded.**

Evidence:

- Code Graph facts are validated against the local `dcg.facts.v1` contract in `aether/dgce/code_graph_context.py:112`.
- Vendored schema checksum validation exists in `aether/dgce/code_graph_context.py:166`.
- Preview falls back to baseline planning when facts are absent or malformed in `aether/dgce/incremental.py:664`.
- Gate input marks Code Graph as `absent`, `invalid`, or `available` in `aether/dgce/decompose.py:4446`.
- Tests in `tests/test_model_execution_slice.py:781` cover schema checksum and drift detection.

Gap: none blocking. This audit did not modify Code Graph or `dcg.facts.v1`.

### 11. Artifact Fingerprinting and Audit Trace Consistency

Status: **Strong, with rerun audit gap.**

Evidence:

- JSON artifacts are written with canonical artifact fingerprints via `_write_json_with_artifact_fingerprint` in `aether/dgce/decompose.py`.
- Review fingerprints use `compute_review_artifact_fingerprint` and are checked by prepare in `aether/dgce/prepare_api.py:473`.
- Prepared plan binding and approval lineage fingerprints are checked before execution in `aether/dgce/execute_api.py:116`.
- Execution audit manifests and cross-links are persisted in `aether/dgce/execute_api.py:1080` and `aether/dgce/execute_api.py:1122`.
- Bundle manifests and indexes are fingerprinted in `aether/dgce/execute_api.py:1009`.

Gap: the safe-modify rerun audit test failure means audit trace consistency is not yet proven for rerun mutation scenarios.

### 12. API / SDK / Read-Model Production Readiness

Status: **Partial.**

Evidence:

- FastAPI includes operational DGCE routes plus read routes in `apps/aether_api/main.py:86`.
- DGCE read routes enforce optional API key auth in `aether/dgce/read_api_http.py:30`.
- DGCE API middleware adds no-store/security headers, request IDs, and a simple per-client read/write route rate limit in `apps/aether_api/main.py:36`.
- Read API HTTP tests passed in the first command.
- SDK read endpoint tests passed in the first command.

Gaps:

- SDK is read-only (`aether/dgce/sdk.py:13`).
- Operational approve/prepare/execute endpoint tests are red.
- There is no OpenAPI or versioned public API stability document for the operational lifecycle endpoints.

### 13. Existing Failing or Flaky Tests

Known failures from this audit:

- 3 approval API tests fail because they look for execution gate artifacts under `.dce/preflight/` while current implementation writes `.dce/execution/gate/`.
- 1 execute API rerun audit test fails with HTTP 400 during safe-modify rerun.
- 1 Stage 7.5 provider invalid-response test fails because the artifact records `provider_exception` instead of `invalid_provider_response`.
- 1 external Dockerfile dry-run normalizer test fails because compact findings include the phrase `unknown instruction`.

No flakiness was proven; these reproduced in deterministic targeted runs.

### 14. Ranked Gaps

#### P0 Blockers

1. Relevant Phase 4 suites are red: 6 total failing tests across targeted DGCE stability commands.
2. Stage 6 Guardrail is not yet proven as an ALLOW/BLOCK/MODIFY enforcement loop; current code only hands an unchanged factual gate input through `_pass_gate_input_to_guardrail`.
3. Safe-modify rerun audit behavior is failing for owned bundles, blocking production confidence in rerunnable Stage 8/Stage 9 audit chains.

#### P1 Production-Stability Risks

1. Approval API gate artifact location is inconsistent across tests and implementation. Newer implementation points to `.dce/execution/gate/`; some approval tests still expect `.dce/preflight/`.
2. Stage 7.5 invalid provider response classification is not matching the locked expected reason code.
3. External Dockerfile dry-run finding summaries are not compact enough for the locked contract.
4. Stage 0 through Stage 9 exists as pieces, but there is no single explicit lifecycle/read-model contract that includes Stage 0, Stage 7.5, and Stage 9 terminology together.

#### P2 Hardening

1. Public docs lag the implemented lifecycle. `docs/dgce_controlled_write_contract.md` and `docs/dgce_workspace_artifacts.md` describe older workspace/run contracts and omit prepared plans, bundle audit manifests, Stage 7 alignment, Stage 7.5 projections, and read-model convergence.
2. SDK is read-only. If production consumers need lifecycle operations, add a versioned operational SDK contract before expansion.
3. Add a concise production runbook for failure triage: stale approval, binding mismatch, alignment drift, simulation fail/indeterminate, rerun refusal, and read-model invalidation.

#### P3 Polish

1. Add a repo-local pointer or copy for `DGCE Master Handoff Document v1.3` so future audits can cite it directly.
2. Add a small stage-number mapping table to docs so Stage 2/3/4/5/6/7/7.5/8/9 terminology matches artifact names.
3. Consider making operator dashboard surfaces link directly to verification reports and failing check IDs for faster triage.

## Recommended Next Build Slices, Ordered by Risk

1. **Restore green Phase 4 stability suites.** Reconcile the gate artifact path tests, fix safe-modify rerun audit behavior, fix Stage 7.5 invalid-provider reason normalization, and fix compact external Dockerfile findings.
2. **Seal the Stage 6 Guardrail decision contract.** Without changing the Guardrail repo, define and test how DGCE consumes an ALLOW/BLOCK/MODIFY decision artifact or adapter result, and prove fail-closed behavior.
3. **Stabilize safe-modify rerun provenance.** Add focused tests around owned-path mutation, prepared-plan rebinding, audit manifest changes, and deterministic rerun payloads.
4. **Publish a Stage 0 to Stage 9 lifecycle/read-model contract.** Map stage numbers to artifact names, paths, fingerprints, read models, and blocking semantics, including Stage 7.5 and Stage 9 surfaces.
5. **Harden API/SDK readiness.** Decide whether SDK remains read-only for Phase 4 or grows versioned lifecycle methods, then add contract docs and tests accordingly.

## Production-Stability Verdict

The Software Generation Adapter is close in several core areas: deterministic preview/review, prepared-plan fingerprint chains, Code Graph fallback, alignment drift detection, Stage 7.5 lock coverage, write-scope enforcement, and Stage 9 read-model convergence are all meaningfully implemented.

It is **not production-stable yet** because the relevant stability suites are red, Guardrail ALLOW/BLOCK/MODIFY enforcement is not proven end-to-end, and safe-modify rerun audit behavior is currently failing.

## Untouched Scope Confirmation

Confirmed untouched during this audit:

- Stage 7.5 design and runtime behavior
- GCE Stage 0 behavior
- Code Graph and `dcg.facts.v1`
- Simulation engines
- Game Adapter Stage 2 planning
- Guardrail repository files
- Runtime behavior and tests

