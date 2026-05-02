# DGCE SDK Lifecycle Scope

This document defines the current lifecycle scope and readiness posture for the DGCE Python SDK. It documents implemented behavior only. It does not change runtime behavior, reopen GCE Stage 0, redesign Stage 7.5, add simulation engines, modify Code Graph or `dcg.facts.v1`, modify Guardrail, add external adapter families, or start Game Adapter Stage 2 work.

Related contracts:

- `docs/dgce_stage0_to_stage9_lifecycle_contract.md`
- `docs/phase4_software_adapter_operational_runbook.md`
- `aether/dgce/sdk.py`
- `aether/dgce/read_api_http.py`
- `tests/test_dgce_sdk.py`

## Current Purpose

`DGCEClient` is a thin Python SDK for operator and integration consumers that need to inspect DGCE read models over HTTP. Its current purpose is read-only lifecycle observation, not lifecycle control.

The intended user is an operator tool, reporting integration, dashboard, CI audit job, or developer script that already has a DGCE workspace path and needs to read the persisted `.dce/` state through supported HTTP read endpoints.

The SDK returns HTTP JSON payloads exactly as served, without transforming schemas or hiding fields. This keeps the SDK aligned with the Stage 9 read-model and consumer-contract surfaces.

## Scope Verdict

The Phase 4 SDK is **read-only**.

It is not lifecycle-driving today. It does not expose prepare, approve, preflight/gate refresh, execute, rerun, bundle planning, bundle execution, provenance mutation, artifact repair, or direct file-write methods.

Safe SDK use today means:

- read existing lifecycle/read-model state,
- inspect artifact inventories and contracts,
- inspect GCE Stage 0 read-only artifacts,
- pass `X-API-Key` for read endpoints when configured,
- treat HTTP 400/404/other error mappings as read failures rather than execution permissions.

## Supported SDK Surfaces

| SDK method | HTTP route | Purpose |
| --- | --- | --- |
| `get_dashboard(workspace_path)` | `GET /v1/dgce/dashboard` | Read operator dashboard projection. |
| `get_workspace_index(workspace_path)` | `GET /v1/dgce/workspace-index` | Read workspace section index and artifact links. |
| `get_lifecycle_trace(workspace_path)` | `GET /v1/dgce/lifecycle-trace` | Read ordered lifecycle trace and section summaries. |
| `get_consumer_contract(workspace_path)` | `GET /v1/dgce/consumer-contract` | Read consumer-facing read-model field contract. |
| `get_export_contract(workspace_path)` | `GET /v1/dgce/export-contract` | Read export-facing contract projection. |
| `get_artifact_manifest(workspace_path)` | `GET /v1/dgce/artifact-manifest` | Read available artifact inventory. |
| `list_available_artifacts(workspace_path)` | `GET /v1/dgce/artifact-manifest` | Alias for artifact manifest. |
| `list_gce_stage0_artifacts(workspace_path)` | `GET /v1/dgce/gce/stage0-artifacts` | Read GCE Stage 0 artifact index. |
| `get_gce_stage0_artifact(workspace_path, artifact_name)` | `GET /v1/dgce/gce/stage0-artifacts/{artifact_name}` | Read one GCE Stage 0 artifact read model or read error. |

The SDK also supports optional API-key authentication by sending `X-API-Key` for its read requests.

## Read Models And Manifests

The SDK reads Stage 9 surfaces generated from persisted DGCE artifacts:

- `.dce/dashboard.json`
- `.dce/workspace_index.json`
- `.dce/lifecycle_trace.json`
- `.dce/consumer_contract.json`
- `.dce/export_contract.json`
- `.dce/artifact_manifest.json`

`get_artifact_manifest` and `list_available_artifacts` expose the same artifact inventory. They do not repair, regenerate, approve, or execute artifacts. If a read endpoint reports 400, the persisted artifact failed validation or integrity checks. If it reports 404, the requested workspace/artifact is not available through the read surface.

## GCE Stage 0

The SDK exposes GCE Stage 0 read-only surfaces only:

- list Stage 0 artifacts,
- read a specific Stage 0 artifact read model,
- receive deterministic read-error payloads for missing, malformed, or invalid artifacts.

The SDK does not assemble, persist, release, reopen, or mutate GCE Stage 0 input packages. GCE Stage 0 remains complete and locked. Stage 0 write/release helpers remain local code/API concerns outside the read-only SDK scope.

## Status, Health, And Version

The local API exposes public service endpoints:

- `GET /health`
- `GET /version`

`DGCEClient` does not currently wrap these endpoints. Operators may call them directly when they need service liveness or version information. The SDK readiness contract should treat health/version wrappers as optional future convenience methods, not as Phase 4 lifecycle scope.

## Safe Lifecycle Use Through SDK

The following lifecycle activities are safe through the SDK today:

- Confirm that a workspace has produced Stage 9 read models.
- Inspect whether a section reached `latest_stage: outputs` through workspace index, lifecycle trace, or dashboard surfaces.
- Inspect Stage 6 decision exposure such as `guardrail_decision`, `gate_status`, and `execution_gate_path` when those fields are present in read models.
- Inspect Stage 7 alignment and Stage 7.5 simulation projections in lifecycle trace or dashboard read models.
- Inspect artifact availability before a human or API workflow performs prepare/approve/execute.
- Verify exported consumer/export contracts for downstream reporting consumers.
- Inspect GCE Stage 0 read-only artifact state.

These uses are observational. The SDK response must not be treated as a write authorization by itself; execution authority remains with the lifecycle APIs and persisted gate/approval/alignment artifacts.

## API-Only Lifecycle Operations

The following operations should remain API-only for now:

- `POST /v1/dgce/sections/{section_id}/prepare`
- `POST /v1/dgce/sections/{section_id}/approve`
- `POST /v1/dgce/sections/{section_id}/execute`
- `POST /v1/dgce/refresh`
- `POST /v1/dgce/sections/plan-bundle`
- `POST /v1/dgce/sections/execute-bundle`
- section/bundle provenance, verification, summary, overview, and dashboard helper endpoints outside the current SDK method set
- rerun execution controls
- any artifact repair or workspace mutation flow

Keeping these API-only preserves a narrow Phase 4 SDK contract and avoids implying that SDK callers can bypass Guardrail, approval, stale-check, alignment, Stage 7.5, or write-scope enforcement.

## Boundaries

- No direct file writes: the SDK does not write project files or `.dce/` artifacts.
- No Guardrail bypass: the SDK only observes read models that may include Guardrail decision state.
- No Stage 7.5 simulation engine control: the SDK does not select providers, trigger engines, or interpret raw provider output.
- No GCE Stage 0 reopening: the SDK reads Stage 0 artifacts but does not change Stage 0 package/release behavior.
- No Code Graph changes: the SDK does not read or write `dcg.facts.v1` directly and does not change Code Graph fallback behavior.
- No Game Adapter Stage 2: the SDK scope does not add adapter planning or new adapter families.

## Error Handling Contract

`DGCEClient` maps HTTP read errors as follows:

- HTTP 400 becomes `ValueError`.
- HTTP 404 becomes `FileNotFoundError`.
- Other HTTP errors become `RuntimeError`.

This mapping is intentionally small. It preserves read endpoint details without inventing SDK-side lifecycle states.

## Risks And Future Work

Current risks:

- The SDK is intentionally smaller than the operational API. Consumers that need prepare/approve/execute must use HTTP routes directly.
- The SDK has no typed response models; callers receive raw dictionaries and must follow the consumer/export contracts for field expectations.
- The SDK has no explicit semantic version negotiation beyond the service `/version` endpoint and persisted read-model contract fields.
- Section and bundle operator helper reads are available over HTTP but are not wrapped by the SDK today.

Future work to decide explicitly:

- Whether approve, prepare, execute, rerun, and bundle flows should ever be exposed as SDK methods.
- Whether the production SDK should remain permanently read-only for operators and reporting consumers.
- Whether health/version wrappers should be added as read-only convenience methods.
- Whether SDK response typing should be introduced after read-model contracts stabilize further.
- What backward-compatibility policy applies to SDK method names, returned JSON, error mapping, and route versioning.

Recommended long-term default: keep the Phase 4 SDK read-only until a separate versioned lifecycle-control SDK contract is approved. If lifecycle methods are added later, they should be explicit, audited, and tested against the same fail-closed contracts as the HTTP APIs.

## Readiness Checklist

- SDK methods map only to supported read routes.
- SDK sends `X-API-Key` when configured.
- SDK returns HTTP JSON without transformation.
- SDK read calls are deterministic for repeated reads of unchanged artifacts.
- SDK exposes artifact manifest and read-model surfaces needed by operator/reporting consumers.
- SDK exposes GCE Stage 0 artifact reads as read-only surfaces.
- SDK does not expose prepare, approve, execute, rerun, bundle execution, direct file writes, Guardrail overrides, Stage 7.5 provider controls, Code Graph mutation, or Game Adapter Stage 2 behavior.
- Operators know to use lifecycle APIs, not SDK read calls, for prepare/approve/execute.
- Future SDK expansion requires a versioned contract and focused tests before adding lifecycle-driving methods.
