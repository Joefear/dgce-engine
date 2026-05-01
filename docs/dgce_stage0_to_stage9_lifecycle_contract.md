# DGCE Stage 0 to Stage 9 Lifecycle Contract

This document is the authoritative developer/operator map for the current DGCE software-generation lifecycle and its locked GCE Stage 0 boundary. It documents implemented behavior only. It does not reopen GCE Stage 0, redesign Stage 7.5, add simulation engines, change Code Graph or `dcg.facts.v1`, modify Guardrail, or begin Game Adapter Stage 2 planning.

The canonical DGCE lifecycle order exposed by read models is:

```text
preview -> review -> approval -> preflight -> gate -> alignment -> execution -> outputs
```

Stage 7.5 is a reserved simulation seam between Stage 7 and Stage 8. It is intentionally not inserted into the canonical lifecycle order.

## Stage Map

| Stage | Canonical Name | Primary Contract |
| --- | --- | --- |
| Stage 0 | GCE input boundary / software pass-through | Normalize and lock adapter-aware input before release to Stage 1. |
| Stage 1 | Section intake and planning basis | Persist section input and deterministic task/change planning basis. |
| Stage 2 | Preview | Produce deterministic preview/change intent for operator review. |
| Stage 3 | Review | Produce human-readable review bundle for the preview. |
| Stage 4 | Approval | Persist operator approval and selected execution mode. |
| Stage 5 | Preflight and stale-check | Revalidate approval, preview, review, and execution permission before gate. |
| Stage 6 | Execution gate | Consume Stage 5 facts plus Guardrail decision state before downstream execution. |
| Stage 7 | Alignment | Detect approved-scope/write-plan drift before execution. |
| Stage 7.5 | Reserved simulation seam | Locked fail-closed seam for simulation/provider evidence; engines remain reserved. |
| Stage 8 | Execution | Perform controlled writes and persist execution audit/stamp artifacts. |
| Stage 9 | Outputs and read models | Persist output records and operator/SDK/read-model surfaces. |

## Stage 0: GCE Input Boundary / Software Pass-Through

**Purpose:** Stage 0 is the locked GCE ingestion boundary. GCE inputs normalize into a `DGCEStage0InputPackage`; existing software-generation inputs pass through to the existing `DGCESection` lifecycle without changing software ingestion.

**Required input artifacts:** Structured GCE ingestion input declaring the GCE ingestion contract, or an existing software-generation section object. Free-form natural language is blocked and cannot bypass Stage 0.

**Emitted output artifacts:** For GCE, a Stage 0 package with `contract_name: DGCEStage0InputPackage`, `contract_version: dgce.stage0.input_package.v1`, normalized session intent or clarification request, validation metadata, and `stage_1_release_blocked`.

**Canonical persistence paths:** GCE Stage 0 packages persist at `.dce/input/gce/{source_id}.{input_path}.stage0.json`. Software-generation sections later persist as `.dce/input/{section_id}.json` in Stage 1.

**Fingerprint/integrity behavior:** Persisted GCE Stage 0 packages are written with `artifact_fingerprint`. Release from disk requires a valid artifact fingerprint.

**Blocking/fail-closed behavior:** Stage 0 blocks release when the package is malformed, ambiguity is unresolved, validation fails, normalized session intent is absent, clarification is present, or the persisted fingerprint is missing/invalid.

**Read-model/operator exposure:** GCE Stage 0 behavior is documented in `docs/gce_stage0_ingestion.md` and locked by GCE Stage 0 contract/read API tests. It is complete and locked; this document does not reopen it.

## Stage 1: Section Intake and Planning Basis

**Purpose:** Stage 1 establishes the per-section software-generation workspace identity and deterministic planning basis.

**Required input artifacts:** A valid `DGCESection` payload or Stage 0 released session content that can be represented as section input.

**Emitted output artifacts:** Section input, task plan, state, and change-plan basis when applicable.

**Canonical persistence paths:** `.dce/input/{section_id}.json`, `.dce/plans/{section_id}.json`, `.dce/plans/{section_id}.change_plan.json`, `.dce/state/{section_id}.json`, and `.dce/index.yaml`.

**Fingerprint/integrity behavior:** The section input participates in later prepared-plan binding through `compute_json_file_fingerprint`. Change plans are deterministic JSON. State files are status snapshots rather than authorization artifacts.

**Blocking/fail-closed behavior:** Invalid or missing section input prevents downstream section lookup. Generated scaffold file plans must not target `.dce/`; attempts to write DGCE workspace paths raise before execution.

**Read-model/operator exposure:** Section identity appears in `workspace_index`, `lifecycle_trace`, `artifact_manifest`, and dashboard surfaces once workspace views refresh.

## Stage 2: Preview

**Purpose:** Stage 2 emits deterministic implementation intent without writing project source files.

**Required input artifacts:** Stage 1 section input and planning/change-plan basis. When Code Graph context is supplied, it must validate as `dcg.facts.v1`; absent or malformed Code Graph context degrades to conservative fallback.

**Emitted output artifacts:** Preview artifact describing planned actions, target files, recommended mode, ownership/write intent, and Code Graph fallback or validation status.

**Canonical persistence paths:** `.dce/plans/{section_id}.preview.json`. The supporting change plan is `.dce/plans/{section_id}.change_plan.json`.

**Fingerprint/integrity behavior:** Preview artifacts are written with `artifact_fingerprint`. Approval and prepared-plan binding later reference the preview fingerprint.

**Blocking/fail-closed behavior:** Stage 2 itself is read-only. Invalid preview fingerprints, broken artifact linkage, or drift in preview identity prevent approval/prepare/execution downstream.

**Read-model/operator exposure:** Review index, workspace summary, workspace index, lifecycle trace, dashboard, artifact manifest, consumer contract, and export contract expose preview path, preview outcome class, and recommended mode.

## Stage 3: Review

**Purpose:** Stage 3 produces the operator-facing review bundle for the Stage 2 preview.

**Required input artifacts:** Stage 2 preview and Stage 1 section context.

**Emitted output artifacts:** Markdown review bundle with embedded review fingerprint metadata.

**Canonical persistence paths:** `.dce/reviews/{section_id}.review.md`.

**Fingerprint/integrity behavior:** Review Markdown uses the DGCE review fingerprint convention. Approval and prepared-plan lineage use `compute_review_artifact_fingerprint`; prepare requires `verify_review_artifact_fingerprint`.

**Blocking/fail-closed behavior:** Missing or invalid review artifacts prevent approval and prepare from becoming eligible. Review fingerprint drift after approval invalidates stale-check/gate readiness.

**Read-model/operator exposure:** Review index and workspace summary expose `review_path`, `review_status`, review-derived latest decision fields, and navigation links.

## Stage 4: Approval

**Purpose:** Stage 4 records explicit operator approval and selected execution mode.

**Required input artifacts:** Current Stage 1 input, Stage 2 preview, and Stage 3 review. API approval also requires valid artifact linkage from the manifest/index.

**Emitted output artifacts:** Approval artifact containing approval status, selected mode, execution permission, approval source, and fingerprints for approved input/preview/review.

**Canonical persistence paths:** `.dce/approvals/{section_id}.approval.json`.

**Fingerprint/integrity behavior:** Approval artifacts are written with `artifact_fingerprint`. Prepared-plan approval lineage stores approval artifact fingerprint and approval record fingerprint.

**Blocking/fail-closed behavior:** Invalid selected modes are rejected. Missing current input/preview/review, invalid artifact linkage, rejected approval, missing approval, or invalid approval fingerprint prevents prepare/execution. Successful execution supersedes consumed approval.

**Read-model/operator exposure:** Review index, workspace summary, lifecycle trace, workspace index, dashboard, provenance, summary, overview, and verification APIs expose approval status, selected mode, execution permission, and approval consumption where applicable.

## Stage 5: Preflight and Stale-Check

**Purpose:** Stage 5 revalidates that the approved input, preview, and review are still current and that the selected mode permits execution.

**Required input artifacts:** Stage 4 approval plus current Stage 1 input, Stage 2 preview, and Stage 3 review.

**Emitted output artifacts:** Preflight record and stale-check record.

**Canonical persistence paths:** `.dce/preflight/{section_id}.preflight.json` and `.dce/preflight/{section_id}.stale_check.json`.

**Fingerprint/integrity behavior:** Preflight records are written with `artifact_fingerprint`. Stale-check records deterministically compare approval-linked fingerprints to current input/preview/review fingerprints. Prepared-plan binding includes preflight and stale-check file fingerprints.

**Blocking/fail-closed behavior:** Missing approval, rejected approval, non-executable selected mode, stale preview/review/input fingerprints, missing/invalid preflight fingerprint, or stale-check failure prevents Stage 6 from allowing execution.

**Read-model/operator exposure:** Review index and workspace summary expose `preflight_path`, `preflight_status`, `execution_allowed`, `stale_check_path`, `stale_status`, and `stale_detected`.

## Stage 6: Execution Gate

**Purpose:** Stage 6 consumes Stage 5 facts plus the DGCE Guardrail decision-consumption contract before downstream lifecycle stages are allowed.

**Required input artifacts:** Stage 5 preflight and stale-check records, Stage 4 approval, Stage 2 preview, Stage 1 input, and the Stage 6 gate-input artifact handed to Guardrail unchanged.

**Emitted output artifacts:** Gate-input artifact and execution-gate artifact. The gate artifact records `gate_status`, `execution_blocked`, `guardrail_decision`, `guardrail_decision_supported`, checked artifacts, checks, reasons, and decision summary.

**Canonical persistence paths:** The canonical Stage 6 execution gate path is `.dce/execution/gate/{section_id}.execution_gate.json`. The companion gate-input path is `.dce/execution/gate/{section_id}.gate_input.json`. Stage 6 gate artifacts do not live under `.dce/preflight/`.

**Fingerprint/integrity behavior:** Gate input is written with `artifact_fingerprint` and includes `gate_input_fingerprint`. The execution gate stores `gate_input_path` and `gate_input_fingerprint`. Prepare recomputes gate input and gate facts, verifies the persisted gate-input artifact fingerprint, and requires the persisted gate to match the recomputed gate. Prepared-plan binding includes the execution-gate file fingerprint.

**Blocking/fail-closed behavior:** `ALLOW` permits downstream only when all required gate fields and fingerprints are valid, gate status is `gate_pass`, execution is not blocked, the decision summary allows execution, and persisted gate input matches. `BLOCK` prevents execution. `MODIFY` is not supported as a DGCE Stage 6 consumption behavior and fails closed. Missing, malformed, unknown, or unsupported decision values fail closed.

**Read-model/operator exposure:** Review index and workspace summary expose `execution_gate_path`, `gate_status`, `execution_blocked`, `guardrail_decision`, and `guardrail_decision_supported`. Lifecycle trace exposes the `gate` stage status. Prepared-plan binding and verification surfaces expose execution-gate fingerprint consistency.

## Stage 7: Alignment

**Purpose:** Stage 7 verifies that the write plan remains aligned with the approved scope before execution writes.

**Required input artifacts:** Stage 6 execution gate, Stage 4 approval, current file plan, change plan, write transparency, and ownership index where safe-modify/ownership rules apply.

**Emitted output artifacts:** Alignment record with alignment status, drift findings, selected mode, and blocked state.

**Canonical persistence paths:** `.dce/execution/alignment/{section_id}.alignment.json`.

**Fingerprint/integrity behavior:** Alignment artifacts are written with `artifact_fingerprint`. Execution records link the alignment artifact. Prepared execution and rerun safety recompute alignment-like checks for write-scope and safe-modify validation.

**Blocking/fail-closed behavior:** Alignment drift, ownership violations, unsupported modifications, or safe-modify violations block Stage 8. Blocked alignment records lead to an execution stamp with `execution_blocked: true` and no project writes.

**Read-model/operator exposure:** Review index and workspace summary expose `alignment_path`, `alignment_status`, and `alignment_blocked`. Lifecycle trace exposes the `alignment` stage status. Execution records link the alignment artifact.

## Stage 7.5: Locked Reserved Simulation Seam

**Purpose:** Stage 7.5 is a locked reserved seam between alignment and execution. It records simulation/provider trigger and result facts without redefining the canonical lifecycle order.

**Required input artifacts:** Stage 7 alignment context, write plan context, and a simulation trigger input when simulation is requested. Existing provider families are limited to the current inventory; no new external adapter families are introduced by this contract.

**Emitted output artifacts:** Simulation trigger artifact and simulation record.

**Canonical persistence paths:** `.dce/execution/simulation/{section_id}.simulation_trigger.json` and `.dce/execution/simulation/{section_id}.simulation.json`.

**Fingerprint/integrity behavior:** Trigger and simulation records are written with `artifact_fingerprint`. Simulation projections are normalized into section summaries and read models. Raw provider internals, stack traces, stdout, and stderr are not persisted as unbounded read-model text.

**Blocking/fail-closed behavior:** Simulation `pass` allows Stage 8; `fail` blocks; `indeterminate` blocks; skipped/not-triggered records `simulation_stage_applicable` in the projection. Provider exceptions and malformed responses normalize to stable fail-closed reasons. Simulation engines remain reserved; this contract does not implement them.

**Read-model/operator exposure:** Stage 7.5 is not in `DGCE_LIFECYCLE_ORDER`. Its projection appears under `section_summary.simulation` in review index, workspace summary, lifecycle trace, workspace index, dashboard, consumer contract, and export contract.

## Stage 8: Execution

**Purpose:** Stage 8 performs controlled project writes and persists execution audit/stamp artifacts.

**Required input artifacts:** Valid prepared plan, valid Stage 4 approval lineage, valid Stage 6 execution gate, valid Stage 7 alignment, Stage 7.5 pass/skip where applicable, file plan, change plan, ownership index, and write transparency.

**Emitted output artifacts:** Execution record/stamp, optional archived prior execution for reruns, prepared-plan audit manifest/cross-link embedded in execution record, and project source files allowed by write transparency.

**Canonical persistence paths:** `.dce/execution/{section_id}.execution.json`, `.dce/execution/archive/{section_id}.{execution_fingerprint}.execution.json` for archived rerun provenance, and `.dce/plans/{section_id}.prepared_plan.json` for the sealed prepare artifact consumed by execution.

**Fingerprint/integrity behavior:** Prepared plans are written with `artifact_fingerprint` and bind input, preview, review, approval, preflight, stale-check, and execution-gate fingerprints. Execution validates prepared-plan binding and approval lineage before writes. Execution records include prepared-plan audit manifest fingerprints and cross-link fingerprints. Function-stub execution may include model execution basis and content fingerprints.

**Blocking/fail-closed behavior:** Execution refuses when prepare is ineligible, prepared-plan binding drifts, approval lineage drifts, prior execution exists without `rerun=true`, rerun is unsafe, Stage 6 gate is invalid, Stage 7 blocks, or Stage 7.5 blocks. Write-scope enforcement prevents `.dce` targets and blocks/skips writes outside approved ownership/mode.

**Read-model/operator exposure:** Execution records are linked from review index, workspace summary, lifecycle trace, workspace index, dashboard, provenance, verify, summary, overview, bundle manifest/index, and output records.

## Stage 9: Outputs and Read Models

**Purpose:** Stage 9 publishes the final section output and converged operator/consumer read models.

**Required input artifacts:** Stage 8 execution facts, write transparency, file plan, validation summary, advisory result, ownership state, and all prior lifecycle artifacts needed for read-model convergence.

**Emitted output artifacts:** Section output record, advisory index, ownership index, review index, workspace summary, lifecycle trace, workspace index, dashboard, artifact manifest, consumer contract, export contract, and Markdown contract references.

**Canonical persistence paths:** `.dce/outputs/{section_id}.json`, `.dce/advisory_index.json`, `.dce/ownership_index.json`, `.dce/reviews/index.json`, `.dce/workspace_summary.json`, `.dce/lifecycle_trace.json`, `.dce/workspace_index.json`, `.dce/dashboard.json`, `.dce/artifact_manifest.json`, `.dce/consumer_contract.json`, `.dce/export_contract.json`, `.dce/consumer_contract_reference.md`, and `.dce/export_contract_reference.md`.

**Fingerprint/integrity behavior:** Stage 9 read models are deterministic JSON/Markdown projections from persisted artifacts. The artifact manifest and consumer/export contracts are cross-checked for convergence before writing. Output records carry execution outcome, output summary, generated artifacts, and write transparency.

**Blocking/fail-closed behavior:** Stage 9 does not authorize writes. It reflects the final persisted lifecycle state. Schema validation failures, invalid artifact references, or consumer/export contract divergence fail validation/read APIs rather than granting execution permission.

**Read-model/operator exposure:** Read API, HTTP read API, SDK, dashboard, review index, workspace summary, lifecycle trace, workspace index, artifact manifest, consumer contract, export contract, provenance, verification, summary, overview, and bundle surfaces consume Stage 9 outputs.

## Artifact Distinctions

- Preflight artifacts are Stage 5 records under `.dce/preflight/`. They answer whether approval and execution permission are current.
- Stage 6 execution gate artifacts are under `.dce/execution/gate/`. The canonical gate path is `.dce/execution/gate/{section_id}.execution_gate.json`; this is distinct from preflight.
- Stage 7 alignment artifacts are under `.dce/execution/alignment/`. They answer whether the write plan still matches approved scope.
- Stage 7.5 simulation artifacts are under `.dce/execution/simulation/`. They are locked reserved-seam artifacts and are not part of the canonical lifecycle order.
- Stage 8 execution artifacts are under `.dce/execution/` and record controlled write results, prepared-plan audit links, rerun provenance, and execution status.
- Stage 9 output artifacts are under `.dce/outputs/` and root `.dce/` read-model files. They are consumer/operator projections, not authorization sources.

## Production Stability Checklist

- GCE Stage 0 remains complete and locked; release requires valid structured input and artifact fingerprint when persisted.
- Stage 2 preview and Stage 3 review must be deterministic and fingerprinted before approval.
- Stage 4 approval must reference current input, preview, and review fingerprints.
- Stage 5 stale-check must fail closed on approval/input/preview/review drift.
- Stage 6 must consume only the canonical `.dce/execution/gate/{section_id}.execution_gate.json` gate path and fail closed for missing, malformed, unknown, `BLOCK`, or unsupported `MODIFY` decisions.
- Prepared plans must bind current input, preview, review, approval, preflight, stale-check, and execution-gate fingerprints before Stage 8.
- Stage 7 must block alignment drift and write-scope violations.
- Stage 7.5 pass allows Stage 8; fail and indeterminate block; skipped/not-triggered must still project `simulation_stage_applicable`.
- Stage 8 must write only approved project paths and must not target `.dce`.
- Stage 9 read models must converge with artifact manifest, consumer contract, export contract, and references.
- Code Graph / `dcg.facts.v1` remains optional input context with deterministic fallback when absent or invalid.
- Simulation engines remain reserved; no new simulation engine behavior is implied by this contract.
