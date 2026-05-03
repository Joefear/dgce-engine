# Stage 7 Alignment

Stage 7 Alignment is the deterministic contract checkpoint between approved design intent and later execution authority. In the current locked implementation it builds, persists, and exposes `alignment_record.v1` artifacts only. It does not advance lifecycle state by itself.

## Purpose

Stage 7 Alignment records whether approved expectations match the preview and observed target set available to the builder. The current builder is intentionally narrow: it compares explicit structured targets, emits bounded drift codes, and derives execution permission from blocking drift.

## Contract

The canonical schema is:

```text
packages/dgce-contracts/schemas/alignment/alignment_record.v1.schema.json
```

The schema defines the full `alignment_record.v1` artifact, including fingerprints, alignment result, drift items, evidence, enrichment status, execution permission, and summary counts.

## Artifact Path

Persisted alignment records use:

```text
.dce/execution/alignment/{section_id}.alignment.json
```

Persistence validates the record against the locked schema before writing. Invalid records fail closed and are not written.

## Builder Behavior

`build_alignment_record_v1` accepts explicit structured inputs for alignment identity, timestamps, fingerprints, approved design expectations, preview proposed targets, and current observed targets.

The builder currently generates:

- `missing_expected_artifact` when an approved expected target is absent from preview and observed targets.
- `unexpected_artifact` when preview or observed targets include a target outside approved expectations.
- `structure_mismatch` when comparable structured metadata exists and differs from approved expectations.

If any blocking drift exists, the record is `misaligned`, `drift_detected` is `true`, and `execution_permitted` is `false`. Informational drift does not independently block execution.

The builder does not infer from free text and does not use LLMs.

## Reserved Drift Codes

The contract reserves these drift codes, but the v0.1 builder does not generate them yet:

- `symbol_resolution_conflict`
- `insertion_point_invalid`
- `design_contract_violation`
- `dependency_mismatch`
- `adapter_constraint_violation`

## Persistence

`persist_alignment_record_v1` writes only a contract-valid `alignment_record.v1` payload to the canonical `.dce/execution/alignment/` path. It uses deterministic pretty JSON and does not create execution stamps, output artifacts, Stage 8 artifacts, or lifecycle advancement records.

## Read Model

The read model is a compact bounded projection derived only from the alignment record:

- `section_id`
- `alignment_id`
- `alignment_result`
- `drift_detected`
- `execution_permitted`
- `blocking_issues_count`
- `informational_issues_count`
- `primary_reason`
- `drift_codes`
- `evidence_sources`
- `enrichment_status`
- `code_graph_used`
- `resolver_used`

It does not expose raw fingerprints, `timestamp`, full `drift_items`, full `evidence` objects, raw file contents, or raw model/provider text.

## Read Surfaces

Python read API:

- `get_stage7_alignment_read_model(workspace_path, section_id)`

HTTP:

- `GET /v1/dgce/stage7/alignment/{section_id}`

SDK:

- `DGCEClient.get_stage7_alignment_read_model(workspace_path, section_id)`

The API and SDK are read-only. Missing, malformed, contract-invalid, or unsafe section IDs return safe read failures and do not repair, generate, or write artifacts.

## Explicit Boundaries

Stage 7 Alignment currently does not perform policy evaluation, simulation validation, execution, Blueprint mutation, Unreal project writes, lifecycle advancement, Stage 8 invocation, Unreal resolver integration, or Code Graph integration.
