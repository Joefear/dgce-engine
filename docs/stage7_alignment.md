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

## Resolver Enrichment

Stage 7 may use the Light Unreal Symbol Resolver as optional bounded enrichment when a valid persisted resolver output is already available under `.dce/plans/`. The resolver is optional, resolver enrichment is never required, and resolver enrichment never bypasses Stage 6 Gate.

Resolver absence results in `resolver_used=false` and `enrichment_status=not_used`. Missing, unavailable, malformed, invalid, or incomplete resolver output is ignored as enrichment and does not crash the lifecycle; Stage 7 continues deterministically from its baseline structured inputs.

When resolver evidence is used, Stage 7 records only bounded metadata:

- Exact resolved symbols can add bounded resolver evidence and set `resolver_used=true`.
- Candidate matches create informational `symbol_resolution_conflict` drift and do not block lifecycle advancement.
- Unresolved symbols create blocking `symbol_resolution_conflict` drift and block before Stage 7.5 and Stage 8.

Resolver evidence must not include raw symbol tables, raw resolver payloads, raw file contents, model/provider text, Blueprint graph data, binary Blueprint payloads, or unbounded blobs. The resolver does not perform policy evaluation, simulation validation, Unreal project mutation, Blueprint mutation, Unreal project writes, Blueprint asset parsing, or binary Blueprint parsing.

## Code Graph Enrichment

Stage 7 may use valid `dcg.facts.v1` Code Graph facts as optional bounded enrichment when those facts are already present on the section input and pass the existing Code Graph contract validation. Code Graph is never required, never bypasses Stage 6 Gate, and is not policy authority.

Code Graph absence, unavailability, malformed facts, invalid facts, or facts without usable bounded Stage 7 context result in `code_graph_used=false`. If resolver enrichment is also absent, `enrichment_status=not_used`; otherwise resolver enrichment status remains governed by the resolver behavior above.

When Code Graph evidence is used, Stage 7 records only bounded metadata:

- `source=code_graph`
- a bounded deterministic reference
- an optional deterministic `snippet_hash`

Code Graph enrichment may only contribute to existing drift codes: `insertion_point_invalid`, `structure_mismatch`, `missing_expected_artifact`, `unexpected_artifact`, and `dependency_mismatch`. Blocking Code Graph drift can block lifecycle only through those existing drift codes. Informational Code Graph drift does not block lifecycle advancement, and the legacy compatibility view exposes only blocking `drift_findings`.

Code Graph evidence must not include raw `dcg.facts.v1` payloads, full graphs, unbounded file contents, raw file bodies, policy outcomes, Blueprint graph data, binary Blueprint payloads, or write endpoints.

## Reserved Drift Codes

The contract reserves these drift codes. The baseline builder generates the first three listed in Builder Behavior, and resolver enrichment may generate `symbol_resolution_conflict` only:

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

Stage 7 Alignment currently does not perform policy evaluation, simulation validation, execution, Blueprint mutation, Unreal project writes, lifecycle advancement, Stage 8 invocation, mandatory resolver execution, mandatory Code Graph execution, Code Graph mutation, full graph storage, or raw `dcg.facts.v1` persistence.

## Final Lock Declaration v0.2

Stage 7 Alignment is implemented and locked through Resolver Enrichment v0.1 and Code Graph Enrichment v0.1.

Stage 7 is now complete for the current Phase 6 alignment scope.

Locked lineage:

- Previous Stage 7 lock declaration: `c765255`
- Latest Code Graph enrichment commit: `2daf330`
- Stage 7 Alignment Contract Schema v1.0: `47c2a4a`
- Stage 7 Alignment Record Builder v0.1: `21b05c0`
- Stage 7 Artifact Persistence + Read Model v0.1: `529c222`
- Stage 7 Read API + SDK v0.1: `d73452b`
- Stage 7 Docs + Fixtures + Contract-Lock Expansion: `07fd535`
- Stage 7 Resolver Enrichment v0.1: `7ecaf7d`
- Stage 7 Resolver Enrichment docs/contract-lock: `5349d49`

Canonical artifact:

```text
.dce/execution/alignment/{section_id}.alignment.json
```

Canonical schema:

```text
packages/dgce-contracts/schemas/alignment/alignment_record.v1.schema.json
```

Read API route:

```text
GET /v1/dgce/stage7/alignment/{section_id}
```

SDK helper:

```text
DGCEClient.get_stage7_alignment_read_model
```

Locked declaration:

- Legacy lifecycle compatibility view is preserved separately from the canonical v1 artifact.
- Resolver enrichment is optional bounded enrichment.
- Code Graph enrichment is optional bounded non-authoritative enrichment.
- Code Graph absence, invalid facts, or malformed facts do not block lifecycle.
- Code Graph remains non-authoritative and does not bypass Stage 6 Gate.
- Code Graph does not modify dcg.facts.v1.
- Code Graph evidence is bounded and does not store raw facts.
- Code Graph evidence is bounded and does not store full graphs.
- Code Graph evidence is bounded and does not store file contents.
- Code Graph evidence is bounded and does not store policy outcomes.
- Stage 7.5 remains unchanged.
- Stage 8 remains unchanged.
- Stage 7 blocks before Stage 7.5 and Stage 8 when `execution_permitted` is `false`.
- informational drift does not block lifecycle.
- legacy drift_findings expose blocking-only drift.
- Resolver evidence is bounded and does not store raw symbol tables or raw resolver payloads.
- Code Graph evidence is bounded and does not store raw `dcg.facts.v1` payloads or full graphs.
