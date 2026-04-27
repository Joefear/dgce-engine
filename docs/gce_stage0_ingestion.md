# GCE Stage 0 Ingestion

This document summarizes the current GCE Stage 0 ingestion chain. It is a developer reference for the implemented contract boundary only.

## Input Paths

`GCEIngestionCore` accepts two structured input paths:

- `formal_gdd`: a formal GDD-shaped input with a `document` payload.
- `structured_intent`: a structured intent input with an `intent` payload.

Both paths require `contract_name`, `contract_version`, `input_path`, `metadata`, and `ambiguities`. The body must contain `session_objective` and `sections`.

## Normalized Output

Valid `formal_gdd` and `structured_intent` inputs normalize to one `GCESessionIntent` shape before Stage 1. The normalized intent includes metadata, the session objective, sections, section classifications, and an unblocked `stage_1_release` marker.

## Clarification

If a GCE input includes unresolved ambiguity, Stage 0 returns a deterministic `clarification_request` and blocks Stage 1. The blocked package is not released until a structured `GCEClarificationResponse` resolves the requested fields.

`GCEClarificationResponse` must include:

- `contract_name: GCEClarificationResponse`
- `contract_version: gce.clarification_response.v1`
- `source_clarification_request_fingerprint`
- `operator_response` metadata
- structured `resolved_fields`

The response produces a new `structured_intent` input only after it validates through the existing GCE ingestion validator.

## Persistence

GCE Stage 0 packages persist under:

```text
.dce/input/gce/{source_id}.{input_path}.stage0.json
```

Persisted packages carry the canonical `artifact_fingerprint` used by existing DGCE artifact conventions.

## Release Gate

The GCE Stage 0 release gate emits `stage0_release_result`. Stage 1 is allowed only when:

- the package is a valid GCE Stage 0 package
- the package is not blocked
- validation passed
- `normalized_session_intent` is present
- `clarification_request` is absent
- persisted artifact fingerprint is present and valid when reading from disk

All other states fail closed.

## Non-Goals

Natural-language parsing is not implemented. Free-form natural language cannot bypass Stage 0 and cannot directly write code, files, assets, graphs, or release a GCE session intent to Stage 1.

This chain does not implement Unreal generation, Blueprint/C++ strategy selection, Unreal symbol resolution, Blueprint graph validation, simulation engines, or Code Graph requirements.
