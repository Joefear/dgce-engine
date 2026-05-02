# Light Unreal Symbol Resolver v0.1

The Light Unreal Symbol Resolver is authorized by DGCE Master Handoff Document v1.7 as a narrow, metadata-only Game Adapter slice. Its current role is to resolve requested Unreal-like symbols from already-produced DGCE artifacts before any Stage 2 preview dispatch or Stage 7 alignment integration exists.

## Authorization Scope

The v0.1 resolver is read-only and path-metadata-only. It may consume:

- a validated resolver input contract
- a validated Unreal project structure manifest
- a validated Unreal symbol candidate index

It may produce:

- a validated resolver output contract
- an optional persisted resolver output artifact under `.dce/plans/`
- read-only inspection models over persisted resolver outputs

## Contract-First Sequence

The resolver was built contract-first:

1. Lock the input/output schema.
2. Validate resolver inputs and outputs fail-closed.
3. Implement path-metadata matching against manifest/candidate artifacts only.
4. Persist resolver outputs only as preview-safe plan artifacts.
5. Expose persisted outputs through read-only read API, HTTP GET routes, and SDK GET helpers.

No runtime Stage 2 or Stage 7 integration has been added.

## Resolver Input Contract

The input artifact uses:

- `artifact_type`: `game_adapter_unreal_symbol_resolver_input`
- `contract_name`: `DGCEGameAdapterUnrealSymbolResolver`
- `contract_version`: `dgce.game_adapter.unreal_symbol_resolver.v1`
- `adapter`: `game`
- `domain`: `game_adapter`
- `source_manifest_fingerprint`
- `source_candidate_index_fingerprint`
- exactly one of `requested_symbols` or `requested_targets`
- `allowed_symbol_kinds`
- `stage_usage`: `Stage2Preview`, `Stage7Alignment`, or `both`

Inputs reject raw file content, raw model/provider text, binary Blueprint payloads, graph/node data, and execution/write directives.

## Resolver Output Contract

The output artifact uses:

- `artifact_type`: `game_adapter_unreal_symbol_resolver_output`
- `contract_name`: `DGCEGameAdapterUnrealSymbolResolver`
- `contract_version`: `dgce.game_adapter.unreal_symbol_resolver.v1`
- `adapter`: `game`
- `domain`: `game_adapter`
- `source_input_fingerprint`
- `resolved_symbols`
- `unresolved_symbols`
- `resolution_status`
- `integration_points`
- `artifact_fingerprint`

Output validation verifies deterministic shape and fingerprint.

## Path-Metadata Behavior

The resolver only matches bounded metadata from the candidate index:

- `candidate_name`
- `candidate_kind`
- `source_path`

It never opens Unreal project files and never reads or interprets file contents. Exact target path aliases can produce `exact_path_match`; symbol-name matches produce `candidate_match`.

## Manifest And Candidate Dependencies

The resolver depends on:

- `DGCEGameAdapterUnrealProjectStructureManifest`
- `DGCEGameAdapterUnrealSymbolCandidateIndex`

The resolver input fingerprints must match the provided manifest and candidate index artifacts. The candidate index must also point back to the same manifest fingerprint. Any mismatch fails closed.

## Resolution Status

`resolution_status` is one of:

- `resolved`: all requests resolved
- `partially_resolved`: at least one request resolved and at least one unresolved
- `unresolved`: no requests resolved
- `input_invalid`: reserved by the locked output contract for invalid-input output artifacts

## Symbol Shapes

Resolved symbols are metadata-only:

- `symbol_name`
- `symbol_kind`
- `source_path`
- `resolution_method`: `path_metadata`
- `confidence`: `exact_path_match` or `candidate_match`

Unresolved symbols are also bounded:

- `symbol_name`
- `symbol_kind`
- `source_path`: `null`
- `resolution_method`: `path_metadata`
- `confidence`: `unresolved`

## Persistence Path

Resolver outputs may be persisted with:

`.dce/plans/unreal-symbol-resolver*.resolution.json`

The default artifact path is:

`.dce/plans/unreal-symbol-resolver.resolution.json`

Persistence validates the locked output contract before and after writing, using existing DGCE artifact fingerprint conventions.

## Read Surfaces

Persisted resolver outputs are exposed through read-only inspection surfaces:

- Python read API:
  - `list_game_adapter_unreal_symbol_resolver_outputs`
  - `get_game_adapter_unreal_symbol_resolver_output`
- HTTP GET routes:
  - `/v1/dgce/game-adapter/unreal-symbol-resolutions`
  - `/v1/dgce/game-adapter/unreal-symbol-resolutions/{artifact_name}`
- SDK GET helpers:
  - `list_game_adapter_unreal_symbol_resolver_outputs`
  - `get_game_adapter_unreal_symbol_resolver_output`

Detail reads verify `artifact_fingerprint` and validate the locked output contract. Missing, malformed, invalid-fingerprint, or contract-invalid artifacts return deterministic read-error models.

## Explicit Non-Goals

This slice does not include:

- Stage 2 preview dispatch integration
- Stage 7 alignment integration
- Unreal file parsing
- Blueprint binary parsing
- Blueprint graph inspection
- Blueprint graph validation
- Stage 8 execution
- Blueprint mutation
- C++ writes
- Unreal project writes
