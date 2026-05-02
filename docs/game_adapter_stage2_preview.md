# Game Adapter Stage 2 Preview

DGCE Master Handoff Document v1.5 authorizes Game Adapter Stage 2 as a preview-only slice. The current contract lets DGCE turn a released GCE Stage 0 session intent into a deterministic Game Adapter preview artifact for review and Guardrail inspection. It does not authorize execution, Unreal project mutation, Blueprint graph writes, C++ writes, symbol resolution, Blueprint graph validation, or simulation engines.

## Scope

Game Adapter Stage 2 is limited to preview artifact construction and read-only inspection. The adapter may describe bounded planned changes for game-development targets, but it must stop before Stage 8 execution and before any Unreal project write surface.

The current artifact contract is implemented by `aether/dgce/game_adapter_preview.py` and persisted by the isolated dispatch helper in `aether/dgce/game_adapter_stage2_dispatch.py`.

## Preview Contract

The preview artifact uses:

- `artifact_type`: `game_adapter_stage2_preview`
- `contract_name`: `DGCEGameAdapterStage2Preview`
- `contract_version`: `dgce.game_adapter.stage2.preview.v1`
- `adapter`: `game`
- `domain`: `game_adapter`
- `source_stage0_fingerprint` or `source_input_reference`
- `planned_changes`
- `governance_context`
- `machine_view`
- `human_view`
- `artifact_fingerprint`

`machine_view` and `human_view` are both deterministically derived from the same canonical `planned_changes` and `governance_context`. The human view is a bounded tabular representation for game-developer readability; it is not free-form prose and must not expose raw model/provider text.

## Planned Changes

Each `planned_changes` entry must contain:

- `change_id`
- `target`
- `operation`
- `domain_type`
- `strategy`
- `summary`

`target` contains:

- `target_id`
- `target_path`
- `target_kind`

`summary` contains controlled summary codes:

- `intent`
- `impact`
- `risk`
- `review_focus`

Allowed operation values are `create`, `modify`, and `delete`.

Allowed strategy values are `Blueprint`, `C++`, and `both`. This field is only a bounded preview descriptor; it is not an execution mechanism.

Before preview artifact creation, DGCE applies a deterministic preview-only strategy selector. The selector uses only bounded fields already present on each planned change: `domain_type`, `target.target_kind`, `summary.intent`, and `summary.review_focus`. It does not call a model, inspect an Unreal project, resolve symbols, validate Blueprint graphs, or write files.

The current selector rules are:

- `domain_type: C++` with `target_kind: CppClass` selects `C++`.
- `domain_type: Blueprint` with `target_kind: BlueprintClass` selects `Blueprint`.
- `binding`, `asset`, and `input_action` planned changes select `Blueprint` for bounded matching target kinds.
- `component`, `variable`, and `event` planned changes select `Blueprint` for bounded matching target kinds.
- `component`, `variable`, and `event` planned changes with `summary.intent: prepare_for_review` and `summary.review_focus: logic_flow` select `both`.

If a planned change already provides `strategy`, the value must match the selector result. Unknown domain types, unsupported target kinds, ambiguous domain/target combinations, and mismatched explicit strategies fail closed.

Allowed domain and target vocabularies are defined in `aether/dgce/game_adapter_preview.py`. Invalid, unknown, raw, or free-form fields fail closed during contract validation.

## Dispatch Requirements

The isolated Stage 2 dispatch path only builds a Game Adapter preview when all of the following are true:

- the source is a released GCE Stage 0 package,
- Stage 1 release is allowed,
- `normalized_session_intent` is present,
- at least one section declares the Game Adapter domain,
- bounded `planned_changes` are present.

Blocked, ambiguous, malformed, or missing Stage 0 release data fails closed before preview persistence.

## Persistence

The canonical preview persistence path is:

```text
.dce/plans/game-adapter-stage2.preview.json
```

Additional preview IDs are normalized into:

```text
.dce/plans/{preview_id}.preview.json
```

The read model only indexes Game Adapter Stage 2 preview artifacts matching:

```text
.dce/plans/game-adapter-stage2*.preview.json
```

This avoids confusing existing Phase 4 software preview artifacts with Game Adapter previews.

## Read-Only Inspection

The read model exposes bounded preview data through:

- `list_game_adapter_stage2_preview_artifacts(workspace_path)`
- `get_game_adapter_stage2_preview_artifact(workspace_path, artifact_name)`

HTTP exposes GET-only routes:

- `GET /v1/dgce/game-adapter/stage2-preview-artifacts`
- `GET /v1/dgce/game-adapter/stage2-preview-artifacts/{artifact_name}`

The SDK exposes read-only helpers:

- `DGCEClient.list_game_adapter_stage2_preview_artifacts(workspace_path)`
- `DGCEClient.get_game_adapter_stage2_preview_artifact(workspace_path, artifact_name)`

Detail reads verify `artifact_fingerprint` before returning a read model. Missing, malformed, invalid-fingerprint, and contract-invalid artifacts return deterministic read-error payloads.

## Non-Goals

This slice does not implement:

- Stage 8 execution for Game Adapter,
- Blueprint graph mutation,
- C++ file writes,
- Unreal project file writes,
- Unreal symbol resolution,
- Blueprint graph validation,
- simulation engines,
- Guardrail repository changes,
- Code Graph or `dcg.facts.v1` changes,
- new external adapter families,
- raw model/provider text exposure.
