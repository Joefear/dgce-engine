# DGCE Workspace Artifacts v1

This document defines the persisted control-plane metadata written under `.dce/` for DGCE workspace runs.

These artifacts are informational metadata for orchestration, inspection, and tooling. They are not generated source files and are not part of the scaffold/project output written by the executor path.

Inspector note:
- `python -m dgce inspect <project_root>` reads these persisted artifacts in a read-only way.
- Optional focus mode: `python -m dgce inspect <project_root> --section <section_id>`.
- `python -m dgce explain <project_root> --section <section_id>` prints a deterministic read-only explanation for one persisted section run.
- The inspector reads `.dce/workspace_summary.json`, `.dce/advisory_index.json`, `.dce/ownership_index.json`, and `.dce/outputs/{section_id}.json` when section detail is needed.
- The explain helper reads `.dce/outputs/{section_id}.json` only and formats stored facts into a short human-readable summary.
- It does not write new artifacts or recompute runtime state.
- Explain output is a deterministic summary of persisted facts, not generated reasoning.

Top-level workspace artifacts:
- `.dce/outputs/{section_id}.json`
- `.dce/advisory_index.json`
- `.dce/ownership_index.json`

## `.dce/outputs/{section_id}.json`

Purpose:
- Stores the finalized per-section workspace output metadata for the current DGCE run.
- Captures the derived file plan, execution outcome summary, read-only advisory, and observational write transparency.

When written:
- During `WRITE`, after `write_file_plan(...)` completes and the final execution outcome has been updated with the actual written file count.

Top-level fields:
- `section_id`
  - Deterministic section identifier derived from `preflight_section(...)`.
- `run_mode`
  - Descriptive run-mode label for the current workspace run.
- `run_outcome_class`
  - Normalized descriptive outcome classification for the current workspace run.
- `file_plan`
  - Deterministic DGCE file plan for the current run.
- `execution_outcome`
  - Finalized structured run summary.
- `advisory`
  - Read-only Itera advisory object, or `null`.
- `write_transparency`
  - Observational per-path write decisions for the current run's `WRITE` stage.

`run_mode` values:
- `create_only`
  - Default conservative mode. Existing `modify` targets are not overwritten.
- `safe_modify`
  - Opt-in mode. Existing `modify` targets may be directly replaced when the current controlled-write, ownership, and identical-content rules allow it.

`run_outcome_class` values:
- `validation_failure`
- `execution_error`
- `partial_skipped_modify`
- `partial_skipped_ownership`
- `partial_skipped_ignore`
- `partial_skipped_exists_fallback`
- `partial_skipped_identical`
- `success_create_only`
- `success_safe_modify`

Run classification note:
- `run_mode` and `run_outcome_class` are descriptive, traceability-oriented fields in v1.
- They do not carry control authority.
- `run_outcome_class` is derived deterministically from the finalized run facts already present in the artifact.

`execution_outcome` fields:
- `section_id`
  - Section identifier for this run.
- `stage`
  - Final lifecycle stage represented by the outcome summary. v1 currently persists `WRITE`.
- `status`
  - One of `success`, `partial`, or `error`.
- `validation_summary`
  - Structured validation summary for DGCE structured artifacts.
- `change_plan_summary`
  - Counts of `create`, `modify`, and `ignore` actions from the persisted change plan.
- `execution_summary`
  - Counts of written files and skipped write targets.

`validation_summary` fields:
- `ok`
- `error`
- `missing_keys`

`change_plan_summary` fields:
- `create_count`
- `modify_count`
- `ignore_count`

`execution_summary` fields:
- `written_files_count`
- `skipped_modify_count`
- `skipped_ignore_count`
- `skipped_identical_count`
- `skipped_ownership_count`
- `skipped_exists_fallback_count`

`advisory` fields:
- `type`
- `summary`
- `explanation`

`write_transparency` fields:
- `write_decisions`
  - Ordered per-path write decisions for the current run's file plan.
- `write_summary`
  - Aggregated counts derived from `write_decisions`.

Authority note:
- `write_transparency` is the authoritative persisted record of final per-path write decisions for the run.
- Helper pre-checks may be used during planning or filtering, but contract-level write outcomes should be read from this artifact.

`write_decisions` entry fields:
- `path`
  - Relative path from the current run's file plan.
- `decision`
  - `written` or `skipped`.
- `reason`
  - v1/v2.5 reason code: `create`, `modify`, `ignore`, `exists_fallback`, `identical`, or `ownership`.
- `bytes_written`
  - Present only for entries that were actually written.
- `diff_visibility`
  - Present only for modify-written entries where Safe Modify overwrote different content.

Safe Modify note:
- By default, `modify` entries are skipped.
- When Safe Modify v2 is explicitly enabled, `modify` entries may appear as `decision: written` with direct replacement semantics.
- When Safe Modify v2.5 detects exact byte-for-byte equality, a `modify` entry may instead be persisted as `decision: skipped` with `reason: identical`.
- When Safe Modify ownership enforcement blocks an unowned modify path, the entry is persisted as `decision: skipped` with `reason: ownership`.
- When Safe Modify overwrites a different owned modify path, `diff_visibility` may be attached as observational metadata.

`diff_visibility` fields:
- `before_bytes`
- `after_bytes`
- `changed_lines_estimate`

Diff visibility note:
- `diff_visibility` is observational only.
- `changed_lines_estimate` is a lightweight approximate count, not a patch or semantic diff.

`write_summary` fields:
- `written_count`
- `modify_written_count`
- `diff_visible_count`
- `skipped_modify_count`
- `skipped_ignore_count`
- `skipped_identical_count`
- `skipped_ownership_count`
- `skipped_exists_fallback_count`
- `before_bytes_total`
- `after_bytes_total`
- `changed_lines_estimate_total`
- `bytes_written_total`

`file_plan` fields:
- `project_name`
  - Stable DGCE project identifier for the current file plan.
- `files`
  - Ordered list of persisted file-plan entries.

`file_plan.files` entry shape:
- `path`
  - Relative output path for the generated scaffold file.
- `purpose`
  - Short deterministic description of why the file exists.
- `source`
  - DGCE task source that produced the file-plan entry.

Stability expectations for v1:
- Field names above are treated as the stable contract for v1.
- Additional fields should be added cautiously because downstream tooling may inspect this artifact directly.
- Summary wording may evolve, but the structural shape should remain stable within v1.

## `.dce/advisory_index.json`

Purpose:
- Provides a minimal quick-inspection summary for the current section run.
- Mirrors the most useful advisory and outcome signals without requiring callers to read the full per-section output artifact.

When written:
- After `execution_outcome` and `advisory` are finalized for the current run.

Top-level fields:
- `section_id`
- `run_mode`
- `run_outcome_class`
- `status`
- `validation_ok`
- `advisory_type`
- `advisory_explanation`
- `written_files_count`
- `skipped_modify_count`
- `skipped_ignore_count`

Field meanings:
- `section_id`
  - Section identifier for the current run.
- `status`
  - Final outcome status (`success`, `partial`, `error`).
- `run_mode`
  - Descriptive run mode for the current section run (`create_only` or `safe_modify`).
- `run_outcome_class`
  - Normalized descriptive outcome classification for the current run.
- `validation_ok`
  - Boolean validation state derived from `execution_outcome.validation_summary.ok`.
- `advisory_type`
  - Advisory type string, or `null`.
- `advisory_explanation`
  - Advisory explanation tag list, or `null`.
- `written_files_count`
  - Number of files written in the current run.
- `skipped_modify_count`
  - Number of file-plan entries skipped due to `modify` gating.
- `skipped_ignore_count`
  - Number of file-plan entries skipped due to `ignore` gating.

Stability expectations for v1:
- This artifact is intentionally single-run and non-historical.
- v1 overwrites the file on each run; it is not appended and does not aggregate historical entries.
- The listed field names are the stable contract for v1.
- No timestamps or historical aggregation are included in v1.

## `.dce/ownership_index.json`

Purpose:
- Provides a minimal per-run ownership view for files actually written by DGCE.
- Records only current-run write results derived from `write_transparency`.

When written:
- After `write_transparency` is finalized for the current run.

Authority note:
- Safe Modify enforcement reads the ownership index that exists before the current run's writes are finalized.
- The artifact written after the run is the current run's ownership output and does not self-authorize writes during that same run.
- That persisted output becomes part of the enforcement input considered by a later run.

Top-level fields:
- `files`
  - Deterministically sorted list of files actually written in the current run.

`files` entry fields:
- `path`
  - Relative POSIX path for the written file.
- `section_id`
  - Current run section identifier.
- `last_written_stage`
  - Constant `WRITE` in v1.
- `write_reason`
  - `create` or `modify`, derived from `write_transparency`.

Stability expectations for v1:
- The artifact written by the current run is an ownership output artifact, not a same-run authorization source.
- Safe Modify enforcement uses the ownership index that already exists before the current run starts writing.
- v1 overwrites the file on each run and does not retain history.
- Skipped files are not included.
- Files skipped as `identical` are not included.

## `.dce/workspace_summary.json`

Purpose:
- Provides a minimal read-only workspace view across the currently persisted `.dce/outputs/*.json` artifacts.

When written:
- After the current run's outputs, advisory index, and ownership index are finalized.

Top-level fields:
- `total_sections_seen`
- `sections`

`sections` entry fields:
- `section_id`
- `latest_run_mode`
- `latest_run_outcome_class`
- `latest_status`
- `latest_validation_ok`
- `latest_advisory_type`
- `latest_advisory_explanation`
- `latest_written_files_count`
- `latest_skipped_modify_count`
- `latest_skipped_ignore_count`

Field meanings:
- `latest_run_mode`
  - Descriptive run mode copied from the persisted section output artifact.
- `latest_run_outcome_class`
  - Normalized descriptive run outcome classification copied from the persisted section output artifact.

Stability expectations for v1:
- This artifact is informational only.
- It reflects only the currently persisted per-section outputs present in `.dce/outputs/`.
