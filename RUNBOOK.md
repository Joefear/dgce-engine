# DGCE Runbook

## Identity

DGCE operates a governed generation workflow inside DCE. Guardrail remains authoritative, Aether controls execution, and DGCE does not bypass lifecycle controls.

## Lifecycle Overview

DGCE is operated as a 9-stage path:

1. Input: structured section intent enters the system.
2. Preview: deterministic preview artifact is prepared for review.
3. Review: operator examines the proposed section output.
4. Approval: explicit approval state is recorded.
5. Preflight: execution eligibility is validated.
6. Gate: execution gate confirms the section may proceed.
7. Alignment: approved mode and run alignment are checked.
8. Execution: generation work is performed under Aether control.
9. Outputs: validated artifacts and controlled writes are recorded.

Execution happens in the Execution stage only. Files are written only through the controlled write path after execution output has passed validation.

## Governed Generation

In practice, governed generation means:

- DGCE generates only within the approved section scope.
- Guardrail decisions are not bypassed.
- Aether controls routing and execution.
- validation happens before any write
- outputs are recorded as artifacts, not ad hoc edits
- `stub` remains the default provider path; `claude` may be used only when explicitly configured
- function-stub writes are trace-bound with a bounded execution fingerprint; this is not a cross-run cache or deduplication system

## How to Run Tests

Run the full test suite:

```bash
python -m pytest tests
```

Run the focused execution-slice test:

```bash
python -m pytest tests/test_model_execution_slice.py
```

## How to Verify Outputs

- confirm the target file matches the approved section scope
- confirm the execution artifact exists under `.dce/execution/`
- confirm the output artifact exists under `.dce/outputs/` when execution succeeds
- confirm `lifecycle_trace.json` reflects the expected stage progression
- confirm invalid model output did not produce a write

## Failure Modes

- validation failure: execution output is rejected and no governed write should occur
- provider-side execution failure: config, transport, or pre-output execution fails before raw model output is obtained
- validation-side execution failure: raw model output is obtained but strict validation rejects it
- guardrail failure: the run is blocked or routed for review before execution
- preflight failure: the section is not execution-eligible and must not proceed
- execution records may include bounded `provider_request_context`; it is audit-safe and excludes prompts, outputs, and secrets
- execution records may include bounded `execution_timing` for the model path; this is not a full tracing or observability system
- function-stub execution records include a bounded `model_execution_basis_fingerprint`; it is a consistency trace for the governed model path, not a prompt or payload log
- validated function-stub output is canonicalized in a bounded way before fingerprinting and write; this is formatting normalization only, not semantic rewriting

## What NOT to do

- do not write files directly
- do not skip lifecycle stages
- do not manually edit persisted DGCE artifacts
