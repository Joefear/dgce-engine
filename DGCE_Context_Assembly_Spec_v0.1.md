# DGCE_Context_Assembly_Spec_v0.1.md

**Defiant Industries — Internal Specification**
**Version:** 0.1
**Status:** Authoritative — Additive to DGCE_Alignment_v1.2
**Classification:** Confidential Internal Reference | Not for Distribution

---

## 01 // Purpose

The Context Assembly subsystem is responsible for transforming a project ingestion document into a validated, session-ready input package for DGCE.

It is the system that makes the following lifecycle assumption true:

> *"Structured design document received. Section validated against schema."*
> — DGCE_Alignment_v1.2, Stage 1

Without Context Assembly, DGCE has no guaranteed mechanism to:

- Validate ingestion documents against the contract
- Resolve volatile sections safely
- Produce a deterministic session input

---

## 02 // Scope and Architectural Position

**Lifecycle Position:**

Context Assembly is **Stage 0** of DGCE. It executes before any lifecycle stage and is a prerequisite for Stage 1.

```
Stage 0 — Context Assembly
→ Stage 1 — Input (validated ingestion document received)
→ Stage 2 — Preview
→ Stage 3 — Review
→ ...
```

**Responsibilities:**

- Validate ingestion document against DGCE_INGESTION_CONTRACT
- Resolve all `[VOLATILE][INJECTED]` sections from registered sources
- Enforce machine-parse guarantees defined in the contract
- Produce a deterministic session context package
- Fail closed on invalid input

**Non-Responsibilities:**

- Does NOT generate code
- Does NOT modify policy
- Does NOT execute tools beyond registered read-only sources
- Does NOT call models
- Does NOT bypass Guardrail
- Does NOT enforce governance — it extracts and surfaces it only

---

## 03 // Inputs

### Primary Input

A project ingestion document conforming to:

```
DGCE_INGESTION_CONTRACT_v0.3
```

The document must be UTF-8 encoded, section-ordered, and structurally complete before assembly begins. The assembler does not repair malformed input.

### Secondary Inputs

Registered volatile source providers, which may include:

- Code Graph (dcg.facts.v1) — structural facts and file inventory
- Guardrail audit outputs — active policy slice state
- DGCE artifact logs — last run summary and execution history
- Git read-only queries — changed files since last session

All secondary inputs are optional. Their absence triggers warnings, not failures.

---

## 04 // Pipeline Definition

Context Assembly executes the following deterministic pipeline. Steps execute in order. No step may be skipped.

---

### Step 1 — Structural Validation

Validate the ingestion document against DGCE_INGESTION_CONTRACT_v0.3:

- All required sections present and non-empty
- All required fields present within each required section
- Section headings match the canonical pattern (`## [N] — [Name]`, em dash U+2014)
- Section IDs are integers and sequential
- Machine markers, if present, are consistent with section classification
- Document encoding is UTF-8, no BOM

**Failure behavior:** Any structural violation that maps to a hard failure condition in the contract is a hard stop. Assembly does not continue. A validation report is produced identifying the specific failure. Stage 1 does not execute.

---

### Step 2 — Section Classification Resolution

Identify and verify the classification of every section present in the document:

- `[DURABLE][HUMAN]` — content must be human-authored prose, not placeholder markers
- `[VOLATILE][INJECTED]` — content must consist of placeholder markers only, not manually authored prose

Verify:

- No manually authored content exists in volatile sections
- No required durable sections are empty or contain only whitespace

**Failure behavior:** A volatile section containing manually authored content is a validation warning — content is flagged as potentially stale and the section is treated as requiring injection. A required durable section that is empty is a hard failure carried forward from Step 1.

---

### Step 3 — Volatile Resolution

For each `[VOLATILE][INJECTED]` section present in the document, resolve placeholder fields using registered sources only.

**Resolution rules:**

- Invoke each field's registered source via `resolve_source(source_id)`
- Apply read-only constraint — no source may execute write operations, API mutations, or filesystem modifications
- Apply determinism constraint — each source must produce identical output given identical system state

**Per-field failure behavior:** If a registered source is unavailable or returns an error, log a warning, mark the field as absent, and continue. Do not halt assembly for a single source failure.

**Full section failure behavior:** If all volatile sources for Section 7 (Current State) fail or produce no usable content, the assembler treats Section 7 as absent rather than invalid. Assembly continues with a warning logged. The resulting session package proceeds in single-source reasoning mode, using durable design intent only. This condition degrades planning precision at Stage 2 (Preview) and Stage 7 (Alignment) but does not block Stage 1 execution. The single-source reasoning mode condition must be recorded in the validation report and the assembly metadata.

---

### Step 4 — Merge

Combine durable content and resolved volatile content into a single unified document:

- Durable sections carried forward unchanged
- Volatile sections replaced with resolved content, or marked absent if resolution produced no content
- Document structure and section ordering preserved
- No new content introduced by the assembler

The merged document is the resolved ingestion document. It is the primary payload of the session context package.

---

### Step 5 — Governance Extraction

Extract the content of Section 5 (Policy and Constraints) into a structured governance context object:

```
governance_context = {
  permitted_scope,
  prohibited_scope,
  constraints,
  out_of_scope
}
```

This extraction is performed as a read operation only. The governance context is:

- Extracted as a governance handoff surface intended for future Guardrail session governance
- No Guardrail wiring, enforcement, interpretation, or policy-pack generation is defined in v0.1
- Packaged alongside the resolved ingestion document in the session context package
- Available to future subsystems that implement the Guardrail session governance handshake

The governance context must not be modified, interpreted, or acted upon by the assembler.

---

### Step 6 — Output Construction

Construct the session context package from assembled components:

```
session_context_package = {
  ingestion_document_resolved,
  governance_context,
  validation_report,
  assembly_metadata
}
```

See Section 05 for full output specification.

---

### Step 7 — Final Validation

Verify the session context package is complete and internally consistent before releasing it to Stage 1:

- All required sections present in resolved ingestion document
- No structural violations in merged document
- Encoding valid throughout
- Schema consistent with DGCE_INGESTION_CONTRACT_v0.3
- Validation report populated and well-formed
- Assembly metadata present

**Failure behavior:** Any inconsistency detected at this step is a hard stop. The package is not released to Stage 1. A final validation failure indicates an assembler-level defect, not an input defect, and must be logged separately from ingestion validation failures.

---

## 05 // Output Specification

### Output: Session Context Package

The session context package is the sole output of Stage 0. It is the input to Stage 1.

**Required contents:**

- `ingestion_document_resolved` — fully merged document with durable content intact and volatile sections resolved or marked absent
- `governance_context` — extracted Section 5 structure (see Step 5)
- `validation_report` — complete record of all validation results, warnings, and failure conditions
- `assembly_metadata` — operational fields describing the assembly process

**Assembly metadata definition:**

Assembly metadata may include operational assembly-time fields for local process observability — such as source registry version used, ingestion contract version validated against, and single-source reasoning mode flag. Such fields are not part of the resolved ingestion document and must not be persisted into deterministic DGCE lifecycle artifacts. Assembly metadata is process-scoped, not artifact-scoped.

**Output guarantees:**

- Deterministic given identical inputs and identical source states
- No side effects produced during construction
- Fully parseable by Stage 1
- Complete or not released (fail-closed)

---

## 06 // Registered Source Registry

All volatile sources must be explicitly registered before assembly. Unregistered sources cannot be invoked. There is no mechanism for ad-hoc or freeform source execution.

### Registry Entry Definition

Each registered source must declare:

```
source_id        — unique identifier, immutable once registered
description      — human-readable description of what the source provides
data_contract    — expected output format and schema
execution_method — how the source is invoked (filesystem read, Code Graph query, etc.)
determinism_guarantee — documented basis for determinism claim
```

### Allowed Source Types

- Code Graph queries (dcg.facts.v1 contract)
- Filesystem reads (read-only, path-bounded)
- DGCE artifact reads (.dce/ workspace, read-only)
- Git read-only queries (log, diff --name-only, ls-files)
- Guardrail audit reads (read-only output files)

### Disallowed Source Types

- Arbitrary shell command execution
- Network mutations or API posts
- Filesystem write operations
- Non-deterministic sampling or random output
- Live user input prompts
- Any source not present in the registered source registry

### Invocation Model

Sources are invoked only via the registered source interface:

```
resolve_source(source_id)
```

The `!backtick` freeform execution pattern that inspired this design is explicitly not the implementation model. Sources in DGCE Context Assembly are governed and registered, not freeform and arbitrary.

---

## 07 // Error Handling

### Hard Failures — Assembly Stops

The following conditions are hard failures. Assembly stops immediately. Stage 1 does not execute. A validation report is produced.

- Required section missing from ingestion document
- Required section present but empty
- Required field missing within a required section
- Section heading format invalid and unparseable
- Machine marker present and contradicts section classification
- Document encoding is not UTF-8
- Final validation inconsistency detected (assembler-level defect)

### Warnings — Assembly Continues

The following conditions produce warnings logged to the validation report. Assembly continues.

- Optional section absent
- Volatile section partially injected (some fields resolved, some absent)
- All volatile sources for Section 7 failed (single-source reasoning mode activated)
- Volatile section contained manually authored content (flagged as potentially stale)
- Section heading format deviated but was parseable
- Section ordering deviated from contract definition
- Individual source resolution failure

### Validation Report Structure

```
validation_report = {
  status: PASS | FAIL,
  contract_version: "DGCE_INGESTION_CONTRACT_v0.3",
  errors: [
    { section_id, field, condition, severity: HARD }
  ],
  warnings: [
    { section_id, field, condition, severity: WARN }
  ],
  section_results: {
    [section_id]: { status, classification_verified, volatile_resolved }
  },
  reasoning_mode: FULL | SINGLE_SOURCE
}
```

---

## 08 // Governance Handoff Surface

The governance handoff surface is defined here to make its intended future behavior explicit without implementing or committing to any specific wiring in v0.1.

**What it is:**

Section 5 of the ingestion document is a structured governance declaration authored by the project operator. It describes permitted scope, prohibited scope, hard constraints, and out-of-scope declarations for this project's generation session.

**What the assembler does with it:**

The assembler extracts Section 5 content into a `governance_context` object and includes it in the session context package. This is a read and package operation only.

**What the assembler does not do:**

- Does not validate Section 5 content against any Guardrail schema
- Does not generate a Guardrail policy pack
- Does not call Guardrail
- Does not enforce any constraint declared in Section 5
- Does not interpret permitted or prohibited scope

**Intended future behavior (not implemented in v0.1):**

The `governance_context` object is designed to be consumed by Guardrail session governance in a future implementation phase. The structure of `governance_context` — permitted scope, prohibited scope, constraints, out-of-scope — corresponds to the shape of a Guardrail policy evaluation context. When that wiring is implemented, it will be defined in a separate specification. This document reserves the surface and must not be modified to remove it.

---

## 09 // Relationship to DGCE Architecture

| Component | Relationship |
|---|---|
| DGCE Stage 1 | Receives the resolved session context package as its structured input |
| Guardrail | Governance handoff surface reserved for future session governance wiring |
| Code Graph | Registered source for volatile resolution — structural facts and file inventory |
| Itera | May consume Session Notes in a future advisory phase — not defined in v0.1 |
| DGCE_INGESTION_CONTRACT_v0.3 | Normative schema this subsystem validates against |
| DGCE_Alignment_v1.2 | Parent architecture document — this subsystem is additive to it |

---

## 10 // Determinism Guarantees

Context Assembly must be fully deterministic. Given identical inputs and identical source states, it must produce identical output every time.

**Determinism requirements:**

- Resolved ingestion document content is a pure function of input document and source outputs
- No random values, UUIDs, or non-anchored timestamps appear in the resolved document
- Section ordering in output matches section ordering in input
- Source invocation order is stable and defined by the registered source registry
- Assembly metadata fields are process-scoped only and do not affect the resolved document

**What determinism does not mean:**

Determinism does not require that volatile section content be identical across sessions. Volatile content reflects current system state, which changes between sessions by design. Determinism means that given the same system state at assembly time, the same resolved document is produced. The resolved document is a snapshot, not a static artifact.

---

## 11 // What This Subsystem Is Not

- **Not a skill execution system.** It does not execute freeform shell commands or agent skills.
- **Not a prompt builder.** It does not construct LLM prompts. It produces a structured session package.
- **Not an agent runtime.** It does not reason, plan, or make decisions. It assembles and validates.
- **Not a policy engine.** It extracts governance context. It does not enforce it.
- **Not a replacement for Guardrail.** Guardrail remains the policy authority. This subsystem surfaces governance intent for Guardrail to consume in a future phase.
- **Not a code generator.** It produces no code and writes no files into the project workspace.

It is a deterministic context construction system. Its job is to make Stage 1's input assumption true, every time, without exception.

---

## 12 // Version and Change Control

This specification is versioned independently of DGCE_Alignment and DGCE_INGESTION_CONTRACT.

**Breaking changes** (require major version increment):

- Changes to pipeline step order or step addition/removal
- Changes to session context package structure
- Changes to registered source invocation model
- Changes to hard failure conditions
- Changes to governance handoff surface structure

**Non-breaking changes** (minor version increment):

- Adding new registered source types to allowed list
- Adding new warning conditions
- Clarifying existing behavior without changing outcomes
- Adding fields to assembly metadata

---

*Defiant Industries // Confidential Internal Reference // Not for Distribution*

*DGCE_Context_Assembly_Spec_v0.1 — Drafted April 2026*

*Additive to DGCE_Alignment_v1.2. Defines Stage 0 of the DGCE lifecycle.*

*Normative schema reference: DGCE_INGESTION_CONTRACT_v0.3*