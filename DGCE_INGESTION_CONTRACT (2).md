# DGCE_INGESTION_CONTRACT.md
**Defiant Industries — Internal Specification**
**Version:** 0.3
**Status:** Authoritative — Additive to DGCE_Alignment_v1.2
**Classification:** Confidential Internal Reference | Not for Distribution

---

## 01 // Purpose

This document is the normative schema for a valid DGCE input document. It defines what a project ingestion document must contain, how sections are classified, and what validation rules apply before Stage 1 of the DGCE lifecycle can execute.

The ingestion contract is the artifact that makes Stage 1's assumption true:

> *"Structured design document received. Section validated against schema."*
> — DGCE_Alignment_v1.2, Stage 1

Without a formal ingestion contract, Stage 1 has no defined schema to validate against. This document closes that gap.

---

## 02 // What the Ingestion Contract Is

The ingestion contract is a protocol, not a document format.

A project team authors a human-readable engineering design document following this contract's structure. The result is a standardized, machine-parsable input that the DGCE Context Assembly subsystem can resolve into a Stage-1-ready session package.

The contract governs:
- Which sections are required versus optional
- Which sections contain durable design intent versus volatile live state
- Which sections are human-authored versus injected at assembly time
- Which sections carry governance-relevant content intended for future Guardrail session scope
- What validation behavior applies per section type

---

## 03 // Section Classification System

Every section in a DGCE ingestion document carries one of two primary classifications:

| Classification | Meaning |
|---|---|
| `[DURABLE]` | Content authored by a human. Represents design intent, architectural decisions, rationale, and constraints. Does not change between sessions unless deliberately updated by the author. |
| `[VOLATILE]` | Content resolved at assembly time from live sources. Represents current system state. Injected by the Context Assembly subsystem immediately before Stage 1 execution. Must not be manually maintained. |

Every section also carries one of two authorship markers:

| Authorship | Meaning |
|---|---|
| `[HUMAN]` | Section content is written and maintained by the operator or project author. |
| `[INJECTED]` | Section content is populated by the Context Assembly subsystem from defined live sources. The human-authored template contains placeholder markers, not content. |

These classifications combine. For example: a section marked `[DURABLE][HUMAN]` is written once and updated only by deliberate human edit. A section marked `[VOLATILE][INJECTED]` is never manually edited — it is always resolved fresh at assembly time.

---

## 04 // Required Sections

All required sections must be present and non-empty for a document to pass Stage 1 validation. A missing or empty required section is a hard validation failure. DGCE does not proceed.

---

### Section 1 — Project Identity
`[DURABLE][HUMAN]` | **REQUIRED**

**Purpose:** Establishes the unique identity of this project and its governing context within DGCE.

**Required fields:**
- Project name
- Version (semantic: `v0.1`, `v1.0`, etc.)
- Owner name
- Creation date
- Last updated date
- One-paragraph purpose statement

**Validation rule:** All six fields must be present. Purpose statement must be 1–3 sentences. Version must follow semantic versioning format.

---

### Section 2 — System Architecture
`[DURABLE][HUMAN]` | **REQUIRED**

**Purpose:** Describes the components of the system, their roles, and the rationale behind key design decisions. This is the source of truth for what DGCE is being asked to generate.

**Required content:**
- Named component list with one-sentence role descriptions for each
- Dependency relationships described in prose (not diagram)
- At least one documented design decision with stated rationale

**Validation rule:** Component list must contain at least one entry. Dependency prose must reference at least two components by name. Design decision section must not be empty.

**Note:** This section feeds Stage 7 (Alignment). DGCE uses it to detect drift between proposed generation and approved design. It must be specific enough to make drift detectable.

---

### Section 3 — Module Contracts
`[DURABLE][HUMAN]` | **REQUIRED**

**Purpose:** Defines the interface contract for each module or component named in Section 2. This is what DGCE generates against.

**Required content per module:**
- Module name (must match a component named in Section 2)
- Inputs: expected data types, sources, and format
- Outputs: produced artifacts, data types, and destinations
- Constraints: what this module must not do
- Known limitations: acknowledged gaps or unresolved decisions

**Validation rule:** Every component named in Section 2 must have a corresponding module contract entry. Inputs, outputs, and constraints fields must be non-empty for each entry.

---

### Section 4 — Data Flows
`[DURABLE][HUMAN]` | **REQUIRED**

**Purpose:** Describes how data moves through the system. Establishes the canonical data path that DGCE uses to validate generated code's structural correctness.

**Required content:**
- Primary data flows described in prose, named by source and destination
- Data formats or schemas for each primary flow (type names, JSONL, schema references, etc.)
- Any transformation points where data changes format or structure

**Validation rule:** At least one complete flow must be described (source → transformation → destination). Format references must be named, not described generically.

---

### Section 5 — Policy and Constraints
`[DURABLE][HUMAN]` | **REQUIRED**

**Purpose:** Defines the permitted scope, hard limits, and known constraints that govern what DGCE may generate for this project.

**Required content:**
- Permitted generation scope: what DGCE is explicitly allowed to produce for this project
- Prohibited scope: what DGCE must not generate, with stated reason
- Hard constraints: non-negotiable rules (security boundaries, compliance requirements, data handling rules)
- Out-of-scope declarations: adjacent concerns explicitly excluded from this ingestion

**Validation rule:** Permitted scope and prohibited scope must both be non-empty. Hard constraints must include at least one entry.

**Governance handoff surface:** This section is explicitly designed to map into Guardrail session governance in a future implementation phase. The structure defined here — permitted scope, prohibited scope, hard constraints — corresponds to the shape of a Guardrail policy evaluation context. Authors should treat this section as a governance declaration, not informational prose. Automatic policy-pack generation is not implemented in v0.1. The handoff surface is reserved and must not be removed from this contract in future versions.

---

### Section 6 — Build Priorities
`[DURABLE][HUMAN]` | **REQUIRED**

**Purpose:** Declares the operator's ranked generation objectives for this session. This is the human-authored statement of intent that DGCE executes against.

**Required content:**
- Ranked list of generation objectives (1 = highest priority)
- Known blockers for each objective, if any
- Open decisions that require human judgment before generation can proceed

**Validation rule:** At least one ranked objective must be present. Open decisions must be explicitly listed — an empty open decisions field is valid, but the field must be present.

---

## 05 // Optional Sections

Optional sections are not required for Stage 1 validation to pass. Their absence does not block execution. Their presence enriches the session context available to DGCE.

---

### Section 7 — Current State
`[VOLATILE][INJECTED]` | **OPTIONAL**

**Purpose:** Provides a live snapshot of the project's actual state at the time of session assembly. This is the primary volatile section. It is never manually authored — it is always populated by the Context Assembly subsystem.

**Injected fields (examples — specific sources defined in Context Assembly Spec):**
- File inventory snapshot from Code Graph or filesystem
- Active Guardrail policy slices (if applicable)
- Recent audit output summary
- Git diff since last session (changed files, not content)
- Last DGCE run summary

**Validation rule:** If present, all injected fields must be populated by the assembler. A partially populated Current State section (some fields present, some blank) is a validation warning, not a hard failure. A Current State section with no injected content is equivalent to absent and is treated as absent.

**Note:** This section is the primary input for delta reasoning — DGCE comparing intended design (Sections 2–4) against actual system state. Its absence degrades session quality but does not block execution.

---

### Section 8 — Session Notes
`[DURABLE][HUMAN]` | **OPTIONAL**

**Purpose:** A running human-authored log of decisions made, rationale recorded, and context accumulated across sessions. This is the only freeform section in the contract.

**Content:** Unstructured prose. Date-stamped entries recommended but not enforced.

**Validation rule:** No structural validation. If present, must be non-empty.

**Note:** This section is the one place in the contract where the operator can write freely. It is not used as structured generation input in v0.1. It is preserved in the session package for operator reference. Future advisory systems (including Itera) may consume this section for summarization or pattern detection — that behavior is not defined here and must not be assumed in v0.1 implementations.

---

## 06 // Section Ordering

Sections must appear in the order defined in this contract (Sections 1–8). The Context Assembly subsystem parses sections by heading, not by position, but consistent ordering is required for human usability and for future tooling compatibility.

Heading format for each section must follow this pattern:

```
## [Section Number] — [Section Name]
```

Example: `## 5 — Policy and Constraints`

Deviations in heading format are a validation warning. They do not block execution but will be flagged in the assembly log.

---

## 07 // Machine-Parse Guarantees

The ingestion contract must be parseable deterministically. Human-readable structure is not sufficient alone. The following guarantees are required for any document claiming contract compliance.

**Canonical section identifiers:** Each section's numeric ID is its canonical parse key. The Context Assembly subsystem identifies sections by their numeric ID, not by name string. A section named `## 5 — Policy and Constraints` has canonical key `5`. Name changes are non-breaking. Section IDs are immutable once published — they cannot be reassigned or reused across versions. ID changes are breaking by definition and require a major version increment of this contract.

**Required heading pattern:** Section headings must match this exact pattern:

```
## [N] — [Section Name]
```

Where `[N]` is an integer and `—` is an em dash (U+2014). Hyphens are not equivalent. The assembler treats a hyphen-separated heading as a format deviation and logs a warning.

**Optional machine markers:** Documents may include inline parse markers for tooling compatibility. These are not required in v0.1 but are reserved for future assembler use:

```
<!-- DGCE:SECTION=5 -->
<!-- DGCE:FIELD=permitted_scope -->
<!-- DGCE:VOLATILE -->
```

If present, machine markers must be consistent with the heading they accompany. A marker that contradicts its section's contract classification is a hard validation failure.

**Encoding:** Documents must be UTF-8 encoded. No BOM. No smart quotes in field names or markers.

---

## 08 // Volatile Section Source Safety

All sources used to inject content into volatile sections must satisfy the following safety constraints. These constraints apply to the Context Assembly subsystem and are enforced at the assembler level, not the document level. They are defined here because the contract must guarantee what kind of content volatile sections can contain.

**Read-only sources only.** No volatile injection may execute a command with side effects. Write operations, API mutations, network posts, and filesystem modifications are prohibited at injection time. The assembler is a reader, not an actor.

**Deterministic sources only.** A given source invoked twice against the same system state must produce identical output. Non-deterministic sources (random sampling, timestamp-dependent queries without a fixed anchor, live user input) are prohibited.

**Fail-safe on source failure.** If a volatile source is unavailable or returns an error, the assembler logs the failure and treats the corresponding field as absent. It does not halt assembly. A partial volatile section triggers a warning, not a hard failure.

**No shell free-for-all.** Sources are defined in the Context Assembly Spec and registered explicitly. Arbitrary shell command injection into volatile sections is not permitted. The `!backtick` execution pattern that inspired this design is a useful model — but in the DGCE implementation, sources are governed, not freeform.

---

## 09 // Delta Reasoning Role

Delta reasoning is the core capability that Section 7 (Current State) enables. It is defined here explicitly because it is DGCE's primary architectural differentiator and must not be treated as an incidental feature.

**Definition:** Delta reasoning is DGCE's ability to compare intended design state (Sections 2, 3, and 4) against actual system state (Section 7) before generation begins. The delta — the gap between what the system should be and what it currently is — informs every downstream lifecycle stage.

**How the delta is used:**

| Lifecycle Stage | Delta Input |
|---|---|
| Stage 2 — Preview | Delta determines which files need to be created, modified, or left unchanged |
| Stage 7 — Alignment | Delta detects drift between proposed generation and approved design |
| Stage 8 — Execution | Delta scopes generation to actual gaps, not full regeneration |

**What delta reasoning is not:** It is not a diff tool. It is not a file comparison utility. It is DGCE reasoning over the semantic gap between design intent and structural reality. Two systems can have identical file counts and still have a meaningful delta if the module contracts in Section 3 describe behavior that the current implementation does not satisfy.

**When Section 7 is absent:** Delta reasoning degrades to single-source reasoning. DGCE operates against design intent only, with no knowledge of current state. This is valid but produces less precise generation plans. The absence of Section 7 must be logged in the session package.

---

## 10 // Validation Behavior Summary

| Condition | Behavior |
|---|---|
| Required section missing | Hard failure. Stage 1 does not execute. |
| Required section present but empty | Hard failure. Stage 1 does not execute. |
| Required field missing within a section | Hard failure. Stage 1 does not execute. |
| Optional section absent | Pass. No warning. |
| Volatile section partially injected | Warning logged. Execution continues. |
| Volatile section manually authored | Warning logged. Content flagged as potentially stale. |
| Heading format deviation | Warning logged. Execution continues. |
| Section ordering deviation | Warning logged. Execution continues. |

**Fail-closed posture:** Any hard failure at the ingestion stage stops execution before Stage 1. DGCE does not attempt partial execution against an invalid input. The operator receives a structured validation report identifying the specific failure.

---

## 11 // What This Contract Is Not

- **Not a prompt template.** A DGCE ingestion document is not a freeform prompt. It is a structured engineering artifact that happens to be human-readable.
- **Not a static document.** The volatile sections are designed to be resolved fresh at every session. The document format is stable; the content of volatile sections is not.
- **Not a Guardrail policy file.** Section 5 defines governance intent. It does not replace or duplicate a Guardrail policy pack. The governance handoff surface is reserved for future implementation.
- **Not a replacement for the design process.** DGCE cannot generate good software from a poorly authored ingestion document. The quality of generation is bounded by the quality of input.

---

## 12 // Relationship to DGCE Architecture

| Contract Element | Architecture Touchpoint |
|---|---|
| Full document | Stage 1 Input — validated against this schema |
| Section 2 (System Architecture) | Stage 7 Alignment — drift detection source |
| Section 3 (Module Contracts) | Stage 8 Execution — generation scope definition |
| Section 5 (Policy & Constraints) | Stage 6 Gate — governance handoff surface (future) |
| Section 6 (Build Priorities) | Stage 2 Preview — change plan scope |
| Section 7 (Current State) | Delta reasoning input — design vs. actual state |
| Validation failure | Stage 0 Context Assembly — fail-closed before Stage 1 |

---

## 13 // Version and Change Control

This document is versioned independently of DGCE_Alignment. Changes to the ingestion contract require a version increment and must be evaluated for backward compatibility with existing project ingestion documents.

**Breaking changes** (require major version increment):
- Adding a new required section
- Removing any section
- Changing the classification of a section from optional to required
- Changing a field from optional to required within a section

**Non-breaking changes** (minor version increment):
- Adding new optional sections
- Adding optional fields within existing sections
- Clarifying validation rules without changing their outcome
- Adding injected fields to Section 7

---

*Defiant Industries // Confidential Internal Reference // Not for Distribution*
*DGCE_INGESTION_CONTRACT v0.3 — Drafted April 2026*
*Additive to DGCE_Alignment_v1.2. Supersedes no prior document. Establishes new normative standard.*

---

## 14 // Stage 0 Definition

**Stage 0 — Context Assembly:** Resolves a raw ingestion document into a validated, fully-assembled session input before DGCE lifecycle execution begins. Stage 0 is the gate that makes Stage 1's schema validation assumption true. No lifecycle stage executes before Stage 0 completes successfully.

Stage 0 is defined and specified in full in `DGCE_Context_Assembly_Spec_v0.1.md`. This entry exists here to establish Stage 0 as a named lifecycle component within the ingestion contract's scope.

---

## 15 // Future Schema Formalization Note

Field-level validation in v0.2 is defined in prose. Required fields, formats, and constraints are described per section in natural language. This is intentional for the current phase.

A future version of this contract will include a machine-readable schema companion — JSON Schema or YAML spec — that formally encodes field types, formats, and validation rules for automated tooling. That work is deferred until the Context Assembly subsystem exists and field boundaries are proven stable through use.

**Do not prematurely formalize.** Schema should follow demonstrated stability, not precede it.
