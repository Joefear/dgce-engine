# Defiant Creation Engine (DCE)

## Overview

Defiant Creation Engine (DCE) is a governed AI creation system designed to convert structured design intent into build-ready outputs such as code, scaffolds, and system plans.

The system is built on a core principle:

> AI should be used deliberately, not by default.

DCE minimizes unnecessary inference by prioritizing reuse, deterministic resolution, and controlled execution paths before invoking models.

## Quick Mental Model

- DCE = the full creation engine (what the user interacts with)
- Aether = execution/control layer (decision + routing)
- Guardrail = policy/governance
- Itera = reuse/memory

Think of Aether as the engine, not the product.

---

## Core Architecture

DCE is composed of four primary layers:

### 1. Aether (Execution Engine)

Aether is the core execution layer of DCE.

It is responsible for:

* Task classification
* Routing decisions
* Execution control
* Cost-aware model usage
* Orchestration of the full request lifecycle

Aether ensures:

* No model is called without evaluation
* The cheapest valid execution path is selected
* All actions are traceable

---

### 2. Guardrail (Governance Layer)

Guardrail enforces policy and safety constraints.

Responsibilities:

* Policy evaluation (allow / block / review / escalate)
* Risk and security enforcement
* Compliance checks
* Decision auditability

Guardrail is authoritative.
Aether cannot override Guardrail decisions.

---

### 3. Itera (Reuse & Memory Layer)

Itera handles reuse and inference avoidance.

Responsibilities:

* Exact-match reuse
* Artifact storage
* Promotion (experimental → approved)
* Reuse eligibility enforcement

Rules:

* Only approved artifacts are reused
* Experimental outputs are isolated
* Reuse is deterministic and scoped

---

### 4. Creation Layer (DGCE / SNIPER Workflows)

This is the top-level creation system.

Responsibilities:

* Accept structured design inputs
* Decompose into build tasks
* Execute tasks through Aether
* Aggregate results into build artifacts

Two primary workloads:

#### DGCE (Defiant Game Creation Engine)

* Game systems
* Modules
* Scaffolding
* Code generation pipelines

#### Defiant Sky / SNIPER

* System analysis
* Mission planning
* Constraint-aware design
* High-governance decision flows

---

## Execution Flow

1. Input (structured design section)
2. Deterministic task decomposition
3. Aether classification
4. Guardrail policy evaluation
5. Itera reuse check
6. Execution (local or model)
7. Structured artifact generation
8. File plan generation
9. Scaffold generation (filesystem output)
10. Optional promotion for reuse

---

## Design Principles

### 1. Inference is the last resort

Reuse and deterministic resolution are always preferred.

### 2. Governance is mandatory

All actions are subject to Guardrail evaluation.

### 3. Reuse is explicit and controlled

Only approved outputs influence future behavior.

### 4. Determinism where possible

Non-deterministic behavior is minimized unless necessary.

### 5. Internal-first development

The system proves itself by building:

1. DGCE
2. Defiant Sky / SNIPER

Only after this does it move toward external distribution.

---

## Naming Clarification

* **Defiant Creation Engine (DCE)** = full system
* **Aether** = execution/control layer inside DCE
* **Guardrail** = governance layer
* **Itera** = reuse/memory layer

Aether is not the entire system.
It is a critical subsystem within DCE.

---

## Future Phases

### Phase 1 (Current)

* Structured outputs
* File planning
* Scaffold generation

### Phase 2

* File content generation
* Multi-step build chaining

### Phase 3

* Existing filesystem intake
* Change planning
* Incremental builds

### Phase 4

* External packaging
* Multi-tenant support
* Public API and distribution

---

## End Goal

Aether-powered DCE becomes a system that can:

* Take a design document
* Generate a working project
* Improve it over time
* Reuse prior work
* Minimize token usage
* Maintain full auditability

This enables:

* Individual creators
* Startups
* Enterprises

to build software and systems faster, cheaper, and more safely.
