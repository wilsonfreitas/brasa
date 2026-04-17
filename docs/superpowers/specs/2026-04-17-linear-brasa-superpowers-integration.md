# Linear-Brasa + Superpowers Integration — Design

**Date:** 2026-04-17
**Status:** Approved for planning

## Goal

Integrate the `linear-brasa` skill with the `superpowers` brainstorming and plan-writing workflows, persisting design (spec) and implementation plan as sections inside a **single Linear issue** rather than as separate `.md` files. This keeps one source of truth per feature, aligned with how small/focused brasa features (1 PR each) are tracked today.

## Architecture

Two new skills **wrap** the superpowers skills, intercepting only the persistence step. The existing `linear-brasa` skill is updated to route refine/plan requests to the new wrappers. No forking of superpowers content — the discipline (checklists, self-review, gates) is reused as-is.

```
linear-brasa                  (orchestrator; Flow 1 save idea; routes refine/plan)
├── linear-brasa-brainstorm   (wraps superpowers:brainstorming → writes ## Design)
└── linear-brasa-plan         (wraps superpowers:writing-plans → writes ## Implementation Plan)

linear-brasa-executor         (already exists; reads ## Design + ## Implementation Plan,
                               writes ## Execution Log)
```

## Issue Description Structure

Every refined issue converges to this canonical layout:

```markdown
<optional intro / original idea text>

## Design
<spec content>

## Implementation Plan
<tasks with `- [ ]` checkboxes>

## Execution Log
<updates from linear-brasa-executor>
```

Section order is canonical: intro → Design → Implementation Plan → Execution Log. Skills preserve this ordering when updating.

## Flow: `linear-brasa-brainstorm`

1. Requires `WIL-X` as argument. Fetch issue via `get_issue`.
2. If `## Design` already present in description → show current content and ask: "Issue already has a Design. Re-brainstorm and overwrite? (y/n)". Abort on no.
3. Invoke `superpowers:brainstorming` and follow its normal flow: explore project context, clarifying questions (one at a time), 2-3 approaches, design sections with per-section approval.
4. **Override the "Write design doc" step.** Instead of saving to `docs/superpowers/specs/*.md`:
   - `get_issue(WIL-X)` to fetch current description
   - Replace or insert the `## Design` section (preserving intro and any existing `## Implementation Plan` / `## Execution Log`)
   - `save_issue` with the new description
5. Run the spec self-review (superpowers checklist: placeholders, consistency, scope, ambiguity), reading from the issue. Fix inline via `save_issue`.
6. User review gate: "Design saved to WIL-X. Review it in Linear and let me know if you want changes."
7. If the issue label is `Idea`, ask the user to choose `Feature` / `Improvement` / `Bug` and update via `save_issue`. (Label conversion happens at brainstorm close, not plan close, because the design defines the nature of the work.)
8. **Terminate.** Do NOT auto-invoke the plan skill. Final message: "✅ Spec finalized in WIL-X. When you're ready to create the plan, ask."

## Flow: `linear-brasa-plan`

1. Requires `WIL-X`. Fetch issue.
2. Validate that `## Design` exists. If not, abort: "Issue has no Design section. Run the brainstorm skill first."
3. If `## Implementation Plan` already exists → show and ask re-plan confirmation (same pattern as brainstorm).
4. Invoke `superpowers:writing-plans`, passing the content of `## Design` as the input spec.
5. **Override the "Save plan" step.** Instead of saving to `docs/superpowers/plans/*.md`:
   - Fetch current description, replace or insert `## Implementation Plan`, `save_issue`.
6. Run the plan self-review against the Design section (spec coverage, placeholders, type consistency). Fix inline.
7. **Terminate.** Do NOT auto-invoke the executor. Final message: "✅ Plan finalized in WIL-X. When you're ready to execute, use linear-brasa-executor."

## Section Update Semantics

Shared logic each skill implements when writing:

- Parse description by level-2 headers (`## `).
- Target section located by exact header match (e.g., `## Design`).
- If target exists → replace from that header up to the next `## ` header (or EOF).
- If target does not exist → insert at the canonical position (Design before Implementation Plan before Execution Log; intro text preserved at top).
- Write back with `save_issue(description=...)`.

Content outside the managed sections (intro, other sections) is never modified.

## Changes to `linear-brasa` (SKILL.md)

- **Flow 1 (Save Idea):** unchanged.
- **Flow 2 (Refine/Plan) — replaced:**
  - "refine WIL-X" → delegate to `linear-brasa-brainstorm`.
  - "plan WIL-X" → delegate to `linear-brasa-plan`.
  - Remove direct use of `EnterPlanMode` and the inline plan-writing instructions from Flow 2 — those responsibilities move into the wrapper skills.
- The existing "General Notes" (English titles, confirm destructive actions, issue-number lookup, clean markdown) remain.

## What Stays From Superpowers Skills

Reused without modification:
- Brainstorming: phase checklist, scope decomposition check, 2-3 approaches with trade-offs, incremental design-section approval, spec self-review loop, user review gate.
- Writing-plans: mandatory header format, file-structure mapping, bite-sized task granularity, no-placeholders rule, self-review checklist.

Only the persistence target changes.

## What We Do Not Do (YAGNI)

- **No worktree.** The superpowers brainstorming skill normally creates one before writing-plans; we don't touch repo files, so it's unnecessary.
- **No versioning of previous sections.** Linear already keeps full issue history in its activity feed. Duplicating versions in the description (`## Design (v1)`) would pollute the artifact the executor reads.
- **No intermediate brainstorming log.** The conversation lives in the Claude Code transcript. The value of `## Design` is being the distilled final artifact. Important context must live inside the design itself, not as an appendix.
- **No support for starting without an issue.** Every brainstorm targets an existing `WIL-X`; idea capture is Flow 1 and remains separate.

## File Layout

```
.claude/skills/
├── linear-brasa/SKILL.md              (updated)
├── linear-brasa-brainstorm/SKILL.md   (new)
├── linear-brasa-plan/SKILL.md         (new)
└── linear-brasa-executor/SKILL.md     (existing, unchanged in this work)
```
