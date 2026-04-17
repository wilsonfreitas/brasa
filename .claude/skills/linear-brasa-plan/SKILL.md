---
name: linear-brasa-plan
description: >
  Produce an implementation plan from a brasa Linear issue's existing `## Design` section and
  save it as `## Implementation Plan` inside the same issue's description. Wraps
  superpowers:writing-plans but persists to Linear instead of a `.md` file. Use whenever the user
  says "plan WIL-X", "write the plan for WIL-X", or otherwise wants to turn an already-designed
  issue into an actionable plan.
---

# Linear Brasa — Plan (Plan → Issue)

Take an already-designed brasa Linear issue (one whose description contains a `## Design`
section) and produce a bite-sized implementation plan in a `## Implementation Plan` section of
the same issue.

**Announce at start:** "I'm using the linear-brasa-plan skill to write the plan for WIL-X."

**Always use the Linear MCP tools** (`mcp__plugin_linear_linear__*`).

**Workspace constants:**
- Team: `Wilsonfreitas` — `6c3534b0-9f98-476c-a1d6-3f99119e9f88`
- Project: `brasa` — `700095cc-dd63-405b-b285-1895aabce09d`

---

## Phase 1 — Preconditions

1. Require `WIL-X`. If missing, ask once.
2. Fetch the issue via `mcp__plugin_linear_linear__get_issue`.
3. Split the description by level-2 headers. Locate `## Design`.
   - If `## Design` is absent, stop and tell the user: "Issue WIL-X has no Design section. Run
     the linear-brasa-brainstorm skill first."
4. If `## Implementation Plan` already exists:
   - Show its current contents verbatim.
   - Ask: "Issue already has an Implementation Plan. Re-plan and overwrite it? (y/n)".
   - Exit if the user declines.

---

## Phase 2 — Plan (delegated)

Invoke the `superpowers:writing-plans` skill, passing the extracted contents of `## Design` as
the input spec (as if it were the contents of a spec file). Follow its full flow:
- Scope check (decomposition if the spec is too broad).
- File-structure mapping.
- Bite-sized tasks (2–5 minute steps) with exact file paths, complete code, exact commands, and
  the no-placeholders rule.
- Mandatory plan header (`# ... Implementation Plan` with `Goal`, `Architecture`, `Tech Stack`).

**Override the "Save plan" step.** Do NOT write to `docs/superpowers/plans/*.md`. Instead:

1. Fetch the current issue description.
2. Using the Section Update Rules below, replace or insert the `## Implementation Plan` section
   with the approved plan content.
3. Save via `mcp__plugin_linear_linear__save_issue(id=WIL-X, description=<new>)`.

Then run the superpowers plan self-review: re-read `## Design` and `## Implementation Plan` from
the issue, verify spec coverage, scan for placeholders, verify type/name consistency across
tasks. Fix inline via `save_issue` if needed.

---

## Phase 3 — Close Out

Do NOT offer the "Subagent-Driven vs. Inline Execution" choice from the base writing-plans skill,
and do NOT auto-invoke the executor. End with exactly this message:

> ✅ Plan finalized in WIL-X. When you're ready to execute, use linear-brasa-executor.

Share the issue URL.

---

## Section Update Rules

When modifying the issue description:

1. Split the current description into blocks separated by level-2 headers (`^## `).
2. Identify the target header (`## Implementation Plan`).
3. If it exists, replace the block from that header up to (but not including) the next `## `
   header, or to end-of-description if no next header exists.
4. If it does not exist, insert the new `## Implementation Plan` block at the canonical
   position: `<intro>` → `## Design` → `## Implementation Plan` → `## Execution Log`. Preserve
   the intro and the existing `## Design` block exactly.
5. Never modify content outside the `## Implementation Plan` block.

---

## General Notes

- Write plan content in **English**.
- The plan's checkboxes must use `- [ ]` syntax so the executor can tick them off.
- If the user interrupts mid-plan, pause and resume only on their confirmation.
- If the design turns out to require decomposition (multiple independent subsystems), stop and
  tell the user rather than writing a multi-feature plan into a single issue.
