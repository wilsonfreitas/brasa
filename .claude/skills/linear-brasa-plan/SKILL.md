---
name: linear-brasa-plan
description: >
  Read the Design Document from a brasa Linear issue and produce an implementation plan stored as
  an `Implementation Plan` Document on the same issue (not a description section). Moves the issue
  to `Planned` on success. Use whenever the user says "plan WIL-X", "write the plan for WIL-X",
  or wants to turn an already-refined issue into an actionable plan.
---

# Linear Brasa — Plan (Implementation Plan Document → Planned)

Take an already-refined brasa Linear issue (one with a `Design` Document) and produce a
bite-sized implementation plan stored as an **Implementation Plan** Document on the same issue.

**Announce at start:** "I'm using the linear-brasa-plan skill to write the plan for WIL-X."

**Always use the Linear MCP tools** (`mcp__plugin_linear_linear__*`).

**Workspace constants:**
- Team: `Wilsonfreitas` — `6c3534b0-9f98-476c-a1d6-3f99119e9f88`
- Project: `brasa` — `700095cc-dd63-405b-b285-1895aabce09d`

---

## Shared Convention A — Document upsert (never create duplicates)

MCP cannot delete a Document. Always upsert:

```
1. get_issue(id=WIL-X) → read documents: [{id, title}, ...]
2. Find the entry whose title EXACTLY equals "Implementation Plan".
3. If found:      save_document(id=<that id>, content=<new content>)              # update in place
   If not found:  save_document(issue=WIL-X, title="Implementation Plan", content=...)  # create once
4. NEVER call save_document(issue=...) when "Implementation Plan" already exists.
```

## Shared Convention B — Status transition (read-back verified, never on abort)

```
1. Only after the skill's main work succeeded:
   save_issue(id=WIL-X, state="<Target>")   # state, NOT status
2. Read the returned JSON; confirm "status":"<Target>" before announcing.
3. If the skill aborts/errs before completing, do NOT move the status.
```

---

## Phase 1 — Preconditions

1. Require `WIL-X`. If missing, ask once: "Which issue? (e.g., WIL-42)".
2. Fetch the issue via `mcp__plugin_linear_linear__get_issue`. Read title and the `documents` array.
3. Look in `documents` for an entry with title exactly `Design`:
   - If absent, stop: "Issue WIL-X has no Design Document. Run linear-brasa-brainstorm first."
   - If found, note the id for Phase 2.
4. Look in `documents` for an entry with title exactly `Implementation Plan`:
   - If found: call `get_document(id=<that id>)`, show its content verbatim, then ask:
     "Issue already has an Implementation Plan Document. Re-plan and overwrite it? (y/n)". Exit on decline.
   - If not found: proceed.

---

## Phase 2 — Write the Plan

### 2a — Feed Design Document into writing-plans

1. Call `mcp__plugin_linear_linear__get_document(id=<Design doc id>)` to retrieve the full design.
2. Invoke `superpowers:writing-plans`, passing the Document content as the input spec. Follow its full flow:
   - Scope check (decomposition if the spec is too broad).
   - File-structure mapping.
   - Bite-sized tasks (2–5 minute steps) with exact file paths, complete code, exact commands, and
     the no-placeholders rule.
   - Mandatory plan header (`# ... Implementation Plan` with `Goal`, `Architecture`, `Tech Stack`).

### 2b — Override the "Save plan" step

**Do NOT write to `docs/superpowers/plans/*.md` or to the issue description.** Instead, once the plan
is approved by the user, apply Convention A to write it to the **Implementation Plan Document**:

1. Re-read the current `documents` list via `get_issue` (in case it changed during planning).
2. Apply Convention A with target title `Implementation Plan`.
3. The plan content must use `- [ ]` checkbox syntax so the executor can tick steps off.

### 2c — Self-review

After saving the Plan Document:

1. Call `get_document(id=<Design doc id>)` — read the Design.
2. Call `get_document(id=<Plan doc id>)` — read the saved Plan.
3. Verify:
   - Spec coverage: every Design requirement maps to a plan task.
   - Placeholder scan: no TBD / TODO / incomplete steps.
   - Name consistency: function names, file paths, and types match across tasks.
4. Fix any issues via `save_document(id=<Plan doc id>, content=<corrected>)`.

Nothing is written to disk. Nothing is modified in the issue description.

---

## Phase 3 — Status Transition to Planned

Apply Convention B with `state="Planned"`:

1. Call `save_issue(id=WIL-X, state="Planned")`.
2. Read the returned JSON; confirm `"status":"Planned"` before announcing.
3. If the call fails or returns a different status, report the error and do NOT announce Planned.

Close out with exactly:
> ✅ Plan finalized in WIL-X (status: Planned). When you're ready to execute, use linear-brasa-executor.

Share the issue URL.

---

## General Notes

- Write plan content in **English**.
- Do NOT offer the "Subagent-Driven vs. Inline Execution" choice from `writing-plans`, and do NOT
  auto-invoke the executor.
- If the user interrupts mid-plan, pause and resume only on their confirmation.
- If the design turns out to require decomposition (multiple independent subsystems), stop and
  tell the user rather than writing a multi-feature plan into a single issue.
