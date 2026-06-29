---
name: linear-brasa-brainstorm
description: >
  Refine a brasa Linear issue via brainstorm → grill → synthesize, persisting the result as a
  `Design` Document on the issue (not a description section). Moves the issue to `Refined` on
  success. Use whenever the user says "refine WIL-X", "brainstorm WIL-X", "design WIL-X", or
  wants to turn a brasa Linear issue into a reviewed design spec.
---

# Linear Brasa — Brainstorm (Design Document → Refined)

Take an existing brasa Linear issue and produce a reviewed design spec stored as a **Document**
titled `Design` attached to the issue. Sequences brainstorm → grill → synthesize so the final
artifact is adversarially stress-tested before being written.

**Announce at start:** "I'm using the linear-brasa-brainstorm skill to design WIL-X."

**Always use the Linear MCP tools** (`mcp__plugin_linear_linear__*`) — never `gh` or raw HTTP.

**Workspace constants:**
- Team: `Wilsonfreitas` — `6c3534b0-9f98-476c-a1d6-3f99119e9f88`
- Project: `brasa` — `700095cc-dd63-405b-b285-1895aabce09d`
- Labels: `Idea` · `Bug` · `Improvement` · `Feature`

---

## Shared Convention A — Document upsert (never create duplicates)

MCP cannot delete a Document. Always upsert:

```
1. get_issue(id=WIL-X) → read documents: [{id, title}, ...]
2. Find the entry whose title EXACTLY equals "Design".
3. If found:      save_document(id=<that id>, content=<new content>)   # update in place
   If not found:  save_document(issue=WIL-X, title="Design", content=...)  # create once
4. NEVER call save_document(issue=...) when "Design" already exists in documents.
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

1. Require the user to provide a `WIL-X` identifier. If missing, ask once: "Which issue? (e.g., WIL-42)".
2. Fetch the issue via `mcp__plugin_linear_linear__get_issue`. Read title, description, current label,
   and the `documents` array.
3. Look in `documents` for an entry with title exactly `Design`:
   - If found: call `get_document(id=<that id>)`, show its content verbatim to the user, then ask:
     "Issue already has a Design Document. Re-brainstorm and overwrite it? (y/n)". Exit on decline.
   - If not found: proceed.
4. Briefly summarize the issue title + description (one or two sentences) so both of you share context.

---

## Phase 2 — Brainstorm → Grill → Synthesize

### 2a — Brainstorm (diverge)

Invoke the `superpowers:brainstorming` skill. Follow its flow for exploration and candidate
generation:
- Explore project context (files, docs, recent commits).
- Clarifying questions, one at a time.
- 2–3 candidate approaches with trade-offs and a recommendation.

**Stop brainstorming before its "Write design doc" step.** Do not write to `docs/superpowers/specs/`.
The brainstorm's job ends when the user picks a candidate approach.

### 2b — Grill (stress-test)

Invoke the `superpowers:grilling` skill on the chosen approach. Grill adversarially — one question
at a time — until both parties share a solid understanding of the design and its failure modes.

**Grilling writes nothing.** Its output is shared understanding only, not a document.

### 2c — Synthesis (mandatory write step)

After grilling, write the agreed design into the **Design Document** using Convention A:

1. Call `get_issue(WIL-X)` to get the current `documents` list.
2. Apply Convention A (upsert) with target title `Design` and the synthesized content.
3. Announce: "Design Document saved."

This synthesis step is the only place the Design artifact is produced — brainstorm and grill
produce no persistent output.

---

## Phase 3 — Spec Self-Review

After saving the Document:

1. Call `get_document(id=<Design doc id>)` to read back exactly what was saved.
2. Scan for: TBD · TODO · placeholder text · internal contradictions · ambiguous requirements.
3. Fix any issues found via `save_document(id=<Design doc id>, content=<corrected>)`.

**Success condition:** a `Design` Document exists on WIL-X with approved, reviewed content.
Nothing has been written to `docs/superpowers/specs/` or to the issue description.

---

## Phase 4 — User Review Gate

Tell the user:
> "Design Document saved on WIL-X. Review it in Linear and let me know if you want changes."

Wait for their response:
- If they request changes: `get_document(Design id)`, edit the content, `save_document(id=...)`,
  re-run Phase 3 self-review, ask for approval again.
- If they approve: proceed to Phase 5.

---

## Phase 5 — Label Conversion

1. Check the issue's current label (from the last `get_issue` call).
2. If label is `Idea`:
   - Ask: "What label should this become now — Feature, Improvement, or Bug?"
   - Call `save_issue(id=WIL-X, labels=[chosen_label])`.
3. If label is already `Feature`, `Improvement`, or `Bug`: leave it unchanged.

Confirm to the user: "Label updated to [label]."

---

## Phase 6 — Status Transition to Refined

Apply Convention B with `state="Refined"`:

1. Call `save_issue(id=WIL-X, state="Refined")`.
2. Read the returned JSON; confirm `"status":"Refined"` before announcing.
3. If the call fails or returns a different status, report the error and do NOT announce Refined.

Close out with exactly:
> ✅ Design finalized in WIL-X (status: Refined). When you're ready to create the plan, ask.

Share the issue URL.

---

## General Notes

- Write issue content in **English**, even if the user speaks Portuguese.
- Keep markdown clean — no raw HTML, no escape sequences.
- If the user interrupts mid-brainstorm or mid-grill, pause, answer, and resume only on their
  confirmation. Note the interruption in context but do not write it to the Document.
- If the design exceeds what fits comfortably in a single issue, flag it to the user rather than
  splitting silently — they may want to decompose into sub-projects (separate issues).
