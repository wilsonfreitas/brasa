---
name: linear-brasa-brainstorm
description: >
  Brainstorm and save a design spec into a Linear issue for the brasa project. Wraps
  superpowers:brainstorming but persists the final design as a `## Design` section inside the
  target issue's description instead of a `.md` file. Use whenever the user says "refine WIL-X",
  "brainstorm WIL-X", "design WIL-X", or otherwise wants to turn a brasa Linear issue into a
  reviewed design spec.
---

# Linear Brasa — Brainstorm (Design → Issue)

Take an existing brasa Linear issue and produce a reviewed design spec that lives in the
issue's description under a `## Design` section.

**Announce at start:** "I'm using the linear-brasa-brainstorm skill to design WIL-X."

**Always use the Linear MCP tools** (`mcp__plugin_linear_linear__*`) — never `gh` or raw HTTP.

**Workspace constants:**
- Team: `Wilsonfreitas` — `6c3534b0-9f98-476c-a1d6-3f99119e9f88`
- Project: `brasa` — `700095cc-dd63-405b-b285-1895aabce09d`
- Labels: `Idea` · `Bug` · `Improvement` · `Feature`

---

## Phase 1 — Preconditions

1. Require the user to provide a `WIL-X` identifier. If missing, ask once: "Which issue? (e.g., WIL-42)".
2. Fetch the issue via `mcp__plugin_linear_linear__get_issue`. Read title, description, current label.
3. Parse the description by level-2 headers (`## `). If a `## Design` section exists:
   - Show its current contents to the user verbatim.
   - Ask: "Issue already has a Design. Re-brainstorm and overwrite it? (y/n)".
   - If the user declines, stop and exit.
4. Briefly summarize the issue (one or two sentences) so both of you share context.

---

## Phase 2 — Brainstorm (delegated)

Invoke the `superpowers:brainstorming` skill. Follow its entire flow:
- Explore project context (files, docs, recent commits).
- Clarifying questions, one at a time.
- 2–3 candidate approaches with trade-offs and a recommendation.
- Incremental design sections with per-section user approval.

**CRITICAL: Override the "Write design doc" step — this is MANDATORY.**

When `superpowers:brainstorming` reaches its "Write design doc" step (the step where it tries to save the approved design to `docs/superpowers/specs/*.md`), **DO NOT LET IT WRITE TO DISK**. Instead:

### Mandatory Save-to-Linear Flow

**When superpowers:brainstorming says "Writing design doc" or similar:**

1. **Get the approved design content** from superpowers:brainstorming (the full text that was just approved by the user).
2. **Fetch current issue description:**
   ```
   mcp__plugin_linear_linear__get_issue(id=WIL-X)
   ```
3. **Parse description by level-2 headers** — identify `## Design` block (if exists) and `## Implementation Plan` block (if exists).
4. **Construct new description** using Section Update Rules (see below):
   - Keep intro text (above first `##`)
   - Replace or insert `## Design` with approved content
   - Preserve `## Implementation Plan` if it exists
5. **Save to Linear issue:**
   ```
   mcp__plugin_linear_linear__save_issue(
     id=WIL-X,
     description=<new-description-with-design>
   )
   ```

### Spec Self-Review (after save to Linear)

Read the saved `## Design` from the issue and check for:
- Placeholder scan: Any "TBD", "TODO", incomplete sections?
- Internal consistency: Do sections contradict?
- Scope: Focused enough for single plan, or needs decomposition?
- Ambiguity: Any requirement interpretable two ways?

If issues found, fix inline by calling `save_issue` again.

**SUCCESS CONDITION:** `## Design` section exists in Linear issue WIL-X with approved, reviewed content. No files written to `docs/superpowers/specs/`.

---

## Phase 3 — User Review Gate

**After Design is saved to Linear issue WIL-X:**

Tell the user:
> "Design saved to WIL-X. Review it in Linear and let me know if you want changes."

Wait for their response:
- If they request changes: fetch the issue, edit the `## Design` section inline using `save_issue`, re-run spec self-review, ask for approval again.
- If they approve: proceed to Phase 4.

---

## Phase 4 — Label Conversion

After Design is approved:

1. Check the issue's current label from the last `get_issue` call.
2. If label is `Idea`:
   - Ask: "What label should this become now — Feature, Improvement, or Bug?"
   - Call `save_issue(id=WIL-X, labels=[chosen_label])` to update it.
3. If label is already `Feature`, `Improvement`, or `Bug`:
   - Leave it unchanged.

Confirm to the user: "Label updated to [label]."

---

## Phase 5 — Close Out

Do NOT auto-invoke any other skill. End with exactly this message (substituting the identifier):

> ✅ Spec finalized in WIL-X. When you're ready to create the plan, ask.

Share the issue URL.

---

## Section Update Rules

When modifying the issue description:

1. Split the current description into blocks separated by level-2 headers (`^## `).
2. Identify the target header (`## Design`).
3. If it exists, replace the block from that header up to (but not including) the next `## `
   header, or to end-of-description if no next header exists.
4. If it does not exist, insert the new `## Design` block at the canonical position:
   `<intro>` → `## Design` → `## Implementation Plan` → `## Execution Log`. Preserve any intro
   text above the first `## ` header unchanged.
5. Never modify content outside the `## Design` block.

---

## General Notes

- Write issue content in **English**, even if the user speaks Portuguese.
- Keep descriptions clean markdown — no raw HTML, no escape sequences.
- If the user interrupts mid-brainstorm, pause, answer, resume only on their confirmation.
- If the design exceeds what fits comfortably in a single issue, flag it to the user rather than
  splitting silently — they may want to decompose into sub-projects (separate issues).
