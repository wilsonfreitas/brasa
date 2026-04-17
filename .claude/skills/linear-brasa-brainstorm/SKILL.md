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

Invoke the `superpowers:brainstorming` skill and follow its entire flow:
- Explore project context (files, docs, recent commits).
- Clarifying questions, one at a time.
- 2–3 candidate approaches with trade-offs and a recommendation.
- Incremental design sections with per-section user approval.

**Override the "Write design doc" step.** Do NOT write to `docs/superpowers/specs/*.md`. Instead:

1. Fetch the current issue description with `mcp__plugin_linear_linear__get_issue`.
2. Using the Section Update Rules below, replace or insert the `## Design` section with the
   approved design content.
3. Save via `mcp__plugin_linear_linear__save_issue(id=WIL-X, description=<new>)`.

Then run the superpowers spec self-review loop — read the saved Design from the issue, check for
placeholders, internal consistency, scope, ambiguity. Fix inline by calling `save_issue` again.

---

## Phase 3 — User Review Gate

Tell the user: "Design saved to WIL-X. Review it in Linear and let me know if you want changes."

Wait for their response. If they request changes, apply them and re-run the self-review. Only
continue once they approve.

---

## Phase 4 — Label Conversion

If the issue's current label is `Idea`, ask: "What label should this become now — Feature,
Improvement, or Bug?" Update the label via `mcp__plugin_linear_linear__save_issue`.

If the label is already `Feature` / `Improvement` / `Bug`, leave it alone.

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
