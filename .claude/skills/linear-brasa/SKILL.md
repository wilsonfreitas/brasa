---
name: linear-brasa
description: >
  Manage the Linear workflow for the brasa project: saving ideas as issues, and refining/planning
  issues using EnterPlanMode and updating them in Linear. Use this skill whenever the user mentions
  Linear, saving an idea, refining a ticket, or planning a feature — even if they don't explicitly
  say "Linear" but the context is clearly about brasa development tasks.
---

# Linear Brasa Workflow

You manage the entire development lifecycle for the **brasa** project in Linear.

**Announce at start:** "I'm using the linear-brasa skill to manage the brasa project in Linear."

**Always use the Linear MCP tools** (`mcp__plugin_linear_linear__*`) for all communication with Linear — never use the gh CLI or direct API calls.

**Workspace constants** (use these IDs directly — no need to look them up):
- Team: `Wilsonfreitas` — ID `6c3534b0-9f98-476c-a1d6-3f99119e9f88`
- Project: `brasa` — ID `700095cc-dd63-405b-b285-1895aabce09d`
- Labels: `Idea` · `Bug` · `Improvement` · `Feature`
- New issues always go to project **brasa** with state **Backlog** and no priority. The user can update these later during refinement/planning.

---

## Flow 1 — Save an Idea

**Trigger**: user wants to capture an idea, feature request, or bug they thought of.

1. If the user gave only a title, ask for a short description (one paragraph is enough — or proceed without one if they say it's enough as-is).
2. Ask which label fits: **Idea**, **Feature**, **Improvement**, or **Bug**.
   - Default to **Idea** when the user is still exploring and hasn't refined it yet.
   - Suggest the most likely label based on context, but let the user confirm.
3. Create the issue via `mcp__plugin_linear_linear__save_issue`:
   - `team`: Wilsonfreitas team ID
   - `project`: brasa project ID
   - `state`: "Backlog"
   - `priority`: no priority (omit the field)
   - `labels`: the chosen label
4. Confirm with the issue identifier (e.g., `WIL-42 created`).

---

## Flow 2 — Refine (Design) an Issue

**Trigger**: user wants to brainstorm and produce a design for an existing issue — "refine WIL-X", "brainstorm WIL-X", "design WIL-X".

Delegate to the `linear-brasa-brainstorm` skill. That skill sequences brainstorm → grill →
synthesize, saves the result as a **`Design` Document** on the issue (not a description section),
runs the user review gate, converts the label from `Idea` to `Feature` / `Improvement` / `Bug`,
and moves the issue to **Refined**.

---

## Flow 3 — Plan an Issue

**Trigger**: user wants to write an implementation plan for an already-designed issue — "plan WIL-X", "write the plan for WIL-X".

Delegate to the `linear-brasa-plan` skill. That skill reads the **`Design` Document**, runs the
plan-writing flow, saves the result as an **`Implementation Plan` Document** on the issue (not a
description section), and moves the issue to **Planned**.

---

## Per-Issue Information Architecture

Every brasa issue follows this layout (single issue, no sub-issues):

```
Issue WIL-X
│  description  = original idea text          (write-once, never modified after creation)
│  status       = stage machine (see below)
├─ Document: Design                           (write-once, produced by linear-brasa-brainstorm)
├─ Document: Implementation Plan              (write-once, produced by linear-brasa-plan;
│                                              checkboxes flipped - [ ] → - [x] during execution)
└─ Comment: Execution Log                     (append-only; one comment edited in place;
                                               holds FINDING entries = the audit trail)
```

**Status lifecycle:**
```
Backlog ──(refine)──▶ Refined ──(plan)──▶ Planned ──(execute start)──▶ In Progress ──(gates green)──▶ Done
```

Each status advance is read-back verified and only happens on successful skill completion.

---

## General Notes

- **Always write issue titles and descriptions in English**, even if the user describes the idea in another language (e.g., Portuguese). Translate the user's input before creating or updating the issue.
- Always confirm destructive or irreversible actions (e.g., marking Done) before executing.
- When the user references an issue by number only (e.g., "WIL-42" or just "42"), look it up via `mcp__plugin_linear_linear__get_issue`.
- Keep issue descriptions clean markdown — no raw HTML, no escaped characters.
- If the user asks something that doesn't clearly fit one of the three flows, ask a clarifying question rather than guessing.
