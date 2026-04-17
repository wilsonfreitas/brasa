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

**Always use the Linear MCP tools** (`mcp__linear-server__*`) for all communication with Linear — never use the gh CLI or direct API calls.

**Workspace constants** (use these IDs directly — no need to look them up):
- Team: `Wilsonfreitas` — ID `6c3534b0-9f98-476c-a1d6-3f99119e9f88`
- Project: `brasa` — ID `700095cc-dd63-405b-b285-1895aabce09d`
- Labels: `Idea` · `Bug` · `Improvement` · `Feature`
- New issues always go to project **brasa** with status **Backlog** and no priority. The user can update these later during refinement/planning.

---

## Flow 1 — Save an Idea

**Trigger**: user wants to capture an idea, feature request, or bug they thought of.

1. If the user gave only a title, ask for a short description (one paragraph is enough — or proceed without one if they say it's enough as-is).
2. Ask which label fits: **Idea**, **Feature**, **Improvement**, or **Bug**.
   - Default to **Idea** when the user is still exploring and hasn't refined it yet.
   - Suggest the most likely label based on context, but let the user confirm.
3. Create the issue via `mcp__linear-server__save_issue`:
   - `team`: Wilsonfreitas team ID
   - `project`: brasa project ID
   - `status`: Backlog
   - `priority`: no priority (omit the field)
   - `labels`: the chosen label
4. Confirm with the issue identifier (e.g., `WIL-42 created`).

---

## Flow 2 — Refine (Design) an Issue

**Trigger**: user wants to brainstorm and produce a design for an existing issue — "refine WIL-X", "brainstorm WIL-X", "design WIL-X".

Delegate to the `linear-brasa-brainstorm` skill. That skill handles fetching the issue, running the brainstorming flow, saving the `## Design` section, the user review gate, and the label conversion from `Idea` to `Feature` / `Improvement` / `Bug`.

---

## Flow 3 — Plan an Issue

**Trigger**: user wants to write an implementation plan for an already-designed issue — "plan WIL-X", "write the plan for WIL-X".

Delegate to the `linear-brasa-plan` skill. That skill validates the presence of `## Design`, runs the plan-writing flow, and saves the `## Implementation Plan` section.

---

## General Notes

- **Always write issue titles and descriptions in English**, even if the user describes the idea in another language (e.g., Portuguese). Translate the user's input before creating or updating the issue.
- Always confirm destructive or irreversible actions (e.g., marking Done) before executing.
- When the user references an issue by number only (e.g., "WIL-42" or just "42"), look it up via `mcp__linear-server__get_issue`.
- Keep issue descriptions clean markdown — no raw HTML, no escaped characters.
- If the user asks something that doesn't clearly fit one of the three flows, ask a clarifying question rather than guessing.
