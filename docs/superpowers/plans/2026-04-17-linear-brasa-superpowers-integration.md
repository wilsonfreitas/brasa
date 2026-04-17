# Linear-Brasa + Superpowers Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce two new Claude Code skills (`linear-brasa-brainstorm`, `linear-brasa-plan`) that wrap the superpowers brainstorming/writing-plans flows and persist the design and plan as `## Design` / `## Implementation Plan` sections inside a single Linear issue, and update `linear-brasa` to delegate refine/plan to them.

**Architecture:** Pure skill authoring — no Python code changes. Each new skill is a `SKILL.md` file under `.claude/skills/` that invokes the corresponding superpowers skill and intercepts the persistence step by reading/writing the target issue's description via Linear MCP tools (`mcp__plugin_linear_linear__get_issue` / `save_issue`). Section updates parse the description by level-2 headers and replace or insert the target section while preserving the canonical order (intro → Design → Implementation Plan → Execution Log). Verification is manual end-to-end against a scratch Linear issue; there are no unit tests to write because the artifacts are prompt files executed by the Claude harness.

**Tech Stack:** Markdown (`SKILL.md`), Linear MCP tools, Claude Code Skill system, superpowers plugin skills (`superpowers:brainstorming`, `superpowers:writing-plans`).

**Executor scope note:** `linear-brasa-executor` stays as-is in this plan (it continues to use a single pinned comment for its log). The `## Execution Log` slot mentioned in the spec's canonical layout is reserved for a future executor migration and is not created or touched by the two new skills.

---

## File Structure

- Create: `.claude/skills/linear-brasa-brainstorm/SKILL.md` — wraps `superpowers:brainstorming`; fetches issue, gates re-brainstorm, routes the design output into the `## Design` section of the issue description; closes with a terminal "done" message.
- Create: `.claude/skills/linear-brasa-plan/SKILL.md` — wraps `superpowers:writing-plans`; validates `## Design` presence, gates re-plan, feeds the design as input spec, routes the plan output into the `## Implementation Plan` section; closes with a terminal "done" message.
- Modify: `.claude/skills/linear-brasa/SKILL.md` — replace the inline Flow 2 (Refine/Plan via `EnterPlanMode`) with two delegation entries: "refine WIL-X" → `linear-brasa-brainstorm`, "plan WIL-X" → `linear-brasa-plan`. Keep Flow 1 and General Notes.
- Create: `docs/superpowers/plans/2026-04-17-linear-brasa-superpowers-integration.md` — this plan file (already being written).

No source files, tests, or configs under `brasa/` change.

---

## Task 1: Author `linear-brasa-brainstorm` skill

**Files:**
- Create: `.claude/skills/linear-brasa-brainstorm/SKILL.md`

- [ ] **Step 1: Create the skill directory**

```bash
mkdir -p .claude/skills/linear-brasa-brainstorm
```

- [ ] **Step 2: Write `SKILL.md` with the full skill contents**

Write the file exactly as below:

````markdown
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
````

- [ ] **Step 3: Verify the file is valid**

```bash
ls -la .claude/skills/linear-brasa-brainstorm/SKILL.md
head -10 .claude/skills/linear-brasa-brainstorm/SKILL.md
```

Expected: file exists, starts with `---`, has `name: linear-brasa-brainstorm`.

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/linear-brasa-brainstorm/SKILL.md
git commit -m "feat: add linear-brasa-brainstorm skill (design → Linear issue section)"
```

---

## Task 2: Author `linear-brasa-plan` skill

**Files:**
- Create: `.claude/skills/linear-brasa-plan/SKILL.md`

- [ ] **Step 1: Create the skill directory**

```bash
mkdir -p .claude/skills/linear-brasa-plan
```

- [ ] **Step 2: Write `SKILL.md` with the full skill contents**

Write the file exactly as below:

````markdown
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
````

- [ ] **Step 3: Verify the file is valid**

```bash
ls -la .claude/skills/linear-brasa-plan/SKILL.md
head -10 .claude/skills/linear-brasa-plan/SKILL.md
```

Expected: file exists, frontmatter starts with `---`, `name: linear-brasa-plan`.

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/linear-brasa-plan/SKILL.md
git commit -m "feat: add linear-brasa-plan skill (plan → Linear issue section)"
```

---

## Task 3: Update `linear-brasa` to delegate refine/plan

**Files:**
- Modify: `.claude/skills/linear-brasa/SKILL.md`

- [ ] **Step 1: Replace Flow 2 (Refine/Plan) with delegation entries**

Use the Edit tool to replace the entire `## Flow 2 — Refine / Plan an Issue` block (current
lines 44–59 of the file) with:

```markdown
## Flow 2 — Refine (Design) an Issue

**Trigger**: user wants to brainstorm and produce a design for an existing issue — "refine
WIL-X", "brainstorm WIL-X", "design WIL-X".

Delegate to the `linear-brasa-brainstorm` skill. That skill handles fetching the issue, running
the brainstorming flow, saving the `## Design` section, the user review gate, and the label
conversion from `Idea` to `Feature` / `Improvement` / `Bug`.

---

## Flow 3 — Plan an Issue

**Trigger**: user wants to write an implementation plan for an already-designed issue — "plan
WIL-X", "write the plan for WIL-X".

Delegate to the `linear-brasa-plan` skill. That skill validates the presence of `## Design`,
runs the plan-writing flow, and saves the `## Implementation Plan` section.
```

Exact `old_string` to match (match verbatim, including the trailing `---` that separates Flow 2
from "General Notes"):

```
## Flow 2 — Refine / Plan an Issue

**Trigger**: user wants to think through and plan an existing issue — "refine WIL-X", "let's plan WIL-X", "open issue X".

1. Fetch the issue with `mcp__linear-server__get_issue` to load title, description, and current label.
2. Present a brief summary of what the issue is about.
3. Enter plan mode with `EnterPlanMode`. Work collaboratively with the user to produce a plan that covers:
   - Goal and scope
   - Implementation approach (files, modules, decisions)
   - Acceptance criteria or a checklist of requirements (use markdown checkboxes `- [ ]`)
   - Known risks or open questions (if any)
4. Once the user approves the plan, exit plan mode and update the issue via `mcp__linear-server__save_issue`:
   - Replace the description with the full plan (keep the original idea text as a brief intro if useful)
   - If the label was **Idea**, ask what label it should become now: **Feature**, **Improvement**, or **Bug**. Update it.
   - Do not change the status — leave it as-is.
5. Confirm the update with the issue URL.

---
```

Replace with:

```
## Flow 2 — Refine (Design) an Issue

**Trigger**: user wants to brainstorm and produce a design for an existing issue — "refine WIL-X", "brainstorm WIL-X", "design WIL-X".

Delegate to the `linear-brasa-brainstorm` skill. That skill handles fetching the issue, running the brainstorming flow, saving the `## Design` section, the user review gate, and the label conversion from `Idea` to `Feature` / `Improvement` / `Bug`.

---

## Flow 3 — Plan an Issue

**Trigger**: user wants to write an implementation plan for an already-designed issue — "plan WIL-X", "write the plan for WIL-X".

Delegate to the `linear-brasa-plan` skill. That skill validates the presence of `## Design`, runs the plan-writing flow, and saves the `## Implementation Plan` section.

---
```

- [ ] **Step 2: Verify the edit preserved Flow 1 and General Notes**

```bash
grep -n "^## " .claude/skills/linear-brasa/SKILL.md
```

Expected output (section headers in order):
```
## Flow 1 — Save an Idea
## Flow 2 — Refine (Design) an Issue
## Flow 3 — Plan an Issue
## General Notes
```

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/linear-brasa/SKILL.md
git commit -m "refactor: linear-brasa delegates refine/plan to wrapper skills"
```

---

## Task 4: End-to-end smoke test against a scratch Linear issue

**Files:** None. This task exercises the skills against a throwaway Linear issue.

- [ ] **Step 1: Create a scratch issue in Linear**

Use the Linear web UI (or ask Claude in a fresh session to use `linear-brasa` Flow 1) to create
an issue titled `[SCRATCH] linear-brasa wrapper smoke test` in the brasa project, label `Idea`,
status `Backlog`, with this one-line description:

```
We want a CLI flag `--dry-run` on the download command that prints planned downloads without
executing them.
```

Record the issue identifier (e.g., `WIL-99`).

- [ ] **Step 2: Run the brainstorm skill against the scratch issue**

In a fresh Claude Code session in this repo, tell Claude: `refine WIL-99`.

Expected flow:
- Skill announces itself.
- Fetches the issue, summarizes it.
- No `## Design` yet → proceeds directly into `superpowers:brainstorming`.
- Asks clarifying questions one at a time.
- Presents 2–3 approaches, then the design in sections.
- After your approval, writes `## Design` into the issue description via `save_issue`.
- Runs spec self-review, presents the user-review gate.
- Asks to convert the `Idea` label.
- Ends with the exact message: `✅ Spec finalized in WIL-99. When you're ready to create the plan, ask.`

Verify in the Linear UI that the issue description now has a `## Design` section with the
approved content and nothing else was modified.

- [ ] **Step 3: Run the brainstorm skill a second time to exercise the re-brainstorm gate**

In the same or a fresh session, tell Claude: `refine WIL-99` again.

Expected: the skill detects the existing `## Design`, displays it, and asks whether to
overwrite. Answer `n` — the skill should exit cleanly without touching the issue.

- [ ] **Step 4: Run the plan skill against the scratch issue**

Tell Claude: `plan WIL-99`.

Expected flow:
- Skill announces itself.
- Fetches the issue, confirms `## Design` is present, no `## Implementation Plan` yet.
- Invokes `superpowers:writing-plans` with the `## Design` contents as input spec.
- Produces a plan with the mandatory header (`Goal`, `Architecture`, `Tech Stack`) and
  bite-sized tasks with `- [ ]` checkboxes.
- Writes `## Implementation Plan` into the issue description via `save_issue`.
- Runs plan self-review.
- Ends with the exact message: `✅ Plan finalized in WIL-99. When you're ready to execute, use linear-brasa-executor.`

Verify in the Linear UI:
- `## Design` section content is unchanged byte-for-byte.
- `## Implementation Plan` section now exists below it.
- Section order in description is: intro → `## Design` → `## Implementation Plan`.

- [ ] **Step 5: Run the plan skill a second time to exercise the re-plan gate**

Tell Claude: `plan WIL-99` again. Expect the overwrite confirmation prompt; answer `n` and
verify no changes to the issue.

- [ ] **Step 6: Negative test — plan without design**

Create another scratch issue (`WIL-100`) via Flow 1 with no `## Design` section, then tell
Claude: `plan WIL-100`.

Expected: skill aborts with the message `Issue WIL-100 has no Design section. Run the
linear-brasa-brainstorm skill first.` and makes no changes.

- [ ] **Step 7: Clean up scratch issues**

Either close the scratch issues as `Canceled` via the Linear UI, or leave them closed for
future reference. Do not run `linear-brasa-executor` on them.

- [ ] **Step 8: Commit the plan file itself (if not already committed)**

```bash
git add docs/superpowers/plans/2026-04-17-linear-brasa-superpowers-integration.md
git add docs/superpowers/specs/2026-04-17-linear-brasa-superpowers-integration.md
git commit -m "docs: add spec + plan for linear-brasa superpowers integration"
```

(If one or both files are already committed from earlier work in this session, skip the
corresponding `git add` line.)

---

## Self-Review Notes

**Spec coverage:**
- Architecture (two wrapper skills + updated orchestrator): Tasks 1, 2, 3.
- Issue description canonical layout: enforced via "Section Update Rules" in both skill files
  (Tasks 1 & 2) and verified in Task 4 Step 4.
- Flow `linear-brasa-brainstorm` phases 1–5: Task 1 Step 2 (full content).
- Flow `linear-brasa-plan` phases 1–3: Task 2 Step 2.
- Section update semantics: "Section Update Rules" blocks in both skills.
- Changes to `linear-brasa` (Flow 2 replacement, Flow 1 and General Notes preserved): Task 3.
- YAGNI items (no worktree, no versioning, no intermediate logs, no issue-less entry): reflected
  by absence — none of the three skill files reference them.
- Executor-scope note (executor unchanged): called out in the plan header.
- Re-brainstorm / re-plan gate: Task 1 Phase 1 step 3 and Task 2 Phase 1 step 4; exercised in
  Task 4 Steps 3 and 5.

**Placeholder scan:** no `TBD`/`TODO`/"implement later"/"add error handling" placeholders; all
code blocks contain actual content; exact file paths throughout; exact commands in every shell
step.

**Type/name consistency:** skill names (`linear-brasa-brainstorm`, `linear-brasa-plan`),
section headers (`## Design`, `## Implementation Plan`, `## Execution Log`), Linear MCP tool
names (`mcp__plugin_linear_linear__get_issue`, `mcp__plugin_linear_linear__save_issue`), and
workspace IDs are identical across all tasks and match the spec.
