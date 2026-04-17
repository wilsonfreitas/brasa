---
name: linear-brasa-executor
description: >
  Execute a previously-planned Linear issue for the brasa project end-to-end: read the plan from the
  issue, work through each step/phase/task faithfully, maintain an "Execution log" comment on the
  issue, check off items in the plan's checklist as they complete, and only mark the issue Done after
  all tests, ruff, and pre-commit pass. Use this skill whenever the user says things like "execute
  WIL-X", "run the plan for issue X", "implement WIL-X", "carry out this issue", "let's do WIL-X",
  or otherwise wants to turn a planned Linear issue into actual code changes — even if they don't say
  the word "execute".
---

# Linear Brasa — Plan Executor

You take an already-planned Linear issue (produced by the `linear-brasa-plan` skill) and drive it
to completion: code changes, log updates, quality gates, and a final Done status.

**Announce at start:** "I'm using the linear-brasa-executor skill to execute the plan for WIL-X."

**Use Linear MCP tools** (`mcp__plugin_linear_linear__*`) for all Linear communication — never `gh`
or raw HTTP. **Run every Python/tooling command through `uv run`** (see `CLAUDE.md`).

**Issue description layout.** After the `linear-brasa-brainstorm` / `linear-brasa-plan` integration,
every planned issue's description follows this canonical order:

```
<intro>
## Design
## Implementation Plan
```

The executor reads steps **only** from `## Implementation Plan` and checks off boxes **only** inside
that section. The intro and `## Design` are never modified. The canonical `## Execution Log` slot
mentioned in the integration spec is **reserved for a future migration** — this skill deliberately
keeps the Execution log as a single pinned comment (see below), not as a description section.

**Workspace constants** (same as `linear-brasa`):
- Team: `Wilsonfreitas` — `6c3534b0-9f98-476c-a1d6-3f99119e9f88`
- Project: `brasa` — `700095cc-dd63-405b-b285-1895aabce09d`

You can trust the plan. It was written deliberately and approved by the user. Your job is faithful
execution, not re-planning. If the plan is genuinely ambiguous on a specific step, ask one targeted
question rather than guessing — but don't second-guess choices that are already made.

---

## Phase 1 — Understand the Plan

1. Fetch the issue with `mcp__plugin_linear_linear__get_issue`. Read the full description.
2. Locate the `## Implementation Plan` section by splitting the description on level-2 headers
   (`^## `). If the section is absent, stop and tell the user: "Issue WIL-X has no Implementation
   Plan section. Run the linear-brasa-plan skill first." Do not improvise a plan from `## Design`
   or from prose.
3. Parse the structure **inside `## Implementation Plan` only**:
   - Identify the ordered list of **steps** (top-level checklist items, numbered phases, or headed
     sections). A step may contain **sub-steps** — nested checklist items or a sub-list under a
     heading. Sub-steps run sequentially inside their parent step.
   - Identify the **acceptance criteria**, if separate from the steps.
3. Build a `TodoWrite` list mirroring the plan's steps (one todo per top-level step). Sub-steps can
   live inside the same todo — you'll track them in your head and in the Execution log, not as
   separate todos, so the list stays readable.
4. Move the issue to **In Progress** via `mcp__plugin_linear_linear__save_issue` (status only; don't touch
   the description yet).
5. Create the Execution log — see next section.

---

## The Execution Log

The Execution log is a **single Linear comment** on the issue that you update progressively as you
work. One comment, edited in place — not many comments. This keeps the issue history clean and gives
the user (and future-you) a single place to see what actually happened during execution.

**Create it once** at the start of Phase 2, via `mcp__plugin_linear_linear__save_comment`, with this exact
structure:

```markdown
## Execution log

Started: <YYYY-MM-DD HH:MM timezone>

### Step 1 — <step title>
- [ ] in progress

### Step 2 — <step title>
- [ ] pending

...
```

Save the returned comment ID. Every subsequent update is a `save_comment` call passing that same ID
so you're editing, not appending new comments.

**As each step progresses**, rewrite the relevant section of the log:
- Mark the step complete with `- [x] done` and a one-line note of what actually changed (files
  touched, key decisions, anything surprising — not a blow-by-blow of every edit).
- If a sub-step is non-trivial, list it under the step with its own `- [x]` line.
- If something failed and you had to retry or reroute, say so briefly. The log is a truthful record,
  not a highlight reel.

**At the very end**, append a `### Completion` section with the results of the final quality gates
(tests / ruff / pre-commit) and the closing timestamp.

Keep entries terse. The log should fit comfortably on screen — it's a trail, not a transcript.

---

## Phase 2 — Execute the Plan

For each step, in order:

1. Mark the corresponding `TodoWrite` item as `in_progress`.
2. Do the work. If the step has sub-steps, run them sequentially in the order the plan gives them.
   Follow the plan's file paths, function names, and decisions literally — the plan was written for
   a reason.
3. When the step is done:
   - Update the issue **description**: check off the corresponding `- [ ]` items **inside the
     `## Implementation Plan` section only** (turn them into `- [x]`) via
     `mcp__plugin_linear_linear__save_issue`. The intro, `## Design`, and any other section must be
     preserved byte-for-byte — don't rewrite, reformat, or "improve" them.
   - Update the **Execution log** comment (edit the existing comment, don't post a new one).
   - Mark the `TodoWrite` item `completed`.
4. Move to the next step.

If a step genuinely can't be completed as written (e.g., the plan references a function that turns
out not to exist, or a file layout has shifted since the plan was written), stop and tell the user
what you found before improvising. A small deviation is fine; a meaningful one needs a nod.

---

## Phase 3 — Quality Gates (mandatory)

The issue is **not** Done until all three pass. This matches the Definition of Done in `CLAUDE.md`
and is non-negotiable.

Run in order:

1. **Tests**: `uv run pytest`
2. **Ruff**: `uv run ruff check . && uv run ruff format --check .`
3. **Pre-commit**: `uv run pre-commit run --all-files`

### On failure

Try to fix it. You're allowed — and expected — to make additional edits to get the gates green. The
plan assumed these would pass; fixing a real failure is part of executing the plan faithfully, not
scope creep.

- **Test failures**: diagnose the root cause. Don't weaken assertions or skip tests to make them
  pass. If a test is legitimately wrong (outdated expectation), fix it deliberately and note it in
  the Execution log.
- **Ruff failures**: run `uv run ruff check . --fix` and `uv run ruff format .`, then re-check.
- **Pre-commit failures**: read the hook output, fix the underlying issue, re-stage, re-run. Never
  pass `--no-verify`.

After fixing, rerun the failing gate (and any gate that comes after it) from scratch. Log the
failure and the fix tersely in the Execution log — seeing "tests failed, fixed X, green on retry" is
valuable context for the user.

If after a reasonable attempt you can't get a gate green, **stop and report** to the user with the
failing output. Don't mark the issue Done. Don't announce completion. The user will decide how to
proceed.

---

## Phase 4 — Close Out

Only reachable after all three gates are green.

1. Finalize the **Execution log** comment: add the `### Completion` section with gate results and a
   closing timestamp.
2. Update the issue via `mcp__plugin_linear_linear__save_issue`:
   - Ensure all plan checklist items in the description are checked off.
   - Set `status` to **Done**.
3. Announce to the user, verbatim: **"I'm done with this gig!"**
   (This is the English rendering of the user's requested "Terminei essa budega!" — keep the
   playful tone; don't substitute a formal phrasing.)
4. Share the issue URL.

---

## General Notes

- Issue descriptions and comments are **written in English**, matching the `linear-brasa` skill's
  convention — even if the user talks to you in Portuguese.
- The Execution log is a **single comment updated in place**, not a thread. If you accidentally
  create a second log comment, delete the stray one with `mcp__plugin_linear_linear__delete_comment`.
- Don't touch the plan's original text in the description beyond checking off its checklist items
  inside `## Implementation Plan`. The intro and `## Design` are never modified. The plan is the
  source of truth for what was agreed; the Execution log is where the story of the execution lives.
- If `## Implementation Plan` has no checklist at all and is just prose, treat its ordered headings
  or paragraphs as the steps and still build an Execution log from them — the log structure doesn't
  depend on the plan using markdown checkboxes.
- If the user interrupts mid-execution to ask a question or change direction, pause the current
  step, answer, and only resume once they confirm. Keep the log honest about the interruption if it
  affected the work.
