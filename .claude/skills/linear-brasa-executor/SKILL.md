---
name: linear-brasa-executor
description: >
  Execute a previously-planned brasa Linear issue end-to-end: read the plan from the
  Implementation Plan Document, work through each step faithfully, tick checkboxes in that Document
  as they complete, maintain an append-only Execution Log comment with Findings, run quality gates,
  run gated Findings triage at close-out, and mark the issue Done. Use whenever the user says
  "execute WIL-X", "run the plan for WIL-X", "implement WIL-X", or otherwise wants to turn a
  planned issue into actual code changes — even if they don't say "execute".
---

# Linear Brasa — Plan Executor

You take an already-planned brasa Linear issue (produced by the `linear-brasa-plan` skill) and
drive it to completion: code changes, Document updates, Findings log, quality gates, gated triage,
and a final Done status.

**Announce at start:** "I'm using the linear-brasa-executor skill to execute the plan for WIL-X."

**Use Linear MCP tools** (`mcp__plugin_linear_linear__*`) for all Linear communication — never `gh`
or raw HTTP. **Run every Python/tooling command through `uv run`** (see `CLAUDE.md`).

**Workspace constants:**
- Team: `Wilsonfreitas` — `6c3534b0-9f98-476c-a1d6-3f99119e9f88`
- Project: `brasa` — `700095cc-dd63-405b-b285-1895aabce09d`

You can trust the plan. It was written deliberately and approved by the user. Your job is faithful
execution, not re-planning. If the plan is genuinely ambiguous on a specific step, ask one targeted
question rather than guessing — but don't second-guess choices that are already made.

---

## Shared Convention A — Document upsert (tick checkboxes in the Plan Document)

The **Implementation Plan Document is the single source of truth for step tracking.** The issue
description is never modified.

```
For each completed step, update the Plan Document in place:
  save_document(id=<Plan doc id>, content=<plan with that step's - [ ] flipped to - [x]>)
NEVER tick checkboxes in the issue description. NEVER call save_issue for step tracking.
```

## Shared Convention B — Status transition (read-back verified, never on abort)

```
1. Only after the skill's main work succeeded:
   save_issue(id=WIL-X, state="<Target>")   # state, NOT status
2. Read the returned JSON; confirm "status":"<Target>" before announcing.
3. If the skill aborts/errs before completing, do NOT move the status.
```

## Shared Convention C — Findings entry format

Whenever something noteworthy surfaces during execution, append to the Execution Log comment:

```
### FINDING <YYYY-MM-DD HH:MM tz> — <kind>
<one line: what was observed, decided, or discovered>
```

Where `<kind>` is one of:
- `observation` — something interesting but not actionable as a new issue
- `candidate-work` — a concrete piece of work that could become a new issue

## Shared Convention D — Gated Findings triage (at close-out only)

```
1. Re-read the Execution Log comment; collect every FINDING tagged "candidate-work".
2. If none, skip. Otherwise present them as a numbered list and ask ONE prompt:
   "Promote which of these to new issues? (e.g. 1,3 / all / none)"
3. For each approved finding:
   save_issue(team=<team>, project=<project>, title=<finding text>,
              state="Backlog", relatedTo=["WIL-X"])
4. Never create an issue without explicit user approval.
```

---

## Phase 1 — Understand the Plan

1. Fetch the issue with `mcp__plugin_linear_linear__get_issue`. Read title and the `documents` array.
2. Look in `documents` for an entry with title exactly `Implementation Plan`:
   - If absent, stop: "Issue WIL-X has no Implementation Plan Document. Run linear-brasa-plan first."
   - If found, call `get_document(id=<that id>)` and store both the id and the full content.
3. Parse the steps from the Document content:
   - Identify the ordered list of **steps** (top-level checklist items, numbered phases, or headed
     sections). A step may contain **sub-steps** — nested checklist items or a sub-list under a
     heading. Sub-steps run sequentially inside their parent step.
   - Identify **acceptance criteria**, if separate from the steps.
4. Build a `TodoWrite` list mirroring the plan's steps (one todo per top-level step). Sub-steps
   live inside the same todo — track them in your head and in the Execution Log.
5. Apply Convention B with `state="In Progress"` (read-back). Do not touch the issue description.
6. Create the Execution Log — see next section.

---

## The Execution Log

The Execution Log is a **single Linear comment** on the issue, updated progressively. One comment,
edited in place — never post new comments for log updates.

**Create it once** at the start of Phase 2, via `save_comment`, with this structure:

```markdown
## Execution log

Started: <YYYY-MM-DD HH:MM timezone>

### Step 1 — <step title>
- [ ] in progress

### Step 2 — <step title>
- [ ] pending

...
```

Save the returned comment ID. Every subsequent update (step completions, Findings, Completion)
calls `save_comment` with that same ID — editing, not appending.

**As each step progresses**, rewrite the relevant section:
- Mark the step complete with `- [x] done` and a one-line note of what actually changed (files
  touched, key decisions, anything surprising — not a blow-by-blow of every edit).
- If a sub-step is non-trivial, list it under the step with its own `- [x]` line.
- If something failed and you had to retry or reroute, say so briefly. The log is a truthful record.
- Append FINDING entries per Convention C whenever noteworthy things surface.

**At the very end**, append a `### Completion` section with gate results and a closing timestamp.

Keep entries terse. The log should fit comfortably on screen — it's a trail, not a transcript.

---

## Phase 2 — Execute the Plan

For each step, in order:

1. Mark the corresponding `TodoWrite` item as `in_progress`.
2. Do the work. If the step has sub-steps, run them sequentially as the plan gives them.
   Follow the plan's file paths, function names, and decisions literally — the plan was written for
   a reason. Note any Findings via Convention C in the Execution Log as they arise.
3. When the step is done:
   - Tick the step's `- [ ]` → `- [x]` in the **Implementation Plan Document** via Convention A
     (`save_document(id=<Plan doc id>, content=<updated content>)`). The issue description is
     never modified.
   - Update the Execution Log comment (edit in place, same id).
   - Mark the `TodoWrite` item `completed`.
4. Move to the next step.

If a step genuinely can't be completed as written (function doesn't exist, file layout has shifted),
stop and tell the user what you found before improvising. A small deviation is fine; a meaningful
one needs a nod.

---

## Phase 3 — Quality Gates (mandatory)

The issue is **not** Done until all three pass. Non-negotiable.

Run in order:

1. **Tests**: `uv run pytest`
2. **Ruff**: `uv run ruff check . && uv run ruff format --check .`
3. **Pre-commit**: `uv run pre-commit run --all-files`

### On failure

Try to fix it. Fixing a real gate failure is faithful execution, not scope creep.

- **Test failures**: diagnose the root cause. Don't weaken assertions or skip tests. If a test is
  legitimately wrong (outdated expectation), fix it deliberately and note it in the Execution Log.
- **Ruff failures**: run `uv run ruff check . --fix` and `uv run ruff format .`, then re-check.
- **Pre-commit failures**: read the hook output, fix the underlying issue, re-stage, re-run. Never
  pass `--no-verify`.

After fixing, rerun the failing gate (and any gate that comes after it) from scratch. Log the
failure and the fix tersely in the Execution Log.

If after a reasonable attempt you can't get a gate green, **stop and report** to the user with the
failing output. Don't mark Done. Don't announce completion. The user will decide how to proceed.

---

## Phase 4 — Close Out

Only reachable after all three gates are green.

1. **Gated Findings triage**: apply Convention D — scan the Execution Log for `candidate-work`
   findings, present them as a numbered list, ask once which to promote, create approved ones as new
   Backlog issues with `relatedTo=["WIL-X"]`. Never create without approval.
2. **Finalize the Execution Log**: append the `### Completion` section with gate results and a
   closing timestamp.
3. **Ensure all Plan Document checkboxes are ticked**: call `get_document(id=<Plan doc id>)` and
   confirm all `- [ ]` items are now `- [x]`. If any remain, tick them via Convention A.
4. **Move to Done**: apply Convention B with `state="Done"`. Verify the returned JSON shows
   `"status":"Done","statusType":"completed"` before announcing. The description is not modified.
5. Announce verbatim: **"I'm done with this gig!"**
   (English rendering of "Terminei essa budega!" — keep the playful tone.)
6. Share the issue URL.

---

## General Notes

- Issue content is **written in English**, even when the user speaks Portuguese.
- The Execution Log is a **single comment updated in place**. If you accidentally create a second
  log comment, delete the stray one with `delete_comment`.
- The **Implementation Plan Document is the only place checkboxes are ticked** — never modify the
  issue description to track progress.
- The issue description (intro + any existing Design section) is **read-only** throughout execution.
- If the Plan Document has no checkboxes and is just prose, treat its ordered headings or paragraphs
  as steps — the Execution Log structure doesn't depend on the plan using checkboxes.
- If the user interrupts mid-execution, pause the current step, answer, and only resume once they
  confirm. Keep the log honest about the interruption if it affected the work.
