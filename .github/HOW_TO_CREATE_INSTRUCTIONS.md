# How to Create Instruction Files for AI Agents

This guide explains how to create and configure instruction files that guide GitHub Copilot and other AI agents when working on this project.

## Overview

Instruction files help AI agents understand project-specific patterns, conventions, and best practices. They are particularly useful for:

- Domain-specific knowledge (e.g., template system, financial data processing)
- Project conventions and coding standards
- Common patterns and anti-patterns
- References to detailed documentation

## Directory Structure

```
.github/
├── copilot-instructions.md          # Main instruction file (agent entry point)
├── instructions/                     # Specific instruction files
│   ├── python.instructions.md
│   ├── templates.instructions.md
│   ├── r.instructions.md
│   └── ...
└── HOW_TO_CREATE_INSTRUCTIONS.md    # This file
```

## Step-by-Step Guide

### 1. Identify the Need

Create an instruction file when:
- You have comprehensive documentation (e.g., in `docs/`) that agents should follow
- There are specific patterns or conventions for a subsystem (e.g., templates, parsers)
- You want to ensure consistency when modifying specific file types
- There are common mistakes that need to be avoided

### 2. Create the Instruction File

**Location**: `.github/instructions/`

**Naming Convention**: `<topic>.instructions.md`

Examples:
- `templates.instructions.md` - Template system
- `python.instructions.md` - Python coding standards
- `etl.instructions.md` - ETL pipeline patterns
- `api.instructions.md` - API design guidelines

### 3. Structure Your Instruction File

A well-structured instruction file should include:

```markdown
# [Topic] Instructions

## Overview
Brief description of what this instruction file covers.

## Reference Documentation
Point to comprehensive documentation:
- Link to detailed docs (e.g., `docs/TEMPLATES.md`)
- Related architecture documents
- API references

## Key Principles
Core concepts agents must understand:
- Main patterns to follow
- Design philosophy
- Type system or architecture overview

## Common Patterns
Concrete examples of correct usage:
```yaml/python/etc
# Example code showing the right way
```

## Validation Rules
What makes code valid/invalid:
- Required fields or properties
- Naming conventions
- Type requirements

## Anti-Patterns to Avoid
What NOT to do:
❌ Bad pattern
✅ Good pattern

## When Creating New [Files/Components]
Step-by-step checklist

## When Modifying Existing [Files/Components]
Guidelines for changes

## Additional Resources
Links to related files or documentation
```

### 4. Write Actionable Content

**DO:**
- ✅ Use concrete examples with actual code
- ✅ Reference existing files as examples (`See templates/b3-cotahist.yaml`)
- ✅ Include specific patterns to follow
- ✅ List anti-patterns with explanations
- ✅ Keep it focused and scannable
- ✅ Use checklists and bullet points
- ✅ Link to comprehensive docs for details

**DON'T:**
- ❌ Duplicate entire documentation (reference instead)
- ❌ Use vague descriptions without examples
- ❌ Include implementation details (put those in docs)
- ❌ Make it too long (agents have token limits)

### 5. Register in Main Instructions File

Edit `.github/copilot-instructions.md` to reference your new instruction file.

**Option A: Add to Specific Section**

```markdown
## Template System

For working with YAML template configurations:
- **Templates**: See `.github/instructions/templates.instructions.md`
- **Full specification**: `docs/TEMPLATES.md`
```

**Option B: Add to Instructions List** (if there's a central registry)

At the end of `copilot-instructions.md`:

```markdown
<instructions>
<instruction>
<description>Instructions for working with templates</description>
<file>/home/wilson/dev/python/brasa/.github/instructions/templates.instructions.md</file>
<applyTo>templates/**/*.yaml</applyTo>
</instruction>
</instructions>
```

The `applyTo` field specifies which file patterns trigger this instruction.

### 6. Test the Instructions

1. Open a file that should use the instructions
2. Ask Copilot to implement a feature following the patterns
3. Check if the generated code follows the guidelines
4. Refine the instructions based on results

## Real-World Example

### Scenario: Creating Instructions for Templates

**Step 1**: You have `docs/TEMPLATES.md` with comprehensive template documentation

**Step 2**: Create `.github/instructions/templates.instructions.md`

**Step 3**: Structure with:
- Overview of template system
- Reference to `docs/TEMPLATES.md`
- Key patterns (pipeline-based vs function-based)
- Field type examples
- Common pitfalls
- Anti-patterns

**Step 4**: Add concrete YAML examples showing correct template structure

**Step 5**: Update `.github/copilot-instructions.md`:
```markdown
## Template System

See `.github/instructions/templates.instructions.md` for template guidelines.
Always use pipeline-based templates for new code.
```

**Step 6**: Test by asking Copilot to create a new template

## Best Practices

### Keep Instructions Focused

Each instruction file should cover ONE topic or subsystem:
- ✅ `templates.instructions.md` - Template system only
- ✅ `python.instructions.md` - Python coding standards
- ❌ `everything.instructions.md` - Too broad

### Use Tiered Documentation

1. **Main instructions** (`.github/copilot-instructions.md`): Project overview, tech stack, general guidelines
2. **Topic instructions** (`.github/instructions/*.instructions.md`): Focused, actionable rules for specific subsystems
3. **Comprehensive docs** (`docs/*.md`): Complete specifications, architecture, design decisions

### Reference, Don't Duplicate

If you have detailed documentation in `docs/`:
- ✅ Reference it: "See `docs/TEMPLATES.md` for complete specification"
- ✅ Summarize key points agents need immediately
- ❌ Don't copy entire sections (agents can read both files)

### Make It Scannable

Agents process information quickly:
- Use headings and subheadings
- Use bullet points and checklists
- Highlight critical rules with **bold** or ❌/✅ markers
- Keep paragraphs short (2-3 sentences max)

### Include Negative Examples

Show what NOT to do:
```yaml
# ❌ Don't mix dataset (singular) with datasets (plural)
reader:
  pipeline:
    dataset:      # Wrong: using singular
      datasets:   # but then plural here
        - name: data1

# ✅ Use datasets (plural) consistently
reader:
  pipeline:
    datasets:
      - name: data1
      - name: data2
```

### Update Regularly

When patterns change:
1. Update the comprehensive docs (`docs/`)
2. Update the instruction file (`.github/instructions/`)
3. Update any relevant examples in the codebase
4. Test that agents follow the new patterns

## File Patterns and Application

You can specify when instructions apply using glob patterns:

- `**/*.py` - All Python files
- `templates/**/*.yaml` - All YAML files in templates/
- `brasa/parsers/*.py` - Parser files only
- `**/*.{R,r,Rmd,qmd}` - R and Quarto files

This helps agents load relevant context only when needed.

## Troubleshooting

### Agent Doesn't Follow Instructions

1. Check if instruction file is referenced in `copilot-instructions.md`
2. Ensure examples are concrete and unambiguous
3. Add more "anti-pattern" examples showing what to avoid
4. Make critical rules more prominent (use **bold** or ❌/✅)

### Instructions Too Long

1. Move detailed content to `docs/` and reference it
2. Keep only actionable patterns in instruction file
3. Use "See X for details" links liberally
4. Split into multiple focused instruction files

### Conflicting Instructions

1. Review all instruction files for conflicts
2. Establish hierarchy (main → specific topic → docs)
3. Update conflicting sections to align
4. Add clarification notes where needed

## Templates

### Minimal Instruction File

```markdown
# [Topic] Instructions

## Overview
What this covers.

## Reference Documentation
- Link to detailed docs

## Key Patterns
Pattern 1: Example
Pattern 2: Example

## Anti-Patterns
❌ Don't do this
✅ Do this instead
```

### Comprehensive Instruction File

```markdown
# [Topic] Instructions

## Overview
Detailed description of scope.

## Reference Documentation
- Primary: `docs/DETAILED.md`
- Related: `docs/ARCHITECTURE.md`

## Key Principles
Core concepts and design philosophy.

## Common Patterns
### Pattern 1
Example with code

### Pattern 2
Example with code

## Validation Rules
- Rule 1
- Rule 2

## When Creating New [Component]
Step-by-step checklist

## When Modifying [Component]
Guidelines and gotchas

## Anti-Patterns to Avoid
❌ Bad → ✅ Good examples

## File Locations
Where to find related files

## Additional Resources
Related documentation
```

## Checklist for New Instruction Files

- [ ] File created in `.github/instructions/` with `.instructions.md` suffix
- [ ] Clear topic and scope defined
- [ ] References to comprehensive docs included
- [ ] Concrete code examples provided
- [ ] Anti-patterns documented
- [ ] Checklists for common tasks included
- [ ] Referenced in `.github/copilot-instructions.md`
- [ ] File patterns configured (if using XML format)
- [ ] Tested with actual agent interactions
- [ ] Reviewed for clarity and conciseness

## Examples in This Project

Current instruction files you can reference:

1. **`templates.instructions.md`**
   - Topic: Template system configuration
   - References: `docs/TEMPLATES.md`
   - Applies to: `templates/**/*.yaml`

2. **`python.instructions.md`**
   - Topic: Python coding conventions
   - Applies to: `**/*.py`

3. **`r.instructions.md`**
   - Topic: R language conventions
   - Applies to: `**/*.{R,r,Rmd,qmd}`

4. **`python-mcp-server.instructions.md`**
   - Topic: Building MCP servers with Python SDK
   - Applies to: Python files in MCP server contexts

## Conclusion

Instruction files bridge the gap between comprehensive documentation and agent-ready guidance. They help ensure consistency, reduce errors, and encode institutional knowledge in a format that AI agents can effectively use.

When in doubt:
1. Start with a minimal instruction file
2. Add examples from real code
3. Iterate based on agent behavior
4. Keep it focused and actionable
