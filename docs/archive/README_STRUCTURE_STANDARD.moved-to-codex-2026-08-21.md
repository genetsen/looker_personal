> ⚠️ **Superseded — do not use.** This is a frozen snapshot from 2026-08-21, kept only
> per the archive-don't-delete rule. The standard has since evolved (broadened scope,
> refined writing rules, new worked examples). Always use the live, current copy at
> [`~/.codex/README_STRUCTURE_STANDARD.md`](/Users/eugenetsenter/.codex/README_STRUCTURE_STANDARD.md).

# README Content Standard (archived 2026-08-21 snapshot)

**Who this is for:** anyone writing or revising a `README.md` or pipeline guide in
this repository, human or agent.

**What it covers:** the content every pipeline guide must carry, the writing rules
that apply throughout, and a recommended section order to start from.

**Where to go next:** the reference implementation is
[Master Data Model Pipeline v2](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/README_v2.md).
For the operational content specifically, see the
[DCM pipeline guide](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/model/branches/dcm/README_dcm-pipeline.md).

## Why this exists

A reader — or an agent — that cannot find an answer in the guide goes and reads the
source code instead, then guesses. Those guesses are where real mistakes come from.
Every required item below exists because its absence has caused one.

## What is fixed, and what is variable

**Fixed:** the content. Every item in the content contract below must be findable,
and the two writing rules always apply. A guide missing that content is incomplete
however well it is organized.

**Also fixed:** the opening. A table of contents is the first section, and
terminology comes before any diagram or table. A reader must be able to navigate
and understand the vocabulary before meeting anything technical.

**Variable:** everything else. Section names, section count, grouping, and order
should fit the pipeline being documented. A small single-source branch may cover
several content items in one section; the master model needs a section each. Do not
pad a guide with empty headings to match a template, and do not split content
across headings that a reader would rather see together.

## The content contract

Required unless the row says otherwise. Where it lives is up to you.

| Content | Must carry |
|---|---|
| Purpose and endpoint | What the pipeline produces, and a link to its final output object. |
| Terminology | Every specialized term used later, in plain language. |
| Pipeline shape | A visual of the stages — ASCII, Mermaid, or both. |
| Source inventory | Each active source with its purpose, grain, and any warning about using it. |
| Source boundaries | What each source may **not** feed, and the correct path instead. Two sources can describe the same campaign while only one is approved for a given output. |
| Layer responsibilities | What each processing stage is accountable for, in order. |
| Output objects | Which object to use for new work, and which are compatibility or legacy surfaces kept alive for existing consumers. |
| Field semantics | Prefix conventions, the canonical final fields, and any field that must not be summed. |
| Canonical surfaces | A plain statement of what is canonical *today* — workspace, builder, output, runbook. Name the legacy or alternate copies explicitly so they cannot be mistaken for it. |
| Protected surfaces | Anything that must never be written to: read-only sources, user-owned sheets, production tables. |
| Durable warnings | Failure modes that stay true over time. Not volatile counts. |
| Operational commands | The canonical entrypoint and refresh path, as runnable commands, plus what proof each refresh requires. |
| Known gaps | What is expected to be missing, incomplete, or unmatched — stated plainly so it is not rediscovered as a new bug. |
| Usage examples | Runnable queries answering the questions readers actually arrive with. |
| Proof plan | Per work type: who owns the proof and what the minimum proof is. |
| What not to treat as proof | The near-misses that look like verification and are not — dry runs, loader success, schema checks, row counts. |
| Maintenance | The recurring habits that keep the pipeline trustworthy. |
| Troubleshooting | Symptom, what to check, and why that check is the right one. |
| Verification queries | Commands a reader can run to confirm current state instead of trusting the document. |

## The six questions a reader must never have to open code for

A useful self-check. If a guide cannot answer these, it is not finished regardless
of which sections it has.

1. **Which script is canonical**, and which copies are not?
2. **What must never be written to?**
3. **What is the join and the grain** — the exact key, its value format, and what one row represents?
4. **Which field should a consumer read**, and what does it take precedence over?
5. **What is known to be missing or broken?**
6. **How do I verify current state myself?**

## Vocabulary: teach the terms, don't route around them

A reader who is shielded from the real vocabulary stays dependent on someone
translating it. Keep the professional term and make it learnable. Three mechanisms,
in order of preference:

| Mechanism | When | Form |
|---|---|---|
| Term used plainly | The surrounding sentence already explains it | "the placement key is a superset of the package key — it adds program and market" |
| Term + `[aka …]` | A short synonym is all that is needed | "matching rows are added together [aka aggregated]" |
| Term + numbered footnote | The term needs a real definition | "derives a synthetic key[^3]" with `[^3]` defined under `## Definitions` |

**Never replace a term with vague language.** "A short code" instead of "a hash"
costs the reader a word they will meet again in the SQL.

### Which terms earn a footnote

Not "is this jargon?" — that over-footnotes. Ask instead: **does the name tell you
what it does to your data?**

- `view`, `snapshot`, `CTE`, `scheduled query`, `JOIN`, `NULL`, `aggregate` — the name
  is enough. Do not define these.
- `safe cast`, `rebuild`, `hash`, `grain`, `contract`, `synthetic key` — the name hides
  the consequence. `safe cast` sounds harmless but makes bad data look like missing
  data; `rebuild` sounds routine but destroys anything not derivable from source.
  Footnote these, and lead the definition with the consequence.

Use numbered markers (`[^1]`), not named ones, and keep them out of `<summary>` tags —
Markdown inside raw HTML tags renders unreliably.

## Required frontmatter

Open every pipeline doc with YAML frontmatter carrying the facts *about* the pipeline,
separate from the prose explaining it. This is machine-readable, greppable, and does
not age the way sentences do:

```yaml
---
pipeline: TV Estimates
source_type: estimate — not delivery
output: looker-studio-pro-452620.master_stg.data_model_v3
output_grain: source_tv_daily
source_tables: [ … ]
refresh: scheduled query, daily 10:15 UTC
loader_script: none
verified: YYYY-MM-DD
verified_against: [ … the SQL files actually read … ]
reviewers:
  - gene <gene.tsenter@giantspoon.com>
---
```

`loader_script: none` answers "which script is canonical" before the reader opens a
section. `verified_against` must list the files you actually read — a stamp that
overstates what was checked is worse than none.

## Canonical files are settled centrally — do not re-warn per doc

As of 2026-08-21, `model/**` is the only active workspace and every duplicate was
archived. Individual docs must **not** carry "stale copies to avoid" tables; that
duplication across nine docs was itself the bloat problem. Cite the canonical path and
move on.

Authority order when sources disagree: **live production tables → semantic model
documentation → repository SQL.** State this once where it matters, not everywhere.

## Known trap: fields rewritten downstream of a branch

A branch's own SQL is not always the final word on a field it sets. In this model
`row_callouts` in the stable base **rebuilds** `qa_data_issues` from a fixed allow-list
rather than preserving branch output, so a value a branch writes can be silently
dropped. Documenting a field's value from branch SQL alone produced a real error in a
shipped doc. **Always confirm a field's live values with a query before documenting
them.**

## Never state a fact twice

The single largest source of bloat. Before adding a sentence, check whether the fact
already appears elsewhere in the doc. In one draft, the same safe-cast behavior
appeared four times and one schedule appeared four times — roughly 200 of 364 lines
were duplication, navigation, and vocabulary that crowded out real gaps. Cross-
reference instead of restating.

## Two writing rules

**1. Define a section's own term before its first table.** A heading naming a
specialized concept either footnotes the term in the heading itself
(`## Source and Output Contract[^1]`) or opens with a short **What this means:**
paragraph. A reader must never meet a table whose subject has not been explained. For
compact labels — table headers, column names, bullet labels — attach the definition
inline: `Grain — one row is…`.

**2. No undefined jargon anywhere.** If a term is not in Terminology and is not
obvious to a non-specialist, define it inline at first use. Write for a reader who
knows software generally but not this pipeline.

## Collapsed blocks: the headline must carry the claim

Present "things that will bite you" as `<details>` blocks so the page stays scannable.
The summary line must state the **claim**, not the topic: "Renaming a source value
creates a new package" is useful while collapsed; "Renaming" forces every reader to
expand it. This gives three reading depths — headings for the map, headlines for every
claim, expanded for why and what to do.

## Numbers go stale, queries do not

Do not record volatile row counts as durable state. Record the query that produces
them, so a reader can confirm the current value rather than trust a number of
unknown age. Where a count is genuinely useful, stamp it with the date it was
verified.

## Recommended starting order

A default, not a requirement. Adapt it to the pipeline.

1. Title, purpose, and final endpoint
2. **Table of Contents** — first section, always
3. **Terminology**
4. Pipeline overview and diagram
5. Data sources, with boundaries
6. Layer-by-layer notes
7. Outputs and field semantics
8. Operational scripts and refresh path
9. Key concepts
10. Current state, canonical surfaces, and durable warnings
11. Usage examples
12. Verification guidance
13. Maintenance
14. Troubleshooting

An optional `## HITL REVIEWERS` block may sit above the title while a document is
under human review, listing reviewers by name and email.

## Applying this to an existing README

Each step makes the next cheaper:

1. Move the table of contents to the top and add Terminology.
2. Add a **What this means:** line under every heading that names a concept.
3. Walk the content contract and add what is missing, in whatever sections fit.
4. Replace hard-coded counts with the query that produces them.
