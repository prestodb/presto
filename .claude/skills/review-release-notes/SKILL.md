---
name: review-release-notes
description: Review Presto release notes for a version-aggregation PR (e.g. "docs: Add release notes for 0.NNN") or the release-notes section of a feature PR. Checks each entry against Presto's release-notes guidelines — user-facing framing, correct category/verb, completeness, ordering, and RST formatting — and produces suggested rewrites (optionally posted as inline PR comments). Use when the user asks to "review release notes", "review the release notes PR", "check the release notes for 0.NNN", or points at a release-0.NNN.rst diff. Only use in a Presto repository (prestodb/presto).
---

# Presto Release Notes Review Skill

## Overview

This skill reviews Presto release notes the way a maintainer reviews the
version-aggregation PR (e.g. `docs: Add release notes for 0.297`, #27484). The
release notes are read by **end users and operators**, not contributors — so the
review is fundamentally an editorial pass that converts implementation-centric,
contributor-written bullets into clear, user-facing, correctly-categorized,
well-formatted entries.

The authoritative rules live in two wiki pages — read them when in doubt:
- [Release Notes Guidelines](https://github.com/prestodb/presto/wiki/Release-Notes-Guidelines) — format, sections, ordering, style.
- [Working with the Presto release notes PR](https://github.com/prestodb/presto/wiki/Working-with-the-Presto-release-notes-PR) — the PR workflow and reviewer checklist.

CI enforces only a minimal subset via `.github/workflows/release-notes-check.yml`
(the `presto-release-tools check-release-notes` command), which validates that a
`== RELEASE NOTES ==` / `== NO RELEASE NOTE ==` block exists and parses — it does
**not** check quality. This skill covers the quality review CI can't.

## How the release-notes PR works

The version-aggregation PR (`docs: Add release notes for 0.NNN`) is **created
automatically** by tooling that scrapes the `== RELEASE NOTES ==` block from
every merged PR since the last release into
`presto-docs/src/main/sphinx/release/release-0.NNN.rst`. It **blocks the release
cycle until merged**, so the review's job is to get that draft to publishable
quality. A **Release Shepherd** (a committer) owns it: they triage the *Missing
Release Notes* checklist in the PR body (deciding which PRs need an entry and
writing/requesting drafts), edit entries to the guidelines, populate the
**Highlights** section (copy notable entries, don't move them) and **Breaking
Changes** section, and merge once approvals are in.

To preview locally:

```bash
gh pr checkout <N> --repo prestodb/presto
cd presto-docs && ./build      # build the Sphinx docs (needs JDK per the README)
# open the generated HTML for release-0.NNN in a browser to eyeball rendering + links
```

## Prerequisites

Verify this is a Presto repository before proceeding:

```bash
test -f pom.xml && grep -q "com.facebook.presto" pom.xml
```

If not, inform the user and stop.

## How to invoke

The user will typically:
- Give a PR URL/number for a release-notes aggregation PR (`docs: Add release notes for 0.NNN`).
- Point at a `presto-docs/src/main/sphinx/release/release-0.NNN.rst` file or its diff.
- Ask to review the release-notes block in a feature PR body (`== RELEASE NOTES ==`).

Fetch the content to review:

```bash
# Aggregation PR: get the added release notes file and existing review comments
gh pr diff <N> --repo prestodb/presto
gh api repos/prestodb/presto/pulls/<N>/comments --paginate \
  --jq '.[] | "L\(.original_line) [\(.user.login)]: \(.body)"'
```

For each PR referenced by a note, you can open the PR to judge what the
*user-visible* effect actually was when the bullet is too internal to tell.

## The review — apply these checks to EVERY entry

The checks are ordered by how often they fire in real reviews. The first one is
the heart of the review; most rewrites trace back to it.

### 1. User-facing test (the dominant check)

Ask: **"What does a Presto user or operator see, do, or configure differently
because of this change?"** If the bullet doesn't answer that, it's wrong.

- **Internal machinery is not the subject.** Plan nodes, optimizer-rule class
  names, parsers, serialization internals, and refactors mean nothing to users.
  - "users don't know about our plan nodes" — never describe a change in terms
    of internal components.
  - A note may *mention* the optimizer rule as supporting detail, but must
    **lead with the user-visible benefit**, e.g.
    `Add PushdownThroughUnnest optimizer rule` →
    `Improve performance of `UNNEST` by adding a `PushdownThroughUnnest` optimizer rule that pushes projections and filters through the unnest when possible. ...`
  - For optimizer/perf changes, "describe the specific improvement the user is
    expected to see" and "describe the query shape that improves" — e.g.
    "improve parallelism for small tables", "improved performance for `UNNEST`".
- **Before concluding "remove," proactively hunt for the user-facing angle.**
  An internally-worded bullet very often *does* have a user-visible effect the
  author buried. Don't take the bullet at face value — **open the referenced PR**
  (`gh pr view <pr> --repo prestodb/presto`, read the title/description/diff) and
  look for any of:
  - a **performance/latency/memory** change a user would observe (→ "Improve …");
  - a new or changed **SQL syntax, function, type, or query behavior**;
  - a new/renamed **session or config property**, default change, or feature toggle;
  - a new **endpoint, metric, UI element, connector capability, or CLI/JDBC** behavior;
  - a **correctness fix** to a user-visible result, error, or hang.
  If you find one, **rewrite the note around that effect** rather than deleting it.
  e.g. `Add support for NativeFunctionHandle parsing` →
  `Add support for constant folding user defined functions in native clusters`;
  `Add PushdownThroughUnnest optimizer rule` →
  `Improve performance of `UNNEST` … (controlled by `pushdown_through_unnest`)`.
  When you propose such a rewrite, say what user-facing effect you inferred and
  from where, so the author/shepherd can confirm — don't invent a benefit the PR
  doesn't support.
- **Only remove when the dig comes up empty.** Pure refactors, internal-only
  parsing/serialization plumbing, and test-only changes genuinely have no
  user-visible effect → *"Remove, not user facing"* / `== NO RELEASE NOTE ==`.

### 2. Correct category and verb

Each note's leading verb places it in a category and ordering bucket. Check the
verb matches the actual change:

- "Seems like a performance improvement, not a feature" → use **Improve**, not **Add**.
- Each entry must **start with a prescribed action verb**, which also fixes its
  ordering bucket (see ordering below).
- Notes go under the correct **section**. Move misfiled notes to the right section.
  Section names always end with **"Changes"**; connector sections are
  `<Name> Connector Changes`. Canonical section order:

  > **Do not flag a cross-section entry as a duplicate just because two notes
  > share a PR number.** One feature legitimately produces *separate* notes when
  > it has both a general-engine component and a connector component (e.g.
  > incremental MV refresh: the engine machinery under General Changes *and* the
  > connector wiring under Iceberg Connector Changes), or an engine-level
  > SQL-syntax/parser change plus a connector-level implementation. Only call it
  > a duplicate when both notes describe the **same component and the same
  > user-visible effect**. (Highlights and Breaking Changes entries are
  > intentional *copies* of Details entries — never flag those as dups either.)

  1. General Changes
  2. Prestissimo (Native Execution) Changes
  3. Router Changes
  4. Security Changes
  5. JDBC Driver Changes
  6. Web UI Changes
  7. `<Name>` Connector Changes — **connectors alphabetical among themselves**
  8. Verifier Changes
  9. Resource Groups Changes
  10. SPI Changes
  11. Plugin Changes
  12. Documentation Changes

### 3. Completeness — each entry must stand alone

A user reading only this line must understand what changed and how to use it.

- A feature gated by a property **must name the property and link its docs.**
  - *"Needs to include the link to the config or describe inline"*
  - *"Add the config property here, link to documentation"*
  - Name both the session property and the config property when both exist, and
    cross-reference via `:ref:` / `:doc:`.
- New metrics/endpoints: *"at least describe the metrics or how to find them."*
- Reject vague bullets: *"This is incomplete, not understandable to a user."*
- State the default for a toggle: `(default enabled)` / `(disabled by default)`.

### 4. Ordering within a section

Entries within a section are ordered **by leading verb/category**, in this exact
sequence:

```
Fix → Improve → Add → Replace → Rename → Remove → Upgrade/Downgrade → Deprecate → Update
```

(Fixes, Optimizations, Additions, Replacements, Renames, Deletions, Dependency
updates, Deprecations, Updates.) Flag out-of-order sections: *"The ordering is
incorrect here, needs to be fix, then improve, then add, replace, rename,
remove, upgrade."*

### 5. RST formatting and mechanics

- **Imperative present tense**, capitalized, **1–2 sentences**, period-terminated,
  then the PR link. Audience is data engineers / lay users, not developers —
  "both concise and understandable to lay people."
- The whole block is wrapped in **triple backticks** so GitHub doesn't render it;
  long lines may wrap onto a continuation line without a leading `*`.
- When adding a property, say explicitly whether it is a **"configuration
  property"** or a **"session property"**, and give its default. For
  property-focused features, lead with the property name.
- **Double backticks** around all verbatim identifiers: session/config
  properties, SQL keywords, types, function names, file/endpoint names —
  ` ``UNNEST`` `, ` ``pushdown_through_unnest`` `, ` ``SMALLINT`` `,
  ` ``query-manager.required-workers`` `.
- **PR link format**: `` `#27125 <https://github.com/prestodb/presto/pull/27125>`_ ``
  (or the `:pr:`27125`` / `:issue:`27125`` Sphinx roles).
- **Dependency upgrades**: state `from <old> to <new>` and the CVE addressed,
  with a CVE link, e.g.
  `Upgrade aircompressor dependency from 0.27 to version 2.0.2 to fix `CVE-2025-67721 <https://www.cve.org/CVERecord?id=CVE-2025-67721>`_.`
  A non-CVE dependency bump is usually **not** user-facing → `== NO RELEASE NOTE ==`.
- **Cross-references** (Sphinx RST): `:doc:`/develop/openlineage-event-listener``
  for pages, `:ref:`admin/properties:``...`` ` for properties,
  `:func:`ngrams`` for functions. Verify links resolve in the local `./build`.
- **Catch typos** — they survive aggregation: `Materilized`→`Materialized`,
  `ajvto`→`ajv to`.

### 6. Should it exist at all? (NO RELEASE NOTE)

Some entries shouldn't be release notes:

- **Test-only PRs** and **pure refactors/internals** → `== NO RELEASE NOTE ==`.
- **Docs-only PRs**: fixing doc errors → `== NO RELEASE NOTE ==`; documenting a
  *new* feature → a `Documentation Changes` entry.
- This is the same judgment as the user-facing test (check 1), applied at the
  level of "does this belong in the notes" rather than "how is it worded."

## Producing the review

Default output is a per-line review listing, for each problematic entry: the
line, the problem (one of the categories above), and a concrete fix — either a
terse instruction or a ready-to-paste `suggestion`. Match the maintainer's
**terse, direct** comment voice:

- `Not user facing` / `Not user facing, remove`
- `Seems like a performance improvement, not a feature.`
- `Needs to include the link to the config or describe inline`
- `Fix ordering` (followed by the corrected block)
- A GitHub ```suggestion block with the rewritten line when the fix is concrete.

Group the report by severity: **Remove** (not user-facing, unfixable) →
**Rewrite** (user-facing but internal/incomplete) → **Reorder/format**
(mechanical). End with a short summary count.

## Optionally posting to the PR

Only if the user explicitly asks to post comments. Inline review comments with
suggestions are posted via the reviews API; confirm the commit SHA and the exact
bullets first, and **never post to prestodb/presto without the user's explicit
go-ahead this session.** Default to producing the report and letting the user
post, unless told otherwise.

## Reference: anatomy of a good entry

```
* Improve performance of ``UNNEST`` by adding a ``PushdownThroughUnnest`` optimizer rule that pushes projections and filters through the unnest when possible. This rule is controlled using the ``pushdown_through_unnest`` session property (default enabled). `#27125 <https://github.com/prestodb/presto/pull/27125>`_
```

It leads with the user benefit, names and explains the controlling property with
its default, keeps identifiers in backticks, and ends with the PR link.
