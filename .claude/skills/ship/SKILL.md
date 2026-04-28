---
name: ship
description: Create a branch, atomic commits, push, and open a draft PR using the project template
disable-model-invocation: true
allowed-tools: Bash, Read, AskUserQuestion
---

# /ship

Take the current changes (uncommitted or already-committed) and walk them through:
branch → atomic commits → push → draft PR. Three hard confirmation gates so nothing
runs ahead of the user.

This skill is **manual-only** (`disable-model-invocation: true`). Only run when the user
types `/ship`. Never auto-invoke from conversational pattern-matching.

---

## 1. Conventions

**Branch prefixes (binary):**
- Any feature work → `feat/<kebab-case-summary>`
- Everything else (fix, refactor, chore, docs) → `fix/<kebab-case-summary>`

**Branch name:** Derive from the *intent* of the work (from conversation context), not
just the files touched. E.g., `feat/google-oauth-signin`, not `feat/update-auth-controller`.

**Commit format:** Conventional Commits — `<type>(<scope>): <subject>`.
Types used: `feat`, `fix`, `refactor`, `chore`, `docs`, `test`.

**Commit trailer:** Every commit message ends with the trailer
`Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`,
separated from the body by a blank line, per the global protocol. Pass the
full message — subject, blank line, body, blank line, trailer — through a
HEREDOC.

**Commit grouping rules:**
- One logical concern per commit
- Refactors separate from new behavior
- Migrations in their own commit
- Config/env changes in their own commit
- Tests in a separate `test:` commit *after* the implementation commits they cover

**PR title format:** `FEAT: <imperative summary>` or `FIX: <imperative summary>`.

**PR state:** Always raised as draft (`--draft`).

**Marker for unfilled sections in the PR body:** plain `TODO` (not `TODO(human)`).

---

## 2. Phases

### Phase 1 — Discover and classify

Inventory **all** pending changes — both staged and unstaged — so nothing
sneaks past the scan or the plan:
- `git status` for the file list
- `git diff HEAD` for the full content diff (covers staged + unstaged)
- `git diff --cached --name-only` to identify files that were already in the
  index before `/ship` was invoked

Any files already staged before invocation must be either explicitly included
in the commit plan or unstaged before Phase 4 — otherwise they will be silently
swept into the first `git commit`. If pre-staged files exist that don't fit
the plan, surface the list and ask the user whether to include or unstage them
before proceeding.

Classify the work as **feature** or **non-feature**:
- *Feature signals:* new files (routes, components, endpoints), new exports, additions
  to public API, conversation language like "let's add" / "implement" / "build"
- *Non-feature signals:* modifications to existing logic only, conversation language
  like "broken" / "doesn't work" / "should handle"
- *Mixed:* **stop and ask** whether to split into two PRs or roll up under the dominant type

**Edge cases:**
- Already on a feature branch (not `main`/`master`/`develop`) → ask whether to continue
  committing on it or branch off
- **No uncommitted changes (everything already committed locally)** → skip Phases 1–4;
  derive the branch name and PR description from existing commit messages and
  `git diff <base>...HEAD`; push existing commits in Phase 4; proceed to Phase 5
- Branch already pushed → skip the Phase 4 push step
- On `main`/`master`/`develop` with **no changes at all** → stop with a clear message
- Diff spans clearly unrelated areas → stop and ask whether to split

**Secrets scan (hard stop):** run the grep against `git diff HEAD` (so both
staged and unstaged content are inspected) for high-entropy strings and known
key prefixes — `sk-`, `AKIA`, `ghp_`, `gho_`, `xoxb-`, `xoxp-`, `-----BEGIN `
(private keys), and an AWS secret-key assignment regex
`aws_secret_access_key\s*[=:]\s*['"]?[A-Za-z0-9/+=]{40}['"]?` (case-insensitive
— anchors on assignment + a 40-char value so bare references in `.env.example`
don't trip the gate). On hit:
1. Print the matched line and the file it came from
2. Use AskUserQuestion to offer (both options must call
   `git restore --staged <path>` first — leaving the file staged means it
   *will* be included in the next commit, regardless of which paths you
   `git add` afterwards):
   - **Drop from this PR, keep in working tree** —
     `git restore --staged <path>` (file remains on disk for a future ship)
   - **Drop from this PR and remove from working tree** —
     `git restore --staged <path>` then `rm <path>` (or `git rm <path>` if
     it was tracked) so the secret is no longer present anywhere
   - **Cancel ship entirely**
3. Do not proceed past Phase 1 until the user picks one

### Phase 2 — Plan the branch name

Generate `feat/<kebab>` or `fix/<kebab>` based on classification + intent.

**Do not create the branch yet** — present the proposed name as part of the Phase 3 plan.

### Phase 3 — Plan the commits → CONFIRMATION GATE

Group the diff into atomic commits per the rules in Section 1.

Present the plan, e.g.:

```
Branch: feat/google-oauth-signin

Commit 1 (refactor): extract shared validation helper
Commit 2 (feat):     add Google OAuth provider
Commit 3 (feat):     wire OAuth into signup flow
Commit 4 (test):     add tests for OAuth provider
```

**Wait for user confirmation** before any branch creation or commit. Phrase the prompt as a natural question (e.g., "Ready to create the branch and commit?"), not as "Confirmation Gate."

### Phase 4 — Execute

After confirmation:
1. `git checkout -b <branch-name>` (skip if already on the planned branch)
2. For each planned commit:
   - Stage the relevant files: `git add <path>...` (use `-p` only when a single file
     legitimately needs to be split across commits)
   - Commit with a Conventional Commits message via HEREDOC. The HEREDOC
     body must include the `Co-Authored-By` trailer documented in Section 1.
3. `git push -u origin <branch-name>` (skip if already pushed and up to date)

Hard rules:
- Never `--force` / `--force-with-lease`
- Never `--no-verify`. If a pre-commit hook fails, surface the error and stop — let the
  user fix and re-run. Do not amend; create a new commit if needed
- Never `git add -A` / `git add .` — always add by explicit path

### Phase 5 — Draft PR description → CONFIRMATION GATE

Read the PR body template from `.claude/skills/ship/pr-body-template.md` (sibling to
this file). That is the single source of truth — do not look up `.github/`, `docs/`,
or any other repo-level PR template.

Fill each section per the source-of-truth mapping in Section 3.

Use **descriptive, full-sentence prose** — telegraph bullets are not acceptable.
Use `TODO` as a marker for any section that can't be filled confidently.

Every `-` placeholder from the template must be replaced — either with a
filled, prose answer or with a literal `TODO`. A bare `-` is never an
acceptable final value in the rendered body.

**Render the full PR body and show it to the user. Wait for confirmation** before raising. Phrase the prompt as a natural question (e.g., "Ready to open the draft PR?"), not as "Confirmation Gate."

### Phase 6 — Raise the PR

1. Check `gh auth status`. If it fails, instruct the user to run
   `gh auth login` and stop. Do not retry silently — and do not proceed to
   any of the steps below, so no temp file is left behind.
2. Check whether a PR for this branch already exists:
   ```bash
   EXISTING_PR=$(gh pr view --json url --jq '.url' 2>/dev/null || true)
   ```
   If `EXISTING_PR` is non-empty, stop with a clear message: print the
   existing PR URL and tell the user that `/ship` does not currently update
   existing PRs (see Section 9). Suggest they push further commits manually
   with `git push` and edit the PR description in the GitHub UI. Do not
   write the temp file. Do not invoke `gh pr create`.
3. Sanitize the branch name (the `feat/`/`fix/` prefix contains a `/`, which
   would create a non-existent subdirectory under `/tmp`):
   ```bash
   BRANCH_SAFE=$(echo "<branch-name>" | tr '/' '-')
   # e.g. feat/google-oauth-signin → feat-google-oauth-signin
   ```
4. Write the rendered body to `/tmp/pr-body-$BRANCH_SAFE.md`
5. Run:
   ```bash
   # Pick the prefix from the Phase 1 classification:
   #   feature     → TITLE_PREFIX="FEAT"
   #   non-feature → TITLE_PREFIX="FIX"
   TITLE_PREFIX=<FEAT|FIX>     # set per classification
   BASE=$(gh repo view --json defaultBranchRef --jq '.defaultBranchRef.name')
   gh pr create --draft \
     --title "${TITLE_PREFIX}: <imperative summary>" \
     --body-file "/tmp/pr-body-$BRANCH_SAFE.md" \
     --base "$BASE"
   ```
6. Capture stdout, extract and print the PR URL
7. Clean up the temp file (`rm -f "/tmp/pr-body-$BRANCH_SAFE.md"`)

---

## 3. PR section sources

| Section | Source |
|---|---|
| **What** | Diff — one or two sentences summarizing the change |
| **Why** | Conversation context (the diff says *what*, conversation says *why*). If thin, write `TODO`. |
| **How** | Diff structure — which layers touched, which patterns used |
| **Can this PR break any existing features** | Risk scan: did any function signature change, was any shared utility modified, was any DB schema touched, was any config changed. If genuinely low-risk, *say why* (e.g., "Additive only — new endpoint, no existing call sites modified"). Never just "No." |
| **Database Migrations** | Scan for migration files in conventional paths. If none, "None." If some, list by name + one-line purpose. |
| **Env Config** | Grep diff for newly referenced env vars. List them. |
| **Relevant Docs** | Conversation references; otherwise `TODO` |
| **Related Issues or PRs** | Grep conversation for `#123`, `JIRA-456`, etc. If none, `TODO`. |
| **Dependencies Versions** | Diff `package.json`, `pyproject.toml`, `requirements.txt`, `go.mod`, `pdm.lock`, `uv.lock`. List additions/upgrades as `name: old → new`. |
| **Notes on Testing** | See Section 4 |
| **Screenshots** | See Section 5 |
| **Checklist** | Verbatim from template |

---

## 4. Notes on Testing — generation logic

This is the most important section to get right. Synthesize from **three sources** and
emit markdown checkboxes:

1. **Code paths that changed.** For each modified function/component, what user-facing
   behavior does it touch? Each behavior → one checkbox.
2. **Edge cases the conversation surfaced.** If the user said earlier "make sure it
   handles the null case" or "this broke when the user has no permissions," each
   becomes an explicit checkbox. **This is the highest-value source — the diff alone
   can't surface intent.**
3. **Regression risks.** If a shared utility was modified, every caller surface is a
   regression risk → one checkbox per surface.

**Output format:**

```markdown
- [ ] Verify Google OAuth signup creates a new user with correct profile data
- [ ] Verify existing email-password login still works (regression)
- [ ] Verify OAuth flow handles user denial / cancellation gracefully
- [ ] Verify error state when Google API is unreachable
- [ ] Verify the fix for null `displayName` (raised in conversation)
```

**Quality bar:** if the only items produced are generic ("verify the change works"),
the conversation wasn't read carefully enough. **Specificity is the bar.** Every
conversation-derived checkbox should be traceable to something the user actually said.

---

## 5. Screenshots — header generation logic

Infer screenshotable surfaces from changed file paths and component usage. Emit `###`
headers only — **no placeholder image links, no captions** — leaving space for the
developer to paste images underneath.

**Categories to detect:**
- **Page-level surfaces** — from file paths like `pages/Home.jsx` or route definitions
  → one header per page
- **New UI states** — loading, error, empty, success states added to a component
  → one header per state
- **Modals, drawers, tooltips, toasts** — distinct elements not captured in page screenshots
- **Responsive variants** — if responsive CSS was touched → mobile + desktop headers
- **Theme variants** — if theming code was touched → light + dark headers

**Output format:**

```markdown
### Homepage – Updated hero section

### Signup Page – New OAuth button

### Signup Page – Error state when OAuth fails

### Mobile view – Signup page
```

If no UI files changed: `N/A — backend-only change.`

---

## 6. Confirmation gates (summary)

Three hard stops where the skill must wait for user input:

1. **After classification** — only if feat/fix is ambiguous or work is mixed
2. **After commit plan (Phase 3)** — before any branch creation or commit
3. **After PR body draft (Phase 5)** — before `gh pr create` runs

Plus the conditional gate:
- **After secrets-scan hit** — before any further phase

These gates are non-negotiable.

When prompting at any of these gates, follow Section 7 — natural language, no gate or phase numbers.

---

## 7. Talking to the user

The phase / gate / section numbers in this file are internal scaffolding for you to
follow. **Do not mention them in user-facing output.** A user running `/ship` should
never see "Phase 3", "Confirmation Gate #2", "Section 4", or similar.

Instead, describe what's happening in plain sentences — what you're doing now, what's
next. Examples:

- Bad: "Entering Phase 3. Presenting commit plan for confirmation."
- Good: "Here's the commit plan. Ready for me to create the branch and commit?"

- Bad: "Phase 5 complete. Proceeding to Phase 6."
- Good: "PR body looks good — opening the draft PR now."

- Bad: "Secrets scan triggered hard stop per Section 2."
- Good: "Found what looks like a secret in `config.py:14`. How do you want to handle it?"

This applies to all user-visible text: status updates between steps, the confirmation
prompts at each gate, and the final summary after the PR is opened.

---

## 8. Guardrails

- Never force-push
- Never commit without showing the plan first
- Never skip the secrets scan
- Never auto-merge or take the PR out of draft
- Never use `--no-verify`. If a hook fails, fix the underlying issue or surface and stop
- If `gh` is unauthenticated, stop and surface the error
- If on `main`/`master`/`develop` with no changes, stop with a clear message
- If the diff spans clearly unrelated areas, stop and ask whether to split

---

## 9. Out of scope (v1)

Explicitly not handled — keep the skill focused:
- Updating an existing PR. If `/ship` detects a PR already exists for the
  current branch, it stops and surfaces the URL — pushing further commits
  and editing the description must be done manually.
- Pre-push lint/test enforcement
- Auto-merging or marking ready-for-review
- Cross-repo or fork-based PR flows
- Squashing or interactive rebase

---

## 10. PR body template

The template lives in [pr-body-template.md](pr-body-template.md), sibling to this file.

To change the structure of generated PR descriptions, edit that file directly — no
changes to SKILL.md needed. Phase 5 reads it on every run.
