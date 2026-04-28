---
name: ship
description: Create a branch, atomic commits, push, and open a draft PR using the project template
disable-model-invocation: true
allowed-tools: Bash, Read, Edit
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

Run `git status` and `git diff` to inventory uncommitted changes.

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

**Secrets scan (hard stop):** grep the diff for high-entropy strings and known key
prefixes — `sk-`, `AKIA`, `ghp_`, `gho_`, `xoxb-`, `xoxp-`, `-----BEGIN ` (private
keys), AWS-style `aws_secret_access_key`, etc. On hit:
1. Print the matched line and the file it came from
2. Use AskUserQuestion to offer:
   - **Unstage the file** from this ship (`git restore --staged <path>`)
   - **Skip the file from this PR** (leave staged but exclude from commits)
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
   - Commit with a Conventional Commits message via HEREDOC
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

**Render the full PR body and show it to the user. Wait for confirmation** before raising. Phrase the prompt as a natural question (e.g., "Ready to open the draft PR?"), not as "Confirmation Gate."

### Phase 6 — Raise the PR

1. Write the rendered body to `/tmp/pr-body-<branch>.md`
2. Run:
   ```bash
   gh pr create --draft \
     --title "FEAT: <summary>"   # or "FIX: <summary>"
     --body-file /tmp/pr-body-<branch>.md \
     --base main
   ```
3. If `gh` is not authenticated (`gh auth status` fails), instruct the user to run
   `gh auth login` and stop. Do not retry silently
4. Capture stdout, extract and print the PR URL
5. Clean up the temp file (`rm /tmp/pr-body-<branch>.md`)

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
- Updating an existing PR (force-push to same branch + edit body)
- Pre-push lint/test enforcement
- Auto-merging or marking ready-for-review
- Cross-repo or fork-based PR flows
- Squashing or interactive rebase

---

## 10. PR body template

The template lives in [pr-body-template.md](pr-body-template.md), sibling to this file.

To change the structure of generated PR descriptions, edit that file directly — no
changes to SKILL.md needed. Phase 5 reads it on every run.
