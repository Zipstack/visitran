---
## Version History Feature Migration

### What we're building
Git-Based YAML Version Control for the no-code model IDE.
Full spec: users can commit model changes, view history, diff versions,
rollback, resolve conflicts, and sync to GitHub as YAML files.

### Migration status
- Source: older monorepo (partially implemented, ~91 files, ~14,800 lines)
- Target: this OSS repo
- Approach: re-implement intent using this repo's patterns — do NOT copy old code directly

### New repo conventions (always follow these)
Backend:
- Models: inherit BaseModel + DefaultOrganizationMixin, one file per model in core/models/
- Views: function-based @api_view() — NOT ViewSets
- Services: in core/services/
- Auth: @oss_decorator from rbac/oss_decorator.py on all views
- Encryption: backend/utils/encryption.py for secrets
- Cache: use existing cache_decorator.py + redis_client.py
- Celery: @shared_task, register in CELERY_IMPORTS

Frontend:
- JS with PropTypes, no TypeScript
- Ant Design components
- Zustand with persist middleware
- Service functions receive axios from useAxiosPrivate()
- memo() wrapped named exports

### Phase completion tracker
- [x] Phase 1: Models + Errors + Migrations
- [x] Phase 2: Services + Celery
- [x] Phase 3: API Router layer
- [x] Phase 4: Frontend store + services + components
- [x] Phase 5: Integration wiring

### Remaining work
- [ ] **Long-term execute_version fix** — refactor `execute_visitran_run_command()` to accept
  `model_data_override` dict, avoiding the DB write/restore pattern entirely

### Resolved
- [x] `execute_version`: wired + concurrency-safe (Redis lock + `select_for_update`)
- [x] `DraftValidator`: implemented at `backend/application/model_validator/draft_validator.py`
- [x] `validate_draft` view: fully wired to DraftValidator

### Git Config Feature — COMPLETE
Security: Admin-only write endpoints, server-side token validation,
  project-level access check, log level fix, double encryption guard
GitHub Actions: Branch creation, PR creation, PR workflow on commit
GitLab: Full parity with GitHub — all 10 methods, MR support,
  self-hosted detection

- [x] Phase 1A: Admin-only endpoints + server-side token validation + log level fix + double encryption guard
- [x] Phase 1B: Project-level access check on all git config operations
- [x] Phase 2A: Branch + PR methods in GitHubService
- [x] Phase 2B: PR workflow service + model fields + commit wire-in
- [x] Phase 2C: branches, enable-pr-workflow, get-version-pr endpoints
- [x] Phase 2D: PR workflow UI in GitConfigTab + PR badge in VersionTimeline
- [x] Phase 3: GitLab support (GitLabService + factory + frontend labels)
- [x] Manual PR workflow: pr_mode enum (disabled/auto/manual)
- [x] git_branch_name on ModelVersion for manual PR tracking
- [x] git_pr_service split: push_version_to_branch + create_pr_for_version
- [x] create-pr endpoint: POST version/{n}/create-pr
- [x] GitConfigTab: Segmented mode selector (Off/Auto PR/Manual PR)
- [x] VersionTimeline: Create PR/MR in action dropdown (manual mode only)
- [x] 409 handled gracefully — existing PR shown with link

### Bug fixes (post-migration)
- `execute_version`: fixed concurrency (Redis lock + `select_for_update`)
- `_serialize_version`: added `is_current` to response dict
- `get_draft_status`: fixed `committed_data` lookup to use project-level ModelVersion (`config_model=None`) instead of model-scoped versions
- dual-write: added `skip_draft_write` flag to `_update_model` — execution pipeline no longer creates false draft records
- `set_current_version`: added cache invalidation after DB update
- `handleExecuteSuccess`: added `loadDraftStatus()` call to clear stale draft indicator after execute
- `execute_version`: model data now persists on success (restore only on failure)
- `_update_config`: replaced `pr_workflow_enabled` with `pr_mode` in field list, added `_READONLY_PROPS` skip set
- `enable_pr_workflow` view: sends `pr_mode` instead of `pr_workflow_enabled`
- `_serialize_config`: added `pr_mode` to API response (was missing — caused UI to reset to Off on tab switch)
- VersionTimeline: `isManualMode` → `prWorkflowEnabled` — Create PR button now shows for any enabled PR mode
- GitConfigTab: added `useEffect` to sync `prMode`/`prBaseBranch`/`prBranchPrefix` from gitConfig on async load
---
