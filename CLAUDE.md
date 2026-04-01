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
---
