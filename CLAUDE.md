---
## Version History Feature Migration

### What we're building
Git-Based YAML Version Control for the no-code model IDE.
Full spec: users can commit model changes, view history, diff versions,
rollback, and sync to GitHub as YAML files.

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

### Remaining work
- [ ] **Long-term execute_version fix** — refactor `execute_visitran_run_command()` to accept
  `model_data_override` dict, avoiding the DB write/restore pattern entirely

### Current architecture
- Every model save → `_trigger_auto_commit()` → background thread → pushes combined YAML to working branch
- Auto-commit triggers: `_update_model` (transform), `create_a_model`, `delete_a_file_or_folder`
- GitHub commit history = version history source (`get_versions_from_github`)
- DB ModelVersion stores lightweight metadata only (model_data=None for new commits)
- Rollback reads from GitHub at exact commit SHA
- Manual commit = title + description → Celery task → git push to working branch
- PR workflow: manual only — PRs raised from working branch → base branch (no per-version branches)
- `git_project_folder` on GitRepoConfig overrides project slug for YAML path — set automatically by import to reuse source folder in-place (no duplication)

### Git Config — setup flow
- Repo URL + token → Test Connection (validates + returns branch list)
- `test_connection` supports GitHub + GitLab via `_build_service_from_data()` factory
- Branch name: dropdown populated from `test_connection` response (not text input)
- Create branch: inline UI + `create-branch` endpoint (accepts inline credentials, pre-save)
- `base_path` removed from setup form (project slug creates the folder)
- Import from existing project: source branch → project folder dropdown → Save & Import → auto-execute

### Git Config — configured view
- Shows: repo URL, type, working branch, status, last synced
- PR Workflow section: target branch dropdown + save (always manual mode, no Off/Auto toggle)
- Validation: target branch ≠ working branch (backend + frontend)
- Disconnect button with confirmation

### Git Config endpoints
| Method | Path | View |
|--------|------|------|
| GET | `` | `get_git_config` |
| POST | `/save` | `save_git_config` |
| DELETE | `/delete` | `delete_git_config` |
| POST | `/test` | `test_git_connection` — returns `{success, repo_info, branches}` |
| GET | `/available-repos` | `get_available_repos` |
| GET | `/branches` | `list_branches` |
| POST | `/create-branch` | `create_branch` — inline credentials |
| POST | `/project-folders` | `list_project_folders` — scans repo for dirs with models.yaml |
| POST | `/enable-pr-workflow` | `enable_pr_workflow` — sets pr_mode + pr_base_branch |

### Version History endpoints
| Method | Path | View |
|--------|------|------|
| POST | `/commit` | `commit_project` |
| GET | `/versions` | `get_version_history` — GitHub commits first, DB fallback |
| GET | `/compare` | `compare_versions` |
| POST | `/rollback` | `rollback_to_version` |
| GET | `/rollback/validate` | `validate_rollback` |
| GET | `/rollback/preview` | `preview_rollback` |
| POST | `/execute-version` | `execute_version` — supports `commit_sha` for git-sourced versions |
| POST | `/retry-git-sync` | `retry_git_sync` |
| GET | `/current` | `get_current_version` |
| GET | `/audit` | `get_audit_events` |
| GET | `/version/<int>` | `get_version_detail` |
| GET | `/version/<int>/verify` | `verify_version_integrity` |
| GET | `/version/<int>/pr` | `get_version_pr` |
| POST | `/version/<int>/create-pr` | `create_version_pr` |
| GET | `/version-id/<str>` | `get_version_by_id` |
| GET | `/sha/<str>` | `get_version_detail_by_sha` |
| POST | `/import` | `import_from_branch` — imports models + creates ModelVersion |

### Branch Pull & Import (project migration via git)
Flow: new project → connect to existing repo → select source branch → pick project folder → create working branch → Save & Import → auto-execute
- **No folder duplication** — import reads from `source_folder/models.yaml` and sets `git_project_folder = source_folder` on GitRepoConfig; no new folder or commit is created in git
- `git_project_folder` field on GitRepoConfig — overrides project slug for all YAML paths
- All YAML path lookups use `git_project_folder` when set (celery tasks, version history, GitHub fetch)
- `list_directory()` on GitHubService + GitLabService — lists repo contents
- `list_project_folders` — detects folders containing models.yaml, returns model counts
- `import_from_branch` — reads YAML, creates ConfigModels + ModelVersion (uses existing commit SHA from source), sets `git_project_folder`, returns schema warnings
- Auto-execute after import — frontend calls `executeVersion` immediately after import
- `execute_version` accepts `commit_sha` for git-sourced versions without DB records
- Schema warnings: extracts `source.schema_name` and `model.schema_name` from imported models

### PR workflow
- Manual only — no Off/Auto/Manual toggle, just a base branch selector
- PRs created from working branch → base branch via `create_pr_for_version()`
- `git_pr_service.py`: simplified to single function, no per-version branches
- `_sync_to_git`: returns early for any non-disabled PR mode (PR creation is user-initiated)
- Backend validates working branch ≠ base branch
- VersionTimeline: "Create PR" button when `pr_mode !== "disabled"` and version synced

### Key services
| Service | File | Purpose |
|---------|------|---------|
| `model_version_service` | `core/services/model_version_service.py` | Version CRUD, commit, rollback, diff, git sync, import |
| `git_service` | `core/services/git_service.py` | GitHub/GitLab API (push, list, branches, PRs, directory) |
| `git_repo_config_service` | `core/services/git_repo_config_service.py` | Config CRUD, test connection, branch/folder operations |
| `git_pr_service` | `core/services/git_pr_service.py` | PR creation from working → base branch |
| `yaml_serializer` | `core/services/yaml_serializer.py` | Serialize/deserialize project YAML, path construction |
| `version_celery_tasks` | `core/scheduler/version_celery_tasks.py` | auto_commit_to_github, manual_commit_to_github |
| `audit_trail_service` | `core/services/audit_trail_service.py` | Create/query audit events |
| `version_cache_service` | `core/services/version_cache_service.py` | Redis cache for version data |
| `rollback_validation_service` | `core/services/rollback_validation_service.py` | Pre-rollback validation and preview |
| `version_diff_service` | `core/services/version_diff_service.py` | Diff computation between versions |

### Frontend components (`frontend/src/ide/version-history/`)
| Component | Purpose |
|-----------|---------|
| `VersionHistoryDrawer` | Main drawer container, tab switching |
| `Header` | Drawer header with commit button |
| `VersionTimeline` | Timeline of versions from git, action dropdowns |
| `CommitModal` | Manual commit dialog (title + description) |
| `CompareModal` | Side-by-side version diff |
| `ViewVersionModal` | View single version content |
| `RollbackModal` | Rollback confirmation |
| `ExecuteVersionModal` | Execute a version (supports git-sourced via commit_sha) |
| `DiffViewer` | Unified diff rendering |
| `GitConfigTab` | Git config setup + import + PR settings |
| `DisabledOverlay` | Overlay when versioning not configured |
| `services.js` | All API service functions |

### Models
- `ModelVersion` — immutable versioned snapshots, git_commit_sha, pr tracking, is_current flag
- `GitRepoConfig` — per-project git config, credentials, pr_mode, pr_base_branch, git_project_folder
- `VersionAuditEvent` — audit trail entries

### Bug fixes (post-migration, all resolved)
- `execute_version`: concurrency-safe (Redis lock + `select_for_update`), supports `commit_sha` fallback
- `get_versions_from_github`: field names match frontend (`committed_by.name`, `created_at`, `version_id`), directory-level fallback, `total_count` for pagination
- `_trigger_auto_commit`: extracted as reusable helper, wired into create/update/delete model
- `test_connection`: supports GitLab via factory pattern (was hardcoded to GitHub)
- PR workflow: working branch ≠ base branch validation (backend + frontend)
- `ExecuteVersionModal`: handles null `targetVersion` + object shape `{versionNumber, commitSha}`
---
