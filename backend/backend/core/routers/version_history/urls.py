from django.urls import path

from backend.core.routers.version_history.views import (
    commit_project,
    compare_versions,
    create_version_pr,
    execute_version,
    get_audit_events,
    get_current_version,
    get_version_by_id,
    get_version_detail,
    get_version_detail_by_sha,
    get_version_history,
    get_version_pr,
    import_from_branch,
    preview_rollback,
    retry_git_sync,
    rollback_to_version,
    validate_rollback,
    verify_version_integrity,
)

urlpatterns = [
    # Project-level versioning
    path("/commit", commit_project, name="commit-project-version"),
    path("/versions", get_version_history, name="get-version-history"),
    path("/compare", compare_versions, name="compare-versions"),
    path("/rollback", rollback_to_version, name="rollback-version"),
    path("/rollback/validate", validate_rollback, name="validate-rollback"),
    path("/rollback/preview", preview_rollback, name="preview-rollback"),
    path("/execute-version", execute_version, name="execute-version"),
    path("/retry-git-sync", retry_git_sync, name="retry-git-sync"),
    path("/current", get_current_version, name="get-current-version"),
    path("/audit", get_audit_events, name="get-audit-events"),
    path("/version/<int:version_number>", get_version_detail, name="get-version-detail"),
    path("/version/<int:version_number>/verify", verify_version_integrity, name="verify-version-integrity"),
    path("/version/<int:version_number>/pr", get_version_pr, name="get-version-pr"),
    path("/version/<int:version_number>/create-pr", create_version_pr, name="create-version-pr"),
    path("/version-id/<str:version_id>", get_version_by_id, name="get-version-by-id"),
    path("/sha/<str:commit_sha>", get_version_detail_by_sha, name="get-version-by-sha"),
    path("/import", import_from_branch, name="import-from-branch"),
]
