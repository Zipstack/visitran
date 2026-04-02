from django.urls import path

from backend.core.routers.git_config.views import (
    delete_git_config,
    enable_pr_workflow,
    get_available_repos,
    get_git_config,
    list_branches,
    save_git_config,
    test_git_connection,
)

urlpatterns = [
    path("/test", test_git_connection, name="test-git-connection"),
    path("/available-repos", get_available_repos, name="get-available-repos"),
    path("/branches", list_branches, name="list-branches"),
    path("/enable-pr-workflow", enable_pr_workflow, name="enable-pr-workflow"),
    path("/save", save_git_config, name="save-git-config"),
    path("/delete", delete_git_config, name="delete-git-config"),
    path("", get_git_config, name="get-git-config"),
]
