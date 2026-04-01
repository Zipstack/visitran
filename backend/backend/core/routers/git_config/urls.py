from django.urls import path

from backend.core.routers.git_config.views import (
    delete_git_config,
    get_available_repos,
    get_git_config,
    save_git_config,
    test_git_connection,
)

urlpatterns = [
    path("/test", test_git_connection, name="test-git-connection"),
    path("/available-repos", get_available_repos, name="get-available-repos"),
    path("/save", save_git_config, name="save-git-config"),
    path("/delete", delete_git_config, name="delete-git-config"),
    path("", get_git_config, name="get-git-config"),
]
