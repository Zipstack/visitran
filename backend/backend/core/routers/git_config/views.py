"""Git config API views — function-based with @api_view + @handle_http_request."""

import logging

from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from backend.core.services import git_repo_config_service
from backend.core.services.git_service import get_git_service
from backend.core.utils import handle_http_request
from backend.utils.constants import HTTPMethods
from rbac.factory import handle_permission

logger = logging.getLogger(__name__)

RESOURCE_NAME = "gitconfig"


@api_view([HTTPMethods.GET])
@handle_http_request
def get_git_config(request: Request, project_id: str) -> Response:
    """Get git repository configuration for a project."""
    config = git_repo_config_service.get_config(project_id=project_id)
    return Response(data={"status": "success", "data": config}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
def save_git_config(request: Request, project_id: str) -> Response:
    """Create or update git repository configuration for a project."""
    config = git_repo_config_service.save_config(project_id=project_id, config_data=request.data)
    return Response(data={"status": "success", "data": config}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.DELETE])
@handle_http_request
@handle_permission
def delete_git_config(request: Request, project_id: str) -> Response:
    """Remove git repository configuration (disable versioning)."""
    git_repo_config_service.delete_config(project_id=project_id)
    return Response(
        data={"status": "success", "data": "Git configuration removed successfully."},
        status=status.HTTP_200_OK,
    )


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
def test_git_connection(request: Request, project_id: str) -> Response:
    """Test connection to a git repository."""
    result = git_repo_config_service.test_connection(project_id=project_id, config_data=request.data)
    return Response(data={"status": "success", "data": result}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_available_repos(request: Request, project_id: str) -> Response:
    """List repos already configured in the organization."""
    repos = git_repo_config_service.get_available_repos()
    return Response(data={"status": "success", "data": repos}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def list_branches(request: Request, project_id: str) -> Response:
    """List branches in the project's configured git repository."""
    from backend.core.models.git_repo_config import GitRepoConfig

    config = GitRepoConfig.objects.filter(
        project_id=project_id, is_deleted=False, is_active=True,
    ).first()
    if not config:
        return Response(
            data={"status": "failed", "error_message": "No git configuration found for this project."},
            status=status.HTTP_404_NOT_FOUND,
        )
    service = get_git_service(config)
    branches = service.list_branches()
    return Response(data={"status": "success", "data": {"branches": branches}}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
def create_branch(request: Request, project_id: str) -> Response:
    """Create a new branch in the git repository using inline credentials."""
    result = git_repo_config_service.create_branch_from_data(
        project_id=project_id, config_data=request.data,
    )
    return Response(data={"status": "success", "data": result}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def list_project_folders(request: Request, project_id: str) -> Response:
    """List project folders in the repo that contain models.yaml."""
    folders = git_repo_config_service.list_project_folders(
        project_id=project_id, config_data=request.data,
    )
    return Response(data={"status": "success", "data": folders}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
def enable_pr_workflow(request: Request, project_id: str) -> Response:
    """Enable PR workflow for a project's git configuration."""
    payload = request.data
    pr_mode = payload.get("pr_mode", "manual")
    pr_base_branch = payload.get("pr_base_branch", "main")

    from backend.core.models.git_repo_config import GitRepoConfig

    config = GitRepoConfig.objects.filter(
        project_id=project_id, is_deleted=False, is_active=True,
    ).first()
    if not config:
        return Response(
            data={"status": "failed", "error_message": "No git configuration found for this project."},
            status=status.HTTP_404_NOT_FOUND,
        )

    # Validate working branch != base branch
    if config.branch_name == pr_base_branch:
        return Response(
            data={"status": "failed", "error_message": f"Target branch cannot be the same as the working branch ({config.branch_name})."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # Validate that the base branch exists
    service = get_git_service(config)
    branch_info = service.get_branch(pr_base_branch)
    if not branch_info:
        return Response(
            data={"status": "failed", "error_message": f"Branch '{pr_base_branch}' not found in repository."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    updated = git_repo_config_service.save_config(
        project_id=project_id,
        config_data={
            "pr_mode": pr_mode,
            "pr_base_branch": pr_base_branch,
        },
    )
    return Response(data={"status": "success", "data": updated}, status=status.HTTP_200_OK)
