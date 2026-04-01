"""Git config API views — function-based with @api_view + @handle_http_request."""

import logging

from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from backend.core.services import git_repo_config_service
from backend.core.utils import handle_http_request
from backend.utils.constants import HTTPMethods

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
def save_git_config(request: Request, project_id: str) -> Response:
    """Create or update git repository configuration for a project."""
    config = git_repo_config_service.save_config(project_id=project_id, config_data=request.data)
    return Response(data={"status": "success", "data": config}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.DELETE])
@handle_http_request
def delete_git_config(request: Request, project_id: str) -> Response:
    """Remove git repository configuration (disable versioning)."""
    git_repo_config_service.delete_config(project_id=project_id)
    return Response(
        data={"status": "success", "data": "Git configuration removed successfully."},
        status=status.HTTP_200_OK,
    )


@api_view([HTTPMethods.POST])
@handle_http_request
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
