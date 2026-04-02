from typing import Any

from django.db.models import ProtectedError
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from backend.application.context.environment import EnvironmentContext
from backend.application.utils import test_connection_data
from backend.core.utils import handle_http_request
from backend.utils.constants import HTTPMethods
from rbac.factory import handle_permission

RESOURCE_NAME = "environmentmodels"


@api_view([HTTPMethods.GET])
@handle_http_request
def get_all_environments(request: Request) -> Response:
    """This method is used to get the project_connection details from the given
    project."""
    env_context = EnvironmentContext()
    page = int(request.GET.get("page", 1))
    limit = int(request.GET.get("limit", 1_000_000))
    env_list: list[dict[str, Any]] = env_context.get_all_environments(
        page=page, limit=limit
    )
    response_data = {"status": "success", "data": env_list}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_environment(request, environment_id: str) -> Response:
    env_context = EnvironmentContext()
    env_data: dict[str, Any] = env_context.get_environment(
        environment_id=environment_id
    )
    response_data = {"status": "success", "data": env_data}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
def create_environment(request) -> Response:
    request_payload = request.data
    env_context = EnvironmentContext()
    env_data: dict[str, Any] = env_context.create_environment(
        environment_details=request_payload
    )
    response_data = {"status": "success", "data": env_data}
    return Response(data=response_data, status=status.HTTP_201_CREATED)


@api_view([HTTPMethods.PUT])
@handle_http_request
@handle_permission
def update_environment(request, environment_id: str) -> Response:
    request_payload = request.data
    env_context = EnvironmentContext()
    env_data = env_context.update_environment(
        environment_id=environment_id, environment_details=request_payload
    )
    response_data = {"status": "success", "data": env_data}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.DELETE])
@handle_http_request
@handle_permission
def delete_environment(request: Request, environment_id: str):
    env_context = EnvironmentContext()
    try:
        env_context.delete_environment(environment_id=environment_id)
        response_data = {"status": "success"}
        return Response(data=response_data, status=status.HTTP_200_OK)
    except ProtectedError as e:
        protected_objects = e.protected_objects
        blocked_apps = set()
        blocked_data = {}
        for obj in protected_objects:
            app_name = obj._meta.label.split(".")[
                0
            ]  # Extracts "appname. model_name can also be extracted like _meta.model_name"
            if app_name == "job_scheduler":
                key = "Deploy"
                if key not in blocked_data:
                    blocked_data[key] = []
                blocked_data[key] = obj.task_name
            blocked_apps.add(app_name)
        error_details = []
        for model, ids in blocked_data.items():
            error_details.append(f"{ids} from '{model}'")
        error_message = f"Cannot delete this environment record because it is referenced by: {', '.join(error_details)}."
        data = {
            "message": error_message,
            "status": "failed",
        }
        return Response(data=data, status=status.HTTP_400_BAD_REQUEST)


@api_view([HTTPMethods.GET])
@handle_http_request
def reveal_environment_credentials(request: Request, environment_id: str) -> Response:
    """Return decrypted environment connection details for the reveal action."""
    env_context = EnvironmentContext()
    credentials = env_context.reveal_environment_credentials(environment_id=environment_id)
    response_data = {"status": "success", "data": credentials}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def environment_dependent_projects(request: Request, environment_id: str):
    env_context = EnvironmentContext()
    projects_list = env_context.get_environment_dependent_projects(
        environment_id=environment_id
    )
    response_data = {"status": "success", "data": projects_list}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
def test_environment(request: Request):
    request_data: dict[str, Any] = request.data
    datasource: str = request_data.get("datasource")
    connection_data: dict[str, Any] = request_data.get("connection_details")

    # Decrypt sensitive fields from frontend encrypted data
    from backend.utils.decryption_utils import decrypt_sensitive_fields
    if connection_data:
        decrypted_connection_data = decrypt_sensitive_fields(connection_data)
        test_connection_data(datasource=datasource, connection_data=decrypted_connection_data)
    else:
        test_connection_data(datasource=datasource, connection_data=connection_data)

    return Response(data={"status": "success"}, status=status.HTTP_200_OK)
