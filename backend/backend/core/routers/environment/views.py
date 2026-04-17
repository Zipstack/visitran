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
from visitran.events.functions import fire_event
from visitran.events.types import EnvironmentCreated, EnvironmentDeleted

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
    fire_event(EnvironmentCreated(
        environment_name=request_payload.get("name", ""),
    ))
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
    from backend.core.models.environment_models import EnvironmentModels
    from backend.errors.validation_exceptions import EnvironmentInUse

    env_context = EnvironmentContext()
    env_name = environment_id
    try:
        env_obj = EnvironmentModels.objects.get(environment_id=environment_id)
        env_name = env_obj.environment_name or environment_id
    except EnvironmentModels.DoesNotExist:
        pass

    try:
        env_context.delete_environment(environment_id=environment_id)
    except ProtectedError as e:
        job_names = [
            obj.task_name
            for obj in e.protected_objects
            if obj._meta.label.split(".")[0] == "job_scheduler"
        ]
        raise EnvironmentInUse(
            environment_name=env_name,
            job_names=", ".join(job_names) if job_names else "unknown",
        )

    fire_event(EnvironmentDeleted(environment_name=env_name))
    response_data = {"status": "success"}
    return Response(data=response_data, status=status.HTTP_200_OK)


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
