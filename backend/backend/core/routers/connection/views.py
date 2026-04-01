from distutils.util import strtobool
from typing import Any, Union, cast

from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from backend.application.context.connection import ConnectionContext
from backend.core.utils import handle_http_request
from backend.utils.constants import HTTPMethods
from rbac.factory import handle_permission

RESOURCE_NAME = "connectiondetails"


@api_view([HTTPMethods.GET])
@handle_http_request
def get_all_connection(request: Request) -> Response:
    """This method is used to get the project_connection details from the given
    project."""
    con_context = ConnectionContext()
    filter_condition: dict[str, Any] = {}

    page = int(request.GET.get("page", 1))
    limit = int(request.GET.get("limit", 1_000_000))

    connections = con_context.get_all_connections(page=page, limit=limit, filter_condition=filter_condition)
    response_data = {"status": "success", "data": connections}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
def create_connection(request: Request) -> Response:
    request_payload = request.data
    force_create = strtobool(request.query_params.get("force_create", "false"))
    con_context = ConnectionContext()
    connection_data = con_context.create_connection(connection_details=request_payload, force_create=bool(force_create))
    response_data = {"status": "success", "data": connection_data}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_connection(request: Request, connection_id: str) -> Response:
    """This method is used to get the project_connection details from the given
    project."""
    con_context = ConnectionContext()
    connection_data = con_context.get_connection(connection_id=connection_id)
    response_data = {"status": "success", "data": connection_data}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST, HTTPMethods.PUT])
@handle_http_request
@handle_permission
def update_connection(request: Request, connection_id: str) -> Response:
    request_payload = request.data
    con_context = ConnectionContext()
    connection_data: dict[str, Any] = con_context.update_connection(
        connection_id=connection_id, connection_details=request_payload
    )
    response_data = {"status": "success", "data": connection_data}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def test_connection_by_id(request: Request, connection_id: str) -> Response:
    con_context = ConnectionContext()
    con_context.test_connection_by_id(connection_id=connection_id)
    response_data = {"status": "success"}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def connection_dependent_projects(request: Request, connection_id: str) -> Response:
    con_context = ConnectionContext()
    projects_list = con_context.get_connection_dependent_projects(connection_id=connection_id)
    response_data = {"status": "success", "data": projects_list}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def connection_dependent_environments(request: Request, connection_id: str) -> Response:
    con_context = ConnectionContext()
    env_list = con_context.get_connection_dependent_environments(connection_id=connection_id)
    response_data = {"status": "success", "data": env_list}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def connection_usage(request: Request, connection_id: str) -> Response:
    con_context = ConnectionContext()
    proj_and_conn_list = con_context.get_connection_usage(connection_id=connection_id)
    response_data = {"status": "success", "data": proj_and_conn_list}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST, HTTPMethods.DELETE])
@handle_http_request
def delete_connection(request: Request, connection_id: str) -> Response:
    con_context = ConnectionContext()
    con_context.delete_connection(connection_id=connection_id)
    response_data = {
        "status": "success",
        "data": f"{connection_id} is deleted successfully.",
    }
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.DELETE])
@handle_http_request
def delete_all_connections(request: Request) -> Response:
    con_context = ConnectionContext()
    result = con_context.delete_all_connections()
    response_data = {
        "status": "success",
        "data": result,
    }
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def reveal_connection_credentials(request: Request, connection_id: str) -> Response:
    """Return decrypted connection details for the unmask/reveal action."""
    con_context = ConnectionContext()
    credentials = con_context.reveal_connection_credentials(connection_id=connection_id)
    response_data = {"status": "success", "data": credentials}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def test_connection(request: Request) -> Response:
    con_context = ConnectionContext()
    request_data: dict[str, Union[dict[str, Any], str, None]] = request.data
    datasource: str = cast(str, request_data.get("datasource", ""))
    connection_data: dict[str, Any] = cast(dict[str, Any], request_data.get("connection_details", {}))
    connection_id: str = cast(str, request_data.get("connection_id", "")) or None
    con_context.test_connection(datasource=datasource, connection_data=connection_data, connection_id=connection_id)
    return Response(data={"status": "success"}, status=status.HTTP_200_OK)
