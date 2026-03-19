from typing import Any

from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response
from visitran.utils import get_adapter_connection_fields

from backend.application.context.application import ApplicationContext
from backend.core.utils import handle_http_request
from backend.utils.constants import HTTPMethods
from backend.errors.exceptions import (
    ProjectConnectionGetFailed,
    ProjectConnectionUpdateFailed,
    ProjectConnectionTestFailed,
    ProjectConnectionMissingField,
    ProjectConnectionInvalidData
)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_connection(request: Request, project_id: str) -> Response:
    """This method is used to get the project_connection details from the given
    project."""
    try:
        app = ApplicationContext(project_id=project_id)

        connection_details = app.get_connection_details()
        schema_name = app.visitran_context.schema_name
        if app.connection.datasource_name == 'bigquery':
            connection_details["dataset_id"] = schema_name
        else:
            connection_details["schema"] = schema_name
        is_table_exist = False
        try:
            is_table_exist = app.is_table_exists_in_db()
        except Exception as _err:
            # Surprising the exceptions if it is failed.
            _ = _err
        response_data = {
            "project_name": app.project_instance.project_name,
            "datasource_name": app.project_instance.database_type,
            "connection_id": app.project_instance.connection_model.connection_id,
            "connection_name": app.project_instance.connection_model.connection_name,
            "connection_details": connection_details,
            "is_table_exists": is_table_exist,
        }

        if {"schema", "dataset_id"}.intersection(connection_details.keys()):
            response_data["is_schema_exists"] = True

        return Response(data=response_data, status=status.HTTP_200_OK)
    except Exception as e:
        raise ProjectConnectionGetFailed(project_id=project_id, error_message=str(e))


@api_view([HTTPMethods.POST, HTTPMethods.PUT])
@handle_http_request
def update_connection(request, project_id: str, datasource: str) -> Response:
    try:
        request_payload = request.data
        app = ApplicationContext(project_id=project_id)
        datasource_name = datasource or app.visitran_context.database_type
        connection_details = app.update_connection_details(connection_details=request_payload["connection_details"])
        extracted_details = connection_details[datasource_name]
        connection_fields = get_adapter_connection_fields(adapter_name=datasource_name)
        response_data = {
            "project_name": project_id,
            "datasource_name": datasource_name,
            "connection_details": extracted_details,
            "is_schema_exists": "schema" in connection_fields.keys(),
            "is_table_exists": app.is_table_exists_in_db(),
        }
        return Response(data=response_data, status=status.HTTP_200_OK)
    except KeyError as e:
        raise ProjectConnectionMissingField(field_name=str(e))
    except Exception as e:
        raise ProjectConnectionUpdateFailed(project_id=project_id, error_message=str(e))


@api_view([HTTPMethods.GET, HTTPMethods.PUT])
@handle_http_request
def test_connection(request: Request, project_id: str, datasource: str):
    try:
        app = ApplicationContext(project_id=project_id)
        request_data: dict[str, Any] = request.data
        if request_data:
            app.test_connection_details_with_data(
                connection_details=request_data["connection_details"], datasource=datasource
            )
        else:
            app.test_connection_details()
        return Response(data={"status": "success"}, status=status.HTTP_200_OK)
    except KeyError as e:
        raise ProjectConnectionMissingField(field_name=str(e))
    except Exception as e:
        raise ProjectConnectionTestFailed(project_id=project_id, error_message=str(e))
