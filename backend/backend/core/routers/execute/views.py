import re
from typing import Any
from uuid import uuid4

from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response
from visitran.singleton import Singleton
from visitran.errors.base_exceptions import VisitranBaseExceptions
import logging

from backend.application.context.application import ApplicationContext
from backend.errors.visitran_backend_base_exceptions import VisitranBackendBaseException
from backend.core.utils import handle_http_request, sanitize_data
from backend.utils.cache_service.decorators.cache_decorator import clear_cache
from backend.utils.constants import HTTPMethods

_VALID_MODEL_NAME_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")
logger = logging.getLogger(__name__)

@api_view([HTTPMethods.POST])
@handle_http_request
def execute_seed_command(request: Request, project_id: str) -> Response:
    payload = request.data
    environment_id = payload.get("environment", {}).get("id", "")
    app = ApplicationContext(project_id=project_id)
    seed_details = request.data
    Singleton.reset_cache()
    result = app.execute_visitran_seed_command(seed_details, environment_id=environment_id)
    is_table_exist = False
    failed_seeds = list(filter(lambda seed_result: seed_result.get("status") == "Failed", result))
    try:
        is_table_exist = app.is_table_exists_in_db()
    except Exception as _err:
        # Surprising the exceptions if it is failed.
        _ = _err
    response_data = {
        "status": "success" if not failed_seeds else "failed",
        "is_table_exists": is_table_exist,
        "failed_seeds": failed_seeds,
    }
    return Response(
        data=response_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR if failed_seeds else status.HTTP_200_OK
    )


@api_view([HTTPMethods.POST])
@clear_cache(patterns=["model_content_{project_id}_*"])
@handle_http_request
def execute_run_command(request: Request, project_id: str) -> Response:
    payload = request.data
    environment_id = payload.get("environment", {}).get("id", "")
    file_name = payload.get("file_name", "")
    if file_name and not _VALID_MODEL_NAME_RE.match(file_name):
        return Response(
            data={"error_message": "Invalid model name"},
            status=status.HTTP_400_BAD_REQUEST,
        )
    logger.info(f"[execute_run_command] API called - project_id={project_id}, file_name={file_name}, environment_id={environment_id}")
    app = ApplicationContext(project_id=project_id)
    try:
        app.execute_visitran_run_command(current_model=file_name, environment_id=environment_id)
        app.backup_current_no_code_model()
        logger.info(f"[execute_run_command] Completed successfully for file_name={file_name}")
        _data = {"status": "success"}
        return Response(data=_data)
    except (VisitranBaseExceptions, VisitranBackendBaseException) as err:
        logger.exception(f"[execute_run_command] DAG execution failed for file_name={file_name}")
        return Response(data=err.error_response(), status=status.HTTP_400_BAD_REQUEST)
    except Exception:
        logger.exception(f"[execute_run_command] Unexpected error during DAG execution for file_name={file_name}")
        _data = {"status": "failed", "error_message": "An unexpected error occurred while executing the model. Please try again or contact support if the issue persists."}
        return Response(data=_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    finally:
        app.visitran_context.close_db_connection()



@api_view([HTTPMethods.POST])
@handle_http_request
def execute_sql_command(request: Request, project_id: str) -> Response:
    payload = request.data
    sql_models = payload.get("sql_models", "")
    app = ApplicationContext(project_id=project_id)

    # Unique identifier for all tables in this run
    run_id = uuid4().hex[:8]
    created_tables = []

    logger.info(f"[START] SQL execution run_id={run_id} project={project_id}")
    logger.debug(f"Received SQL models: {sql_models}")

    bigquery_proj_id = None

    connection_details= app.get_connection_details()
    if app.connection.datasource_name == 'bigquery':
        bigquery_proj_id = connection_details['project_id']
    try:
        for model in sql_models:
            model_name = model["model_name"]
            schema = model["schema"]
            select_sql = model["model_sql"]

            if bigquery_proj_id:
                table_full_name = f"{bigquery_proj_id}.{schema}.{model_name}"
            else:
                table_full_name = f"{schema}.{model_name}"
            create_sql = f"CREATE TABLE {table_full_name} AS {select_sql}"

            logger.info(f"Creating table: {table_full_name}")
            logger.debug(f"Executing SQL: {create_sql}")

            # Execute SQL
            result = app.execute_sql_command(sql_command=create_sql)
            created_tables.append(table_full_name)

            # Inner failure handling
            if isinstance(result, dict) and result.get("status") == "failed":
                logger.warning(
                    f"Model execution failed for {model_name}. "
                    f"Error: {result.get('error_message')}"
                )
                return Response(result, status=200)

        return Response({"status": "success"}, status=200)

    except Exception as err:
        logger.error(
            f"[EXCEPTION] run_id={run_id} encountered an error: {str(err)}",
            exc_info=True
        )
        return Response({
            "status": "failed",
            "error_message": (
                f'**SQL Transformation Error** failed with error: "{str(err)}".\n'
                f'Review the SQL syntax or the referenced columns and tables.'
            )
        }, status=200)

    finally:
        logger.info(f"[CLEANUP] Dropping created tables for run_id={run_id}")
        for table in created_tables:
            if bigquery_proj_id:
                drop_sql = f"DROP TABLE IF EXISTS {table} ;"
            else:
                drop_sql = f"DROP TABLE IF EXISTS {table} CASCADE;"
            logger.debug(f"Executing cleanup SQL: {drop_sql}")
            try:
                app.execute_sql_command(sql_command=drop_sql)
            except Exception as drop_err:
                logger.warning(f"Failed to drop table {table}: {drop_err}")
