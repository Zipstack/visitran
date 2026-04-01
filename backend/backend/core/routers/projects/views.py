import logging
from typing import Any

import yaml
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response
from visitran.errors import TableNotFound

from backend.application.context.application import ApplicationContext
from backend.application.context.formula_context import FormulaContext
from backend.application.context.model_graph import ModelGraph
from backend.application.context.no_code_model import NoCodeModel
from backend.application.context.token_cost_service import TokenCostService
from backend.application.sample_project.dvd_rental_final import DvdRentalProjectFinal
from backend.application.sample_project.dvd_rental_starter import (
    DvdRentalProjectStarter,
)
from backend.application.sample_project.jaffle_shop_final import JaffleShopProjectFinal
from backend.application.sample_project.jaffle_shop_starter import (
    JaffleShopProjectStarter,
)
from backend.application.utils import set_transformation_sequence
from backend.core.utils import handle_http_request, sanitize_data
from backend.errors import CsvDownloadFailed
from backend.utils.cache_service.decorators.cache_decorator import (
    cache_response,
    clear_cache,
)
from backend.utils.constants import HTTPMethods
from backend.utils.tenant_context import get_current_user
from rbac.factory import handle_permission

RESOURCE_NAME = "projectdetails"


def _create_starter_projects_if_needed():
    """Create starter projects for new users if they haven't been created
    yet."""
    logging.info("Starting starter projects creation check")

    try:
        # Get current user
        from pluggable_apps.tenant_account.organization_member_service import OrganizationMemberService

        current_user = get_current_user()

        if not current_user:
            logging.warning("No current user found in context - skipping starter projects creation")
            return

        if not current_user.get("username"):
            logging.warning("Current user has no username - skipping starter projects creation")
            return

        username = current_user.get("username")
        logging.info(f"Checking starter projects for user: {username}")

        # Check if starter projects have been created using service with cache
        if OrganizationMemberService.is_starter_projects_created(username):
            logging.info(f"Starter projects already created for user {username} - skipping creation")
            return

        logging.info(f"Starter projects not created for user {username} - proceeding with creation")

        # Create starter projects
        _create_starter_projects()

        # Mark as created using service (updates both DB and cache)
        OrganizationMemberService.mark_starter_projects_created(username)

        logging.info(f"Successfully created and marked starter projects for user {username}")

    except Exception as e:
        logging.error(f"Error creating starter projects: {str(e)}", exc_info=True)
        # Don't raise exception to avoid breaking the project list API


def _create_starter_projects():
    """Create starter projects using mapper."""
    logging.info("Starting creation of starter projects")

    # Mapper for starter projects only
    starter_project_mapper = {
        "dvd_starter": DvdRentalProjectStarter,
        "jaffleshop_starter": JaffleShopProjectStarter,
    }

    logging.info(f"Will create {len(starter_project_mapper)} starter projects: {list(starter_project_mapper.keys())}")

    for project_key, project_class in starter_project_mapper.items():
        logging.info(f"Creating {project_key} starter project")

        try:
            project_loader = project_class()
            sample_project_data = project_loader.load_sample_project()

            # Enable onboarding and set project type for this project
            from backend.core.models.project_details import ProjectDetails
            from backend.application.utils import get_filter

            project_id = sample_project_data.get("project_id")
            if not project_id:
                logging.warning(f"No project_id returned for {project_key} - skipping project configuration")
                continue

            logging.info(f"Configuring project {project_key} with ID: {project_id}")

            filter_condition = get_filter()
            filter_condition["project_uuid"] = project_id

            project = ProjectDetails.objects.get(**filter_condition)
            project.onboarding_enabled = True
            project.project_type = project_key
            project.save()

            logging.info(f"Successfully created and configured {project_key} starter project with ID: {project_id}")

        except Exception as e:
            logging.error(f"Error creating {project_key} starter project: {str(e)}", exc_info=True)

    logging.info("Completed starter projects creation process")


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
def create_project(request: Request):
    """This API is used to create a new project."""
    project_details = request.data
    create_and_get_project_id = ApplicationContext.create_project(project_details)
    _data = {
        "status": "success",
        "data": f"{project_details['project_name']} is created",
        "project_id": create_and_get_project_id,
    }
    return Response(data=_data, status=status.HTTP_201_CREATED)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_projects_list(request: Request) -> Response:
    """Return paginated, searchable list of projects.

    Query params:
        search   – filter by project name (case-insensitive contains)
        page     – 1-based page number (default 1)
        page_size – items per page (default 20)
    """
    # Check if starter projects need to be created for this user
    # _create_starter_projects_if_needed()

    search = request.query_params.get("search", "").strip()
    try:
        page = max(int(request.query_params.get("page", 1)), 1)
    except (ValueError, TypeError):
        page = 1
    try:
        page_size = min(max(int(request.query_params.get("page_size", 20)), 1), 100)
    except (ValueError, TypeError):
        page_size = 20
    sort_by = request.query_params.get("sort_by", "modified")

    project_list = ApplicationContext.get_project_lists(
        search=search, page=page, page_size=page_size, sort_by=sort_by
    )
    return Response(data=project_list, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def create_sample_project(request) -> Response:
    load_project_name = request.data.get("template", "jaffleshop_final")

    _mapper = {
        "dvd_starter": DvdRentalProjectStarter,
        "dvd_final": DvdRentalProjectFinal,
        "jaffleshop_starter": JaffleShopProjectStarter,
        "jaffleshop_final": JaffleShopProjectFinal,
    }
    if load_project_name not in _mapper:
        raise ValueError(
            "Invalid project name specified, Please select valid project to load"
        )

    project_loader = _mapper[load_project_name]

    sample_project = project_loader()

    # create connection with postgres for project
    sample_project_data = sample_project.load_sample_project()

    # Set project_type for all sample projects; enable onboarding for starters
    from backend.core.models.project_details import ProjectDetails
    from backend.application.utils import get_filter

    project_id = sample_project_data.get("project_id")
    if project_id:
        filter_condition = get_filter()
        filter_condition["project_uuid"] = project_id
        try:
            project = ProjectDetails.objects.get(**filter_condition)
            project.project_type = load_project_name
            if load_project_name in ["jaffleshop_starter", "dvd_starter"]:
                project.onboarding_enabled = True
            project.save()
        except ProjectDetails.DoesNotExist:
            pass  # Project not found, skip configuration

    _data = {"status": "success", "data": sample_project_data}
    return Response(data=_data, status=status.HTTP_201_CREATED)


@api_view([HTTPMethods.GET])
@handle_http_request
@handle_permission
def get_project_detail(request: Request, project_id: str):
    """This API is used to create a new project."""
    app = ApplicationContext(project_id=project_id)
    response = app.get_project_details()
    _data = {"status": "success", "data": response}
    return Response(data=_data, status=status.HTTP_201_CREATED)


@api_view([HTTPMethods.DELETE])
@handle_http_request
@handle_permission
def delete_project(request: Request, project_id: str):
    app = ApplicationContext(project_id=project_id)
    project_name = app.project_instance.project_name
    app.delete_project()
    _data = {
        "status": "success",
        "data": f"Project `{project_name}` is deleted successfully!",
    }
    return Response(data=_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.PUT])
@handle_http_request
@handle_permission
def update_project(request: Request, project_id: str):
    """This API is used to create a new project."""
    project_details = request.data
    app = ApplicationContext(project_id=project_id)
    app.update_a_project(project_details)
    _data = {"status": "success", "data": f"{project_id} is updated"}
    return Response(data=_data, status=status.HTTP_201_CREATED)


@api_view([HTTPMethods.GET])
@handle_http_request
def check_project_existence(request: Request):
    """This API will validate the project name from the request payload."""
    project_name = request.GET.get("project_name")
    ApplicationContext.check_project_existence(project_name=project_name)
    _data = {"status": "success", "data": f"{project_name} is valid to create"}
    return Response(data=_data)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_lineage(request: Request, project_id: str) -> Response:
    """This API is used to get the lineage of the project."""
    model_graph = ModelGraph(project_id=project_id)
    response_data = model_graph.get_lineage_relation()
    _data = {"status": "success", "data": response_data}
    return Response(data=_data)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_lineage_info(request: Request, project_id: str, model_name: str) -> Response:
    """This API is used to get the node info from the lineage."""
    # Get type parameter (default to 'sql')
    content_type = request.query_params.get("type", "sql")

    app = ApplicationContext(project_id=project_id)
    table_details = app.get_lineage_model_details(
        model_name=model_name, content_type=content_type
    )
    _data = {"status": "success", "data": table_details}
    return Response(data=_data)


@api_view([HTTPMethods.GET])
@handle_http_request
@cache_response(
    key_prefix="model_content", key_params=["project_id", "model_name", "page", "limit"]
)
def get_model_file_content(
    request: Request, project_id: str, model_name: str
) -> Response:
    """This API is used to retrieve the model table content inside the
    project."""

    app = ApplicationContext(project_id=project_id)
    model_name = model_name.replace(" ", "_").strip()
    page = request.query_params.get("page", 1)
    limit = request.query_params.get("limit", 100)


    try:
        response_data = app.get_model_content(
            model_name, page=int(page), limit=int(limit)
        )
    except TableNotFound as table_err:
        logging.warning(f"Table not found, Making run call to make sure the table is recreated - {str(table_err)}")
        app.execute_visitran_run_command()
        app.backup_current_no_code_model()
        response_data = app.get_model_content(
            model_name, page=int(page), limit=int(limit)
        )
        app.visitran_context.close_db_connection()

        TokenCostService.get_session_summary(session_id="1")

    sanitized_data = sanitize_data(response_data)
    return Response(data=sanitized_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def export_model_content_csv(
    request: Request, project_id: str, model_name: str
) -> Response:
    """Export full model content for CSV download without pagination.

    Args:
        request: HTTP request object
        project_id: ID of the project
        model_name: Name of the model to export

    Returns:
        Response containing all model data for CSV export
    """
    try:
        # Initialize application context
        application_context = ApplicationContext(project_id=project_id)

        # Get full model content for export
        export_data = application_context.get_full_model_content_for_export(
            model_name=model_name
        )

        # Structure the response to match frontend expectations
        response_data = {
            "content": export_data["content"],
            "total": export_data["total_records"],
            "hidden_columns": export_data["hidden_columns"],
            "schema_name": export_data["schema_name"],
            "table_name": export_data["table_name"],
        }

        return Response(data=response_data, status=200)

    except CsvDownloadFailed as e:
        # Re-raise the exception to let the framework handle it
        raise e
    except Exception as e:
        # Handle any other unexpected errors
        return Response(
            {"error": f"Failed to export CSV: {str(e)}", "error_type": "general_error"},
            status=500,
        )

@api_view([HTTPMethods.POST])
@handle_http_request
def set_project_schema(request: Request, project_id: str) -> Response:
    app = ApplicationContext(project_id=project_id)
    schema_name = request.data.get("schema_name")
    app.project_instance.project_schema = schema_name
    app.project_instance.save()
    return Response(data={"status": "success"}, status=status.HTTP_200_OK)

@api_view([HTTPMethods.GET])
@handle_http_request
def get_project_schemas(request: Request, project_id: str) -> Response:
    app = ApplicationContext(project_id=project_id)

    try:
        # Retrieving all the schemas below
        _schema_details = app.get_all_schemas()

        _schemas_names: list[str] = []
        _schema_description: dict[str, Any] = {}
        default_project_schema = app.visitran_context.get_profile_schema()

        # Grouping up schema with tables below
        for _schema_name in _schema_details:
            _schemas_names.append(_schema_name)

        # Constructing response here
        data = {
            "schema_names": _schemas_names,
            "schema_description": _schema_description,
            "default_project_schema": default_project_schema,
        }
    except Exception:
        data = {"schema_names": ["default"], "default_project_schema": "default"}
    return Response(data=data, status=status.HTTP_200_OK)


def _get_schema_tables(
    app: ApplicationContext, schema_name: str = ""
) -> tuple[list[str], list[str]]:
    """Process tables for a schema and return table_names_list and full table
    names."""
    table_names_list = []
    full_table_names = []
    table_names = app.get_all_tables(schema_name=schema_name) or []

    for table_name in table_names:
        table_names_list.append(table_name)
        schema_and_table_name = (
            f"{schema_name}.{table_name}" if schema_name else table_name
        )
        full_table_names.append(schema_and_table_name)

    return table_names_list, full_table_names


def _get_filtered_tables(app: ApplicationContext, unsupported_tables) -> dict[str, Any]:
    """Process tables for a schema and return table_names_list and full table
    names."""
    try:
        schema_table_names: list[str] = []
        schema_description: dict[str, Any] = {}

        schema_names: list[str] = app.get_all_schemas()
        default_project_schema = app.visitran_context.get_profile_schema()

        # Process schemas and their tables
        for schema_name in schema_names:
            table_list, schema_table_list = _get_schema_tables(app, schema_name)
            schema_description[schema_name] = {
                "table_schema": schema_name,
                "tables": table_list,
            }
            schema_table_names.extend(schema_table_list)

        # Filter out unsupported tables
        filtered_table_names = [
            table
            for table in schema_table_names
            if table.split(".")[-1] not in unsupported_tables
        ]

        data = {
            "schema_names": schema_names,
            "schema_description": "schema_description",
            "default_project_schema": default_project_schema,
            "table_names": filtered_table_names,
        }

    except NotImplementedError:
        # Handle databases without schema support
        _, table_names = _get_schema_tables(app)
        data = {
            "table_names": [
                name for name in table_names if name not in unsupported_tables
            ]
        }
    return data


@api_view([HTTPMethods.GET])
@handle_http_request
def get_project_schemas_and_tables(request: Request, project_id: str) -> Response:
    app = ApplicationContext(project_id=project_id)

    model_name: str = request.GET.get("model")
    unsupported_tables = set()
    if model_name:
        unsupported_tables = _unsupported_tables(app, model_name)

    data = _get_filtered_tables(app, unsupported_tables)
    return Response(data=data, status=status.HTTP_200_OK)


def _unsupported_tables(app, model_name):
    reference_models = app.get_model_reference_details(model_name=model_name)
    all_models = app.get_all_model_details()
    unsupported_models: list[str] = list(
        set(all_models.keys()) - set(reference_models.keys())
    )
    unsupported_tables = {
        all_models[model_name].get("destination_table")
        for model_name in unsupported_models
    }
    return unsupported_tables


@api_view([HTTPMethods.GET])
@handle_http_request
def get_project_table_columns(
    request, schema_name: str, project_id: str, table_name: str
) -> Response:
    if schema_name == "~":
        schema_name = ""

    column_type = request.GET.get("type", "")
    app = ApplicationContext(project_id=project_id)
    column_details: list[Any] = app.get_table_columns(schema_name, table_name)
    column_description: dict[str, Any] = {}
    column_names: list[str] = []

    if column_type:
        for column in column_details:
            if column_type == column["column_dbtype"]:
                column_description[column["column_name"]] = column
                column_names.append(column["column_name"])
    else:
        for column in column_details:
            column_description[column["column_name"]] = column
            column_names.append(column["column_name"])

    response_data = {
        "table_name": table_name,
        "schema_name": schema_name,
        "column_description": column_description,
        "column_names": column_names,
    }

    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_project_table_content(
    request, schema_name: str, project_id: str, table_name: str
) -> Response:
    if schema_name == "~":
        schema_name = ""

    if table_name == "null":
        response_data = {
            "content": [],
            "schema_name": schema_name,
            "column_description": "",
            "column_names": "",
        }
        return Response(data=response_data, status=status.HTTP_200_OK)

    app = ApplicationContext(project_id=project_id)
    column_details: list[Any] = app.get_table_columns(schema_name, table_name)
    column_description: dict[str, Any] = {}
    column_names: list[str] = []

    for column in column_details:
        column_description[column["column_name"]] = column
        column_names.append(column["column_name"])

    hidden_columns = []
    table_records = app.visitran_context.get_table_records(
        schema_name=schema_name,
        table_name=table_name,
        selective_columns=hidden_columns,
        limit=1000,
        page=1,
    )
    response_data = {
        "content": table_records,
        "schema_name": schema_name,
        "column_description": column_description,
        "column_names": column_names,
    }

    return Response(data=response_data, status=status.HTTP_200_OK)


def _get_table_name_and_description(
    table_details: list, schema_name: str, destination_table_name: str
) -> tuple[list[str], dict[str, Any]]:
    """Process regular tables, returning table names and descriptions."""
    table_names = []
    table_description = {}

    for table in table_details:
        if not isinstance(table, str):
            (table,) = table
        if table == destination_table_name:
            continue

        # Add to table description
        table_description[table] = table
        qualified_name = f"{schema_name}.{table}" if schema_name else table
        table_names.append(qualified_name)

    return table_names, table_description


@api_view([HTTPMethods.GET])
@handle_http_request
def get_project_tables(request: Request, project_id: str, schema_name: str) -> Response:
    """Returns the list of table names in the given schema."""
    app = ApplicationContext(project_id=project_id)
    is_merge_type = request.GET.get("type") == "merge"
    destination_table_name = ""
    if is_merge_type:
        model = request.GET.get("model")
        destination_table_name = app.get_destination_table_name(model_name=model)

    table_details = app.get_all_tables(schema_name=schema_name)
    table_names, table_description = _get_table_name_and_description(
        table_details, schema_name, destination_table_name
    )

    data = {
        "schema_name": schema_name or "default",
        "table_description": table_description,
        "table_names": set(table_names),
    }
    return Response(data=data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.PUT, HTTPMethods.GET])
@handle_http_request
def get_supported_models(request: Request, project_id: str, file_name: str) -> Response:
    request_data = request.GET
    current_references: list[str] = request_data.get("selected_references") or []
    if isinstance(current_references, str):
        current_references = current_references.split(",")
    app = ApplicationContext(project_id=project_id)
    supported_reference_models = app.get_supported_reference_models(
        current_model=file_name, current_references=current_references
    )
    response_data = {"supported_reference_models": supported_reference_models}
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_table_schema(request, project_id: str) -> Response:
    app = ApplicationContext(project_id=project_id)
    table_schema_details = app.get_table_schema()
    return Response(table_schema_details)


@api_view([HTTPMethods.GET])
@handle_http_request
def reload_model(request: Request, project_id: str) -> Response:
    # By default, the files will be uploaded in seeds path as of now
    app = ApplicationContext(project_id=project_id)
    query_params = request.query_params
    model_name: str = query_params.get("file_name", "").replace(" ", "_").strip()
    model_data = app.reload_model(model_name)
    sequence_orders = sequence_lineage = transformation_details = ""
    if model_data:
        _parser = app.get_config_parser(model_data, model_name)
        data = yaml.dump(model_data, default_flow_style=False, sort_keys=False)
        sequence_orders, sequence_lineage = set_transformation_sequence(_parser)
        transformation_details = app._get_transformation_details(model_data, sequence_orders)
    else:
        data = ""

    response_json = {
        "project_name": project_id,
        "yaml": data,
        "sequence_orders": sequence_orders,
        "sequence_lineage": sequence_lineage,
        "model_data": model_data,
        "transformation_details": transformation_details,
    }
    return Response(data=response_json)


@api_view([HTTPMethods.GET])
@handle_http_request
def rollback_model_file_content(
    request: Request, project_id: str, model_name: str
) -> Response:
    # This method is used to save the model file inside the project
    app = ApplicationContext(project_id=project_id)
    data = app.rollback_model_content(model_name=model_name)
    response_json = {"project_name": project_id, "yaml": data}
    return Response(data=response_json, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def save_model_file(request: Request, project_id: str, file_name: str) -> Response:
    # This method is used to save the model file inside the project
    # This API is depreciated and will be removed in next release
    request_data = request.data
    file_name = file_name.replace(" ", "_")
    app = ApplicationContext(project_id=project_id)
    sequence_orders, sequence_lineage = app.save_model_file(
        request_data, model_name=file_name, is_chat_response=False
    )
    response_json = {
        "status": "success",
        "sequence_orders": sequence_orders,
        "sequence_lineage": sequence_lineage,
    }
    return Response(data=response_json)


@api_view([HTTPMethods.POST])
@clear_cache(patterns=["model_content_{project_id}_*"])
@handle_http_request
def set_model_config_and_reference(
    request: Request, project_id: str, file_name: str
) -> Response:
    """API to set the configuration for a given model.

    ### Payload Structure:
    - **model_name** (str): The name of the model being configured.
    - **model_config** (dict):
        - **source** (dict):
            - **schema_name** (str): The name of the source schema.
            - **table_name** (str): The name of the source table.
            - **materialization** (str): Materialization type, e.g., 'TABLE'.
        - **model** (dict):
            - **schema_name** (str): The schema name for the model.
            - **table_name** (str): The table name for the model.

    ### Expected Payload Example:
    ```json
    {
      "model_name": "dev_customers",
      "model_config": {
        "source": {
          "schema_name": "raw",
          "table_name": "raw_customers",
          "materialization": "TABLE"
        },
        "model": {
          "schema_name": "dev",
          "table_name": "customers"
        }
      }
    }
    ```

    ### Response:
    - **status** (str): Status of the operation, e.g., 'success'.
    - **sequence_orders**: Data regarding transformation sequence orders.
    - **sequence_lineage**: Data regarding sequence lineage.

    ### Functionality:
    This method processes the model configuration by saving the provided YAML structure,
    validating, and updating model information. It generates transformation sequences
    and lineage details for the given configuration.

    ### Example Response:
    ```json
    {
      "status": "success",
      "sequence_orders": ["order1", "order2"],
      "sequence_lineage": ["lineage1", "lineage2"]
    }
    ```
    """
    request_data = request.data
    file_name = file_name.replace(" ", "_")
    no_code_model = NoCodeModel(project_id=project_id)
    response_json = no_code_model.set_model_config_and_reference(
        request_data, model_name=file_name
    )
    response_json["status"] = "success"
    return Response(data=response_json)


@api_view([HTTPMethods.POST])
@clear_cache(patterns=["model_content_{project_id}_*"])
@handle_http_request
def set_model_transformation(
    request: Request, project_id: str, file_name: str
) -> Response:
    # This method is used to save the model file inside the project
    request_data = request.data
    file_name = file_name.replace(" ", "_")
    no_code_model = NoCodeModel(project_id=project_id)
    response_json = no_code_model.set_model_transformation(
        request_data, model_name=file_name
    )
    response_json["status"] = "success"
    return Response(data=response_json)


@api_view([HTTPMethods.DELETE])
@clear_cache(patterns=["model_content_{project_id}_*"])
@handle_http_request
def delete_model_transformation(
    request: Request, project_id: str, file_name: str
) -> Response:
    # This method is used to remove a transformation
    request_data = request.data
    file_name = file_name.replace(" ", "_")
    no_code_model = NoCodeModel(project_id=project_id)
    transformation_id = request_data.get("step_id")
    is_clear_all: bool = request_data.get("clear_all", False)
    response_json = no_code_model.delete_model_transformation(
        model_name=file_name,
        transformation_id=transformation_id,
        is_clear_all=is_clear_all,
    )
    response_json["status"] = "success"
    return Response(data=response_json)


@api_view([HTTPMethods.POST])
@clear_cache(patterns=["model_content_{project_id}_*"])
@handle_http_request
def set_model_presentation(
    request: Request, project_id: str, file_name: str
) -> Response:
    # This method is used to set or unset the presentation
    request_data = request.data
    file_name = file_name.replace(" ", "_")
    no_code_model = NoCodeModel(project_id=project_id)
    response_json = no_code_model.set_model_presentation(
        no_code_data=request_data, model_name=file_name
    )
    response_json["status"] = "success"
    return Response(data=response_json)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_transformation_columns(
    request: Request, project_id: str, file_name: str
) -> Response:
    # This method is used to set or unset the presentation
    transformation_id = request.query_params.get("transformation_id")
    transformation_type = (
        request.query_params.get("transformation_type", "current") or "current"
    )
    model_name = file_name.replace(" ", "_")
    no_code_model = NoCodeModel(project_id=project_id)
    response_json = no_code_model.get_transformation_columns(
        model_name=model_name,
        transformation_id=transformation_id,
        transformation_type=transformation_type,
    )
    response_json["status"] = "success"
    return Response(data=response_json)


@api_view([HTTPMethods.POST])
@handle_http_request
def validate_model_file(request: Request, project_id: str, file_name: str) -> Response:
    # This method is used to validate the model file inside the project
    request_data = request.data
    file_name = file_name.replace(" ", "_")
    app = ApplicationContext(project_id=project_id)
    app.validate_model_file(request_data, model_name=file_name)
    response_json = {"status": "success"}
    return Response(data=response_json)


@api_view([HTTPMethods.POST])
@handle_http_request
def write_database_file(request: Request, project_id: str) -> Response:
    # By default, the files will be uploaded in seeds path as of now
    # request_data = request.data
    database_file = request.FILES["file"]
    app = ApplicationContext(project_id=project_id)
    app.write_database_file(database_file=database_file)
    response_json = {"status": "success"}
    return Response(data=response_json)


@api_view([HTTPMethods.POST])
@handle_http_request
def generate_formula(request: Request, project_id: str, model_name: str) -> Response:
    # Generate Excel Formula based on the User prompt with OpenAi support
    user_prompt = request.data["user_prompt"]
    app = FormulaContext(project_id=project_id)

    # Get schema details and construct the prompt
    schema_details = app.get_schema_details(model_name)
    constructed_prompt = app.construct_prompt(user_prompt, schema_details)

    # Generate the formula
    formula_result = app.generate_formula(constructed_prompt)

    if not formula_result:
        return Response(
            data={"status": "error", "message": "Failed to generate formula."},
            status=500,
        )

    # Construct response
    response_json = {
        "status": "success",
        "data": {
            "schema_details": schema_details,
            "formula": formula_result,
        },
    }
    return Response(data=response_json)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_sql_flow(request: Request, project_id: str) -> Response:
    """Get table-level lineage (SQL Flow) for the entire project. Shows JOIN
    relationships between tables across all models in an ER-diagram style
    visualization with column-level join indicators.

    Returns:
        - nodes: List of table cards with schema, columns, and join key indicators
        - edges: List of edges connecting specific columns between tables
        - stats: Summary statistics (total tables, joins, schemas, etc.)
    """
    from backend.application.context.sql_flow import SQLFlowGenerator

    generator = SQLFlowGenerator(project_id=project_id)
    flow_data = generator.generate_flow()

    return Response(data=flow_data, status=status.HTTP_200_OK)


# ===== TRANSFORMATION VERSIONING API ENDPOINTS =====
