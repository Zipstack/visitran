from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from backend.application.context.application import ApplicationContext
from backend.application.context.formula_context import FormulaContext
from backend.application.context.no_code_model import NoCodeModel
from backend.core.utils import handle_http_request
from backend.utils.cache_service.decorators.cache_decorator import clear_cache
from backend.utils.constants import HTTPMethods
from rbac.factory import handle_permission
from visitran.events.functions import fire_event
from visitran.events.types import TransformationApplied, TransformationDeleted, ModelConfigured

RESOURCE_NAME = "configmodels"


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
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
@handle_http_request
@handle_permission
@clear_cache(patterns=["model_content_{project_id}_*"])
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
    model_config = request_data.get("model_config", {})
    src = model_config.get("source", {})
    dest = model_config.get("model", {})
    fire_event(ModelConfigured(
        model_name=file_name,
        source=f"{src.get('schema_name', '')}.{src.get('table_name', '')}",
        destination=f"{dest.get('schema_name', '')}.{dest.get('table_name', '')}",
    ))
    return Response(data=response_json)


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
@clear_cache(patterns=["model_content_{project_id}_*"])
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
    step_config = request_data.get("step_config", {})
    transformation_type = step_config.get("type", "unknown") if isinstance(step_config, dict) else "unknown"
    fire_event(TransformationApplied(
        model_name=file_name,
        transformation_type=transformation_type,
    ))
    return Response(data=response_json)


@api_view([HTTPMethods.DELETE])
@handle_http_request
@handle_permission
@clear_cache(patterns=["model_content_{project_id}_*"])
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
    fire_event(TransformationDeleted(
        model_name=file_name,
        transformation_type="all" if is_clear_all else (transformation_id or "unknown"),
    ))
    return Response(data=response_json)


@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
@clear_cache(patterns=["model_content_{project_id}_*"])
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
    # This method is used to get available columns in a transformation step
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
@handle_permission
def validate_model_file(request: Request, project_id: str, file_name: str) -> Response:
    # This method is used to validate the model file inside the project
    request_data = request.data
    file_name = file_name.replace(" ", "_")
    app = ApplicationContext(project_id=project_id)
    app.validate_model_file(request_data, model_name=file_name)
    response_json = {"status": "success"}
    return Response(data=response_json)


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



@api_view([HTTPMethods.POST])
@handle_http_request
@handle_permission
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
