from django.urls import path

from backend.core.routers.transformation.views import (
    delete_model_transformation,
    generate_formula,
    get_supported_models,
    get_transformation_columns,
    save_model_file,
    set_model_config_and_reference,
    set_model_presentation,
    set_model_transformation,
    validate_model_file,
)

# Mounted at: project/<project_id>/no_code_model/
urlpatterns = [
    path("<str:file_name>/validate", validate_model_file, name="validate-no-code-model-file"),
    path("<str:file_name>/set-model", set_model_config_and_reference, name="set-no-code-model-config"),
    path("<str:file_name>/set-transform", set_model_transformation, name="set-no-code-model-transformation"),
    path("<str:file_name>/delete-transform", delete_model_transformation, name="delete-no-code-model-transformation"),
    path("<str:file_name>/set-presentation", set_model_presentation, name="set-no-code-model-presentation"),
    path("<str:file_name>/columns", get_transformation_columns, name="get-transformation-columns"),
    path("<str:file_name>/supported_references", get_supported_models, name="get-supported-reference-models"),
    path("<str:model_name>/generate_formula", generate_formula, name="generate-formula"),
    path("<str:file_name>", save_model_file, name="save-no-code-model-file"),
]
