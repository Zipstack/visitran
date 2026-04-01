from django.conf.urls import include
from django.urls import path

from backend.core.routers.projects.views import (
    check_project_existence,
    create_project,
    create_sample_project,
    delete_model_transformation,
    delete_project,
    export_model_content_csv,
    generate_formula,
    get_lineage,
    get_lineage_info,
    get_model_file_content,
    get_project_detail,
    get_project_schemas,
    get_project_schemas_and_tables,
    get_project_table_columns,
    get_project_table_content,
    get_project_tables,
    get_projects_list,
    get_sql_flow,
    get_supported_models,
    get_table_schema,
    get_transformation_columns,
    reload_model,
    rollback_model_file_content,
    save_model_file,
    set_model_config_and_reference,
    set_model_presentation,
    set_model_transformation,
    set_project_schema,
    update_project,
    validate_model_file,
    write_database_file,
)

# This API will initialize a new visitran project,
# The name of the project would be obtained using payload
CREATE_NEW_PROJECT = path("/create", create_project, name="create-project")


# List all the projects for the current user
LIST_ALL_PROJECTS = path("s", get_projects_list, name="get-projects-list")


# This API is used to load a sample project
CREATE_SAMPLE_PROJECT = path("/sample-create", create_sample_project, name="create-sample-project")

# get an existing project detail
GET_PROJECT_DETAIL = path("/<str:project_id>", get_project_detail, name="get-project")

# update an existing project model
UPDATE_A_PROJECT = path("/<str:project_id>/update", update_project, name="update-project")


CHECK_PROJECT_EXISTENCE = path("/exists", check_project_existence, name="check-project-existence")


# This path is used to get the list of schemas inside a project
GET_SCHEMAS = path(
    "/<str:project_id>/schemas",
    get_project_schemas,
    name="get-project-schemas",
)

SET_PROJECT_SCHEMA = path(
    "/<str:project_id>/set_schema",
    set_project_schema,
    name="set-project-schema",
)

# This path is used to get the list of schemas & it's table inside a project
GET_SCHEMAS_AND_TABLES = path(
    "/<str:project_id>/schemas/tables",
    get_project_schemas_and_tables,
    name="get-project-schemas-and-tables",
)

GET_TABLE_SCHEMA = path("/<str:project_id>/table_schema", get_table_schema, name="get-table-schema")

# This path is used to get the list of tables inside a project,
# This will fetch the database and schema details from profile
GET_TABLES = path(
    "/<str:project_id>/schema/<str:schema_name>/tables",
    get_project_tables,
    name="get-project-tables",
)


# This API will return the list of columns inside a table.
GET_TABLE_COLUMNS = path(
    "/<str:project_id>/schema/<str:schema_name>/table/<str:table_name>/columns",
    get_project_table_columns,
    name="get-project-table-columns",
)


# This API will return the list of columns inside a table.
GET_TABLE_CONTENT = path(
    "/<str:project_id>/schema/<str:schema_name>/table/<str:table_name>/content",
    get_project_table_content,
    name="get-project-table-content",
)

# ---------------------------------------------------------------------------------------
# The below APIs are used to read/write the project files.
# ---------------------------------------------------------------------------------------
RELOAD_MODEL = path("/<str:project_id>/reload", reload_model, name="reload-model")


# This API will fetch the content of the file from the given path.
WRITE_DATABASE_FILE = path(
    "/<str:project_id>/database/upload",
    write_database_file,
    name="write-database-file",
)

# This API will fetch the content of the file from the given path.
VALIDATE_MODEL_FILE = path(
    "/<str:project_id>/no_code_model/<str:file_name>/validate",
    validate_model_file,
    name="validate-no-code-model-file",
)

# This API is used to store the YAML data
# This method is depreciated
SAVE_MODEL_FILE = path(
    "/<str:project_id>/no_code_model/<str:file_name>",
    save_model_file,
    name="save-no-code-model-file",
)

# This API will capture the source and model data from the configuration window
SET_MODEL_CONFIG = path(
    "/<str:project_id>/no_code_model/<str:file_name>/set-model",
    set_model_config_and_reference,
    name="set-no-code-model-config",
)


# This API will capture the source and model data from the configuration window
SET_MODEL_TRANSFORMATION = path(
    "/<str:project_id>/no_code_model/<str:file_name>/set-transform",
    set_model_transformation,
    name="set-no-code-model-transformation",
)

# This API will capture the source and model data from the configuration window
DELETE_MODEL_TRANSFORMATION = path(
    "/<str:project_id>/no_code_model/<str:file_name>/delete-transform",
    delete_model_transformation,
    name="delete-no-code-model-transformation",
)


# This API will capture the source and model data from the configuration window
SET_MODEL_PRESENTATION = path(
    "/<str:project_id>/no_code_model/<str:file_name>/set-presentation",
    set_model_presentation,
    name="set-no-code-model-presentation",
)


# This API will return the available columns in the specific transformation
GET_TRANSFORMATION_COLUMNS = path(
    "/<str:project_id>/no_code_model/<str:file_name>/columns",
    get_transformation_columns,
    name="get-transformation-columns",
)


# This API will fetch the content of the file from the given path.
FETCH_MODEL_TABLE_CONTENT = path(
    "/<str:project_id>/no_code_model/<str:model_name>/content",
    get_model_file_content,
    name="get-no-code-model-file",
)


# This API will fetch the content of the file from the given
# path with the previous successful content.
ROLLBACK_MODEL_TABLE_CONTENT = path(
    "/<str:project_id>/no_code_model/<str:model_name>/rollback",
    rollback_model_file_content,
    name="rollback-no-code-model-file",
)

EXPORT_MODEL_CONTENT_CSV = path(
    "/<str:project_id>/no_code_model/<str:model_name>/export_csv",
    export_model_content_csv,
    name="export-no-code-model-file-csv",
)

GET_SUPPORTED_REFERENCE_MODELS = path(
    "/<str:project_id>/no_code_model/<str:file_name>/supported_references",
    get_supported_models,
    name="get-supported-reference-models",
)

GET_LINEAGE = path("/<str:project_id>/lineage", get_lineage, name="get-lineage")

GET_LINEAGE_INFO = path("/<str:project_id>/lineage/<str:model_name>/info", get_lineage_info, name="get-lineage-info")

# SQL Flow - Table-level lineage with ER diagram style visualization
GET_SQL_FLOW = path("/<str:project_id>/sql-flow", get_sql_flow, name="get-sql-flow")

# This API will generate formula from given prompt
GENERATE_FORMULA = path(
    "/<str:project_id>/no_code_model/<str:model_name>/generate_formula",
    generate_formula,
    name="generate-formula",
)

DELETE_A_PROJECT = path("/<str:project_id>/delete", delete_project, name="delete_project")

urlpatterns = [
    # APIs used for a project
    CREATE_NEW_PROJECT,
    LIST_ALL_PROJECTS,
    CREATE_SAMPLE_PROJECT,
    GET_PROJECT_DETAIL,
    UPDATE_A_PROJECT,
    DELETE_A_PROJECT,
    CHECK_PROJECT_EXISTENCE,
    GET_SCHEMAS,  # Done
    SET_PROJECT_SCHEMA,
    GET_TABLES,
    GET_SCHEMAS_AND_TABLES,
    GET_TABLE_COLUMNS,
    GET_TABLE_CONTENT,
    RELOAD_MODEL,
    WRITE_DATABASE_FILE,
    VALIDATE_MODEL_FILE,
    SAVE_MODEL_FILE,
    SET_MODEL_CONFIG,
    SET_MODEL_TRANSFORMATION,
    DELETE_MODEL_TRANSFORMATION,
    SET_MODEL_PRESENTATION,
    GET_TRANSFORMATION_COLUMNS,
    FETCH_MODEL_TABLE_CONTENT,
    EXPORT_MODEL_CONTENT_CSV,
    GET_SUPPORTED_REFERENCE_MODELS,
    ROLLBACK_MODEL_TABLE_CONTENT,
    GET_LINEAGE,
    GET_LINEAGE_INFO,
    GET_SQL_FLOW,
    GET_TABLE_SCHEMA,
    GENERATE_FORMULA,
]


# Job Scheduler (core OSS functionality)
JOB_SCHEDULER = path("/<str:project_id>/jobs", include("backend.core.scheduler.urls"))
urlpatterns.append(JOB_SCHEDULER)

try:
    PROJECT_SHARING = path("/<str:project_id>", include("pluggable_apps.project_sharing.urls"))
    urlpatterns.append(PROJECT_SHARING)
except (ModuleNotFoundError, RuntimeError):
    print("Project Sharing Module does not exist or is not configured.")

# Onboarding URLs
ONBOARDING_URLS = path("/<str:project_id>/onboarding/", include("backend.core.routers.onboarding.urls"))
urlpatterns.append(ONBOARDING_URLS)
