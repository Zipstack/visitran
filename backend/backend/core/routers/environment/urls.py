from django.urls import path

from backend.core.routers.environment.views import (
    create_environment,
    delete_environment,
    environment_dependent_projects,
    get_all_environments,
    get_environment,
    reveal_environment_credentials,
    test_environment,
    update_environment,
)

# This API will fetch the connections details of the project
GET_ALL_ENVIRONMENTS = path(
    "s",
    get_all_environments,
    name="get-environment",
)

# This API will test the environment
TEST_ENVIRONMENT = path(
    "/test",
    test_environment,
    name="get-environment",
)

# This API will make a test project_connection to DB
CREATE_ENVIRONMENT = path(
    "s/create",
    create_environment,
    name="test-environment",
)


# This API will fetch the connections details of the project
GET_ENVIRONMENT = path(
    "/<str:environment_id>",
    get_environment,
    name="get-environment",
)

# This API will take the payload and validates
# the project_connection and updates it in profile yaml.
UPDATE_ENVIRONMENT = path(
    "/<str:environment_id>/update",
    update_environment,
    name="update-environment",
)


DELETE_ENVIRONMENT = path(
    "/<str:environment_id>/delete",
    delete_environment,
    name="delete-environment",
)

REVEAL_ENVIRONMENT_CREDENTIALS = path(
    "/<str:environment_id>/reveal",
    reveal_environment_credentials,
    name="reveal-environment-credentials",
)

GET_ENVIRONMENT_DEPENDENT_PROJECTS = path(
    "/<str:environment_id>/usage", environment_dependent_projects, name="environment-dependent-projects"
)

urlpatterns = [
    TEST_ENVIRONMENT,
    GET_ALL_ENVIRONMENTS,
    GET_ENVIRONMENT,
    CREATE_ENVIRONMENT,
    UPDATE_ENVIRONMENT,
    DELETE_ENVIRONMENT,
    REVEAL_ENVIRONMENT_CREDENTIALS,
    GET_ENVIRONMENT_DEPENDENT_PROJECTS,
]
