from django.urls import path

from backend.core.routers.connection.views import (
    connection_dependent_environments,
    connection_dependent_projects,
    connection_usage,
    create_connection,
    delete_all_connections,
    delete_connection,
    get_all_connection,
    get_connection,
    reveal_connection_credentials,
    test_connection,
    test_connection_by_id,
    update_connection,
)

# This API will fetch the connection's details of the project
GET_ALL_CONNECTION = path(
    "s",
    get_all_connection,
    name="get-all-connection",
)

# This API will fetch the connection's details of the project
CREATE_CONNECTION = path(
    "s/create",
    create_connection,
    name="create-connection",
)

# This API will fetch the connection's details of the project
GET_CONNECTION = path(
    "/<str:connection_id>",
    get_connection,
    name="get-connection",
)


# This API will take the payload and validates
# the project_connection and updates it in profile YAML.
UPDATE_CONNECTION = path(
    "/<str:connection_id>/update",
    update_connection,
    name="update-connection",
)

# This API will make a test project_connection to DB
TEST_CONNECTION_BY_ID = path(
    "/<str:connection_id>/test",
    test_connection_by_id,
    name="test-connection",
)

GET_CONNECTION_DEPENDENT_PROJECTS = path(
    "/<str:connection_id>/dependency/projects", connection_dependent_projects, name="connection-dependent-projects"
)

GET_CONNECTION_DEPENDENT_ENVIRONMENTS = path(
    "/<str:connection_id>/dependency/environments",
    connection_dependent_environments,
    name="connection-dependent-environments",
)

GET_CONNECTION_USAGE = path("/<str:connection_id>/usage", connection_usage, name="connection-usage")


REVEAL_CONNECTION_CREDENTIALS = path(
    "/<str:connection_id>/reveal",
    reveal_connection_credentials,
    name="reveal-connection-credentials",
)

DELETE_CONNECTION = path("/<str:connection_id>/delete", delete_connection, name="delete-connection")


TEST_CONNECTION = path("/test", test_connection, name="test-connection")

DELETE_ALL_CONNECTIONS = path(
    "s/delete-all",
    delete_all_connections,
    name="delete-all-connections",
)

urlpatterns = [
    TEST_CONNECTION,
    DELETE_ALL_CONNECTIONS,
    GET_CONNECTION,
    CREATE_CONNECTION,
    GET_ALL_CONNECTION,
    UPDATE_CONNECTION,
    TEST_CONNECTION_BY_ID,
    REVEAL_CONNECTION_CREDENTIALS,
    GET_CONNECTION_DEPENDENT_PROJECTS,
    GET_CONNECTION_DEPENDENT_ENVIRONMENTS,
    GET_CONNECTION_USAGE,
    DELETE_CONNECTION,
]
