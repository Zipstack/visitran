from django.urls import path

from backend.core.routers.project_connection.views import get_connection, test_connection, update_connection

# This API will fetch the connections details of the project
GET_CONNECTION = path(
    "",
    get_connection,
    name="get-project_connection",
)

# This API will take the payload and validates
# the project_connection and updates it in profile yaml.
UPDATE_CONNECTION = path(
    "/<str:datasource>/update",
    update_connection,
    name="update-project_connection",
)


# This API will make a test project_connection to DB
TEST_CONNECTION = path(
    "/<str:datasource>/test",
    test_connection,
    name="test-project_connection",
)


urlpatterns = [GET_CONNECTION, UPDATE_CONNECTION, TEST_CONNECTION]
