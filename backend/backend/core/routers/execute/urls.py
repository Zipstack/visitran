from django.urls import path

from backend.core.routers.execute.views import execute_run_command, execute_seed_command, execute_sql_command

# ---------------------------------------------------------------------------------------
# The below APIs are used to run commands on visitran projects.
# ---------------------------------------------------------------------------------------

# This API will execute seed command in visitran.
EXECUTE_SEED_COMMAND = path(
    "/seed",
    execute_seed_command,
    name="execute-seed-command",
)


# This API will execute run command in visitran.
EXECUTE_RUN_COMMAND = path(
    "/run",
    execute_run_command,
    name="execute-run-command",
)


EXECUTE_SQL_COMMAND = path(
    "/sql",
    execute_sql_command,
    name="execute-sql-command",
)


urlpatterns = [
    # APIs used for executing commands in visitran
    EXECUTE_SEED_COMMAND,
    EXECUTE_RUN_COMMAND,
    EXECUTE_SQL_COMMAND,
]
