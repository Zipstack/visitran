from django.urls import path

from backend.core.routers.explorer.views import (
    create_folder_explorer,
    create_model_explorer,
    delete_a_file_or_folder,
    get_project_file_content,
    get_project_file_explorer,
    rename_a_file_or_folder,
    upload_a_file,
    get_database_table_explorer,
)


# This API will return the files and sub-folders from the project.
GET_LIST = path(
    "",
    get_project_file_explorer,
    name="get-project-file-explorer",
)


CREATE_A_FOLDER = path(
    "/folder/create",
    create_folder_explorer,
    name="create-folder-explorer",
)

CREATE_A_MODEL = path(
    "/model/create",
    create_model_explorer,
    name="create-folder-explorer",
)


DELETE_A_FILE = path(
    "/file/delete",
    delete_a_file_or_folder,
    name="delete-file-explorer",
)


RENAME_A_FILE = path(
    "/file/rename",
    rename_a_file_or_folder,
    name="rename-file-explorer",
)


# This API will fetch the content of the file from the given path.
GET_FILE_CONTENT = path(
    "/get",
    get_project_file_content,
    name="get-file-content",
)

GET_TABLE_LIST = path(
    "/database",
    get_database_table_explorer,
    name="get-database-table-explorer",
)

# This API will fetch the content of the file from the given path.
UPLOAD_A_FILE = path("/upload", upload_a_file, name="get-file-content")


urlpatterns = [GET_LIST, CREATE_A_FOLDER, CREATE_A_MODEL, DELETE_A_FILE, RENAME_A_FILE, GET_FILE_CONTENT, UPLOAD_A_FILE, GET_TABLE_LIST]
