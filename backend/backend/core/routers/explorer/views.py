import logging

from django.http import FileResponse
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from backend.application.context.application import ApplicationContext
from backend.core.utils import handle_http_request
from backend.utils.cache_service.decorators.cache_decorator import clear_cache
from backend.utils.constants import HTTPMethods
from visitran.events.functions import fire_event
from visitran.events.types import ModelCreated, FileDeleted, FileRenamed


@api_view([HTTPMethods.GET])
@handle_http_request
def get_database_table_explorer(request: Request, project_id: str) -> Response:
    query = request.GET.get("reload", False)
    reload_db = False
    if query:
        reload_db = True
    logging.info(f"Project ID: {project_id}")
    app = ApplicationContext(project_id=project_id)
    response_data = app.get_database_explorer(reload_db=reload_db)
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_project_file_explorer(request: Request, project_id: str) -> Response:
    app = ApplicationContext(project_id=project_id)
    response_data = app.get_project_explorer()
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def create_folder_explorer(request: Request, project_id: str) -> Response:
    request_data = request.data
    folder_name = request_data.get("folder_name")
    parent_path = request_data.get("parent_path")
    app = ApplicationContext(project_id=project_id)
    # app.create_a_folder(folder_name=folder_name, parent_path=parent_path)
    return Response(data={"status": "success"}, status=status.HTTP_200_OK)


@api_view([HTTPMethods.POST])
@handle_http_request
def create_model_explorer(request: Request, project_id: str) -> Response:
    try:
        request_data = request.data
        model_name = request_data.get("model_name", "").replace(" ", "_").strip()
        app = ApplicationContext(project_id=project_id)
        app.create_a_model(model_name=model_name, is_generate_ai_request=False)
        fire_event(ModelCreated(model_name=model_name))
        return Response(data={"status": "success"}, status=status.HTTP_200_OK)
    except FileExistsError:
        return Response(
            data={
                "status": "Conflict",
                "Error": "Model Already exists with the same name, Try different name",
            },
            status=status.HTTP_409_CONFLICT,
        )


@api_view([HTTPMethods.DELETE])
@clear_cache(patterns=["model_content_{project_id}_*"])
@handle_http_request
def delete_a_file_or_folder(request: Request, project_id: str) -> Response:
    # By default, the files will be uploaded in seeds path as of now
    # request.data will contain list of files to delete to support multiple delete at once.
    request_data = request.data
    file_list: list = request_data.get("file_name", [])
    table_delete_enabled: bool = request.data.get("delete_table", False)
    wipe_all_enabled = request.data.get("delete_all_models", False)
    app = ApplicationContext(project_id=project_id)
    if wipe_all_enabled:
        app.cleanup_no_code_model(table_delete_enabled=table_delete_enabled)
        fire_event(FileDeleted(file_names="all models"))
        response_json = {"status": "success", "message": f"successfully deleted all model files"}
    else:
        # Build set of model names being deleted in this batch so that
        # dependency checks can ignore models that are also being removed.
        deleting_models = set()
        for f in file_list:
            if f.startswith("models"):
                name = f.split("models/no_code/")[-1].replace(" ", "_").strip()
                deleting_models.add(name)

        deleted_files = []
        for file_name in file_list:
            app.delete_a_file_or_folder(
                file_path=file_name,
                table_delete_enabled=table_delete_enabled,
                deleting_models=deleting_models,
            )
            deleted_files.append(file_name)

        fire_event(FileDeleted(file_names=", ".join(deleted_files)))
        response_json = {"status": "success", "message": f"successfully deleted files {deleted_files}"}
    return Response(data=response_json)


@api_view([HTTPMethods.POST])
@clear_cache(patterns=["model_content_{project_id}_*"])
@handle_http_request
def rename_a_file_or_folder(request: Request, project_id: str) -> Response:
    # By default, the files will be uploaded in seeds path as of now
    request_data = request.data
    file_name: str = request_data["file_name"]
    rename: str = request_data["rename"]
    app = ApplicationContext(project_id=project_id)
    refactored_models = app.rename_a_file_or_folder(file_path=file_name, rename=rename)
    fire_event(FileRenamed(old_name=file_name, new_name=rename))
    response_json = {"status": "success", "refactored_models": refactored_models}
    return Response(data=response_json)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_project_file_content(request: Request, project_id: str) -> FileResponse:
    file_path = request.GET.get("file_path")
    app = ApplicationContext(project_id=project_id)
    file = app.get_file_content(file_path)
    response = FileResponse(file, as_attachment=True)
    return response


@api_view([HTTPMethods.POST])
@handle_http_request
def upload_a_file(request: Request, project_id: str) -> Response:
    # By default, the files will be uploaded in seeds path as of now
    file_content = request.FILES["file"]
    request_data = request.data
    file_name: str = request_data["file_name"]
    app = ApplicationContext(project_id=project_id)
    app.upload_a_file(file_name=file_name, file_content=file_content)

    # Auto-commit seed to Git repo if version control is configured
    if file_name.endswith(".csv"):
        try:
            from pluggable_apps.version_control.services.project_integration import auto_commit_seed
            auto_commit_seed(app.project_instance, file_name, file_content.read().decode("utf-8", errors="replace"))
        except ImportError:
            pass
        except Exception:
            pass  # Non-blocking

    response_json = {"status": "success"}
    return Response(data=response_json)
