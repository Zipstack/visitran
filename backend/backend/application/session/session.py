import datetime
import io
import logging
from typing import Any, List

from django.conf import settings
from django.core.files.base import ContentFile
from django.db.utils import IntegrityError

from backend.application.session.base_session import BaseSession
from backend.application.session.env_session import EnvironmentSession
from backend.core.constants.reserved_names import ProjectNameConstants
from backend.core.models.backup_models import BackupModels
from backend.core.models.config_models import ConfigModels
from backend.core.models.csv_models import CSVModels
from backend.core.models.dependent_models import DependentModels
from backend.errors import (
    CSVFileAlreadyExists,
    BackupNotExistException,
    ModelAlreadyExists,
    ProjectDependencyException,
    ModelNotExists,
)
from backend.errors.exceptions import CSVFileNotUploaded, ProjectNameReservedError
from backend.utils.decryption_utils import decrypt_sensitive_fields

# Import scheduler models (now core OSS)
from backend.core.scheduler.models import UserTaskDetails


class Session(BaseSession):

    def update_project_details(self, project_details: dict[str, Any]):
        if name := project_details.get("project_name"):
            # Check for reserved names
            if ProjectNameConstants.is_reserved_name(name):
                raise ProjectNameReservedError(project_name=name)
            self.project_instance.project_name = name

        if description := project_details.get("description"):
            self.project_instance.project_description = description

        environment_id = project_details.get("environment", {}).get("id")
        connection_id = project_details.get("connection", {}).get("id")

        if connection_id:
            self.project_instance.connection_model_id = connection_id

        if environment_id:
            env_session = EnvironmentSession()
            env_model = env_session.get_environment_model(environment_id=environment_id)
            self.project_instance.environment_model = env_model

        self.project_instance.save()

        # CACHE: invalidate connection-dependent data (schemas/tables)
        self._invalidate_connection_dependent_keys()

    def update_project_connection(self, connection_details: dict[str, Any]) -> dict[str, Any]:
        # TODO - Need to remove the project_connection update from project level

        # Decrypt sensitive fields from frontend encrypted data
        decrypted_connection_details = decrypt_sensitive_fields(connection_details)

        connection_model = self.project_instance.connection_model
        connection_model.connection_details = decrypted_connection_details
        connection_model.save()

        # CACHE: connection changed -> invalidate warehouse browsing caches
        self._invalidate_connection_dependent_keys()
        return decrypted_connection_details

    def delete_project(self):
        """
        This method will delete the current project instance
        :return:
        """
        if UserTaskDetails is not None:
            active_jobs = UserTaskDetails.objects.filter(project_id=self.project_id)
            active_jobs_list = [job.task_name for job in active_jobs]
            if active_jobs:
                raise ProjectDependencyException(
                    project_name=self.project_instance.project_name,
                    jobs=active_jobs_list
                )
        # CACHE: delete known keys for this project
        self._invalidate_models_cache()
        self._invalidate_csv_cache()
        self._invalidate_connection_dependent_keys()

        self.project_instance.delete()

    def check_model_exists(self, model_name: str) :
        """
        Checks if the model exists and returns it if found, else None.
        """
        return self.fetch_model_if_exists(model_name=model_name)

    def create_model(self, model_name: str, is_generate_ai_request: bool) -> str:
        """
        Creates a model:
        - Raises exception if the model exists and this is not an AI generation request.
        - For AI-generated requests, modifies name if already exists.
        """
        existing_model = self.check_model_exists(model_name=model_name)

        if existing_model:
            if not is_generate_ai_request:
                raise ModelAlreadyExists(model_name=model_name, created_at=existing_model.created_at)
            else:
                # Keep modifying the name until we find a unique one
                suffix = "_ai_generated"
                counter = 1
                new_model_name = model_name + suffix
                while self.check_model_exists(new_model_name):
                    new_model_name = f"{model_name}{suffix}_{counter}"
                    counter += 1
                model_name = new_model_name

        self.project_instance.config_model.create(model_name=model_name)

        # CACHE: invalidate lists and existence map; set single model snapshot
        self._invalidate_models_cache()
        self._invalidate_model_key(model_name)
        return model_name

    def fetch_model_data(self, model_name: str) -> dict:
        return self.fetch_model(model_name).model_data

    def fetch_all_models_name(self) -> list[str]:
        children = []
        models: List[ConfigModels] = self.fetch_all_models(fetch_all=True)
        for model in models:
            children.append(model.model_name)
        # CACHE: store names list
        self._cache_set(self._cache_key("models", "names"), children)
        return children

    def convert_dates(self, obj):
        if isinstance(obj, dict):
            return {k: self.convert_dates(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_dates(v) for v in obj]
        elif isinstance(obj, datetime.date):
            return obj.isoformat()  # Convert to 'YYYY-MM-DD'
        return obj

    def update_model(self, model_data: dict[str, Any], model_name: str) -> None:
        config_model: ConfigModels = self.fetch_model(model_name=model_name)
        config_model.model_data = self.convert_dates(model_data)
        config_model.save()
        # CACHE: refresh single model snapshot; invalidate model lists
        self._invalidate_models_cache()
        self._invalidate_model_key(model_name)

        # Auto-commit to version control (if configured)
        try:
            from pluggable_apps.version_control.services.project_integration import auto_commit_model
            import json, yaml
            clean_data = json.loads(json.dumps(model_data, default=str))
            yaml_content = yaml.dump(clean_data, default_flow_style=False, sort_keys=False)
            auto_commit_model(config_model.project_instance, model_name, yaml_content)
        except ImportError:
            pass
        except Exception:
            pass

    def update_model_content(self, model_name: str, model_content: str) -> None:
        try:
            # Validate inputs
            if not model_name:
                raise ValueError("Model name cannot be empty")
            if not model_content:
                raise ValueError("Model content cannot be empty")

            # Fetch model
            model = self.fetch_model(model_name=model_name)

            # Create content file
            try:
                # Create file content
                binary_stream = io.BytesIO(model_content.encode("utf-8"))
                content_file = ContentFile(binary_stream.getvalue(), name=model_name)

                # Update model - ConfigModels.save() will handle file management
                model.model_py_content = content_file
                model.save()
                print("Model saved successfully")

                # CACHE: invalidate lists and single
                self._invalidate_models_cache()
                self._invalidate_model_key(model_name)

            except Exception as e:
                print(f"Error creating/saving file: {e}")
                raise

        except Exception as e:
            print(f"Error in update_model_content: {e}")
            raise

    def get_model_dependency_data(
        self, model_name: str, transformation_id: str, default: Any = None
    ) -> dict[str, Any] | Any:
        try:
            model = self.fetch_model(model_name=model_name)
            dependent_model: DependentModels = self.project_instance.dependent_model.get(
                model=model,
                transformation_id=transformation_id
            )
            return dependent_model.model_data
        except DependentModels.DoesNotExist as model_not_exist:  # Raising exception if default is none
            if default is not None:
                return default
            raise model_not_exist

    def update_model_dependency(self, model_name: str, transformation_id: str, model_data: dict[str, Any]):
        model = self.fetch_model(model_name=model_name)
        try:
            dependent_model = self.project_instance.dependent_model.get(model=model, transformation_id=transformation_id)
            dependent_model.model_data = model_data
            dependent_model.save()
        except DependentModels.DoesNotExist:
            self.project_instance.dependent_model.create(model=model, transformation_id=transformation_id, model_data=model_data)
        # CACHE: dependency updated — invalidate model cache
        self._invalidate_model_key(model_name)

    def rename_a_model(self, model_name: str, rename: str):
        model_exists = self.check_model_exists(model_name=rename)
        if model_exists:
            raise ModelAlreadyExists(model_name=rename, created_at=model_exists.created_at)
        config_model = self.fetch_model(model_name=model_name)
        config_model.model_name = rename
        config_model.save()
        # CACHE: invalidate old and new keys, and lists
        self._invalidate_model_key(model_name)
        self._invalidate_model_key(rename)
        self._invalidate_models_cache()

    def backup_all_model(self):
        models = self.fetch_all_models()
        for model in models:
            try:
                backup_model = self.project_instance.backup_model.get(config_model=model)
                backup_model.model_data = model.model_data
                backup_model.save()
            except BackupModels.DoesNotExist:
                self.project_instance.backup_model.create(config_model=model, model_data=model.model_data)
        # CACHE: no-op for backups

    def rollback_model(self, model_name: str) -> dict[str, Any]:
        # noinspection PyUnresolvedReferences
        model = self.fetch_model(model_name=model_name)
        backup_model: BackupModels = self.project_instance.backup_model.get(config_model=model)
        if not backup_model:
            raise BackupNotExistException(model_name=model_name)
        # CACHE: after rollback, model likely updated
        self._invalidate_model_key(model_name)
        self._invalidate_models_cache()
        return backup_model.model_data

    def is_rollback_exist(self, model_name: str) -> bool:
        # noinspection PyUnresolvedReferences
        try:
            model = self.fetch_model(model_name=model_name)
            backup_model: BackupModels = self.project_instance.backup_model.get(config_model=model)
            return bool(backup_model)
        except (BackupModels.DoesNotExist, ModelNotExists):
            return False

    def delete_model(self, model_name: str):
        models: ConfigModels = self.fetch_model(model_name=model_name)
        models.delete()
        # CACHE: invalidate lists and single
        self._invalidate_model_key(model_name)
        self._invalidate_models_cache()

    def fetch_csv_file_fields(self, csv_name: str):
        csv_model: CSVModels = self.fetch_csv_model(csv_name=csv_name)
        return csv_model.csv_field

    def upload_csv_file(self, file_name: str, file_content):
        is_exist: CSVModels = self.fetch_csv_model_if_exists(csv_name=file_name)
        if is_exist:
            raise CSVFileAlreadyExists(csv_name=file_name, created_at=str(is_exist.updated_at))
        try:
            # noinspection PyUnresolvedReferences
            self.project_instance.csv_model.create(csv_name=file_name, csv_field=file_content)
            # CACHE: update
            self._invalidate_csv_cache(file_name)
        except IntegrityError:
            raise CSVFileAlreadyExists(csv_name=file_name, created_at="just now or with the same name")
        except Exception as e:
            logging.error(f"Error in upload_csv_file: {e}")
            logging.error(f" Error class: {e.__class__}")
            raise CSVFileNotUploaded(csv_name=file_name, reason=str(e))

    def rename_a_csv_file(self, csv_name: str, rename: str):
        is_exists = self.fetch_csv_model_if_exists(csv_name=rename)
        if is_exists:
            raise CSVFileAlreadyExists(csv_name=rename, created_at=str(is_exists.created_at))
        csv_model = self.fetch_csv_model(csv_name=csv_name)
        csv_model.rename_csv_file(rename)
        # CACHE: invalidate both names and list
        self._invalidate_csv_cache(csv_name)
        self._invalidate_csv_cache(rename)

    def delete_csv_model(self, file_name: str):
        model: CSVModels = self.fetch_csv_model(csv_name=file_name)
        model.delete()
        # CACHE: invalidate csv caches
        self._invalidate_csv_cache(file_name)
