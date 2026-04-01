import logging
from typing import Any

from backend.application.session.env_session import EnvironmentSession
from backend.utils.decryption_utils import decrypt_sensitive_fields


class EnvironmentContext:
    def __init__(self):
        self.env_session = EnvironmentSession()

    def get_all_environments(self, page: int, limit: int) -> list[dict[str, Any]]:
        filter_condition = {}
        environments: list[dict[str, Any]] = self.env_session.get_all_environments(
            page=page, limit=limit, filter_condition=filter_condition
        )
        return environments

    def get_environment(self, environment_id: str) -> dict[str, Any]:
        env_data = self.env_session.get_environment(environment_id)
        return env_data

    def reveal_environment_credentials(self, environment_id: str) -> dict[str, Any]:
        """Return decrypted (unmasked) environment connection details."""
        env_model = self.env_session.get_environment_model(environment_id)
        return env_model.decrypted_connection_data

    def get_environment_dependent_projects(self, environment_id):
        return self.env_session.projects_by_environment(environment_id=environment_id)

    def create_environment(self, environment_details: dict) -> dict[str, Any]:
        # Decrypt sensitive fields from frontend encrypted data
        try:
            decrypted_environment_details = decrypt_sensitive_fields(environment_details)
        except Exception as e:
            logging.exception("Failed to decrypt environment creation data")
            # Continue with original data if decryption fails
            decrypted_environment_details = environment_details
        
        env_model = self.env_session.create_environment(environment_details=decrypted_environment_details)
        response_data = {
            "id": env_model.environment_id,
            "name": env_model.environment_name,
            "description": env_model.environment_description,
            "deployment_type": env_model.deployment_type,
            "env_connection_data": env_model.masked_connection_data,
            "datasource_name": env_model.connection_model.datasource_name,
            "created_by": env_model.created_by,
            "last_modified_by": env_model.last_modified_by,
        }
        return response_data

    def update_environment(self, environment_id: str, environment_details: dict[str, Any]):
        # Decrypt sensitive fields from frontend encrypted data
        try:
            decrypted_environment_details = decrypt_sensitive_fields(environment_details)
        except Exception as e:
            logging.exception("Failed to decrypt environment update data")
            # Continue with original data if decryption fails
            decrypted_environment_details = environment_details
        
        env_model = self.env_session.update_environment(
            environment_id=environment_id, environment_details=decrypted_environment_details
        )
        response_data = {
            "id": env_model.environment_id,
            "name": env_model.environment_name,
            "description": env_model.environment_description,
            "deployment_type": env_model.deployment_type,
            "env_connection_data": env_model.masked_connection_data,
            "datasource_name": env_model.connection_model.datasource_name,
            "created_by": env_model.created_by,
            "last_modified_by": env_model.last_modified_by,
        }
        return response_data

    def delete_environment(self, environment_id: str):
        self.env_session.delete_environment(environment_id=environment_id)
