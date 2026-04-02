import logging
from typing import Any

from backend.application.session.connection_session import ConnectionSession
from backend.application.utils import test_connection_data, create_schema_if_not_exist, get_connection_data
from backend.core.models.connection_models import ConnectionDetails
from backend.utils.decryption_utils import (
    decrypt_connection_details_robust
)
from backend.utils.encryption import SENSITIVE_FIELDS
from visitran.errors import ConnectionFailedError

MASKED_SENTINEL = "********"


def _is_masked(value: str) -> bool:
    """Return True if value looks like a masked sentinel."""
    if not isinstance(value, str):
        return False
    # Exact sentinel match for regular fields
    if value == MASKED_SENTINEL:
        return True
    # connection_url uses partial masking — detect consecutive *'s
    if "****" in value:
        return True
    return False


# Fields that are derived/rebuilt by the adapter — skip merge errors for these.
# e.g. connection_url is rebuilt from host/port/user/passw in host-based connections.
_DERIVED_FIELDS = {"connection_url"}


def _merge_with_stored(incoming: dict[str, Any], stored: dict[str, Any]) -> dict[str, Any]:
    """Replace masked sentinel values in *incoming* with real values from
    *stored*.

    Only sensitive fields are checked. Non-sensitive fields are always
    taken from *incoming* (the user may have changed them).

    Raises ConnectionFailedError if the stored value is also masked
    (corrupted by a previous bug) — except for derived fields like
    connection_url which are rebuilt by the adapter from other fields.
    """
    merged = incoming.copy()
    for key in incoming:
        if key.lower() in SENSITIVE_FIELDS and _is_masked(incoming[key]):
            stored_value = stored.get(key)
            if stored_value and not _is_masked(stored_value):
                logging.debug(f"Merging stored value for masked field '{key}'")
                merged[key] = stored_value
            elif key.lower() in _DERIVED_FIELDS:
                # Derived fields will be rebuilt by the adapter — skip silently
                logging.debug(f"Skipping corrupted derived field '{key}', adapter will rebuild it")
            else:
                raise ConnectionFailedError(
                    db_type="connection",
                    error_message=(
                        f"Stored credentials for '{key}' are invalid. "
                        f"Please re-enter your credentials by clearing the field and typing the new value."
                    ),
                )
    return merged


def _has_credentials_changed(incoming: dict[str, Any], stored: dict[str, Any]) -> bool:
    """Return True if any connection credential field differs from stored values."""
    if not stored:
        return True
    for key, value in incoming.items():
        if stored.get(key) != value:
            return True
    return False


class ConnectionContext:

    def __init__(self):
        self._con_session = ConnectionSession()

    @property
    def connection_session(self):
        return self._con_session

    def get_all_connections(self, page: int, limit: int, filter_condition: dict):
        connections = self.connection_session.get_all_connections(
            page=page, limit=limit, filter_condition=filter_condition
        )
        return connections

    def get_connection(self, connection_id: str):
        connection = self.connection_session.get_connection(connection_id=connection_id)
        # Always mask sensitive data — use reveal endpoint for unmasked values
        if connection and "connection_details" in connection:
            model = self.connection_session.get_connection_model(connection_id)
            if model:
                connection["connection_details"] = model.masked_connection_details
        return connection

    def reveal_connection_credentials(self, connection_id: str) -> dict:
        """Return decrypted (unmasked) connection details.

        Called by the dedicated reveal endpoint so the frontend can
        populate the form when the user clicks the unmask/eye icon.
        Sample-project connections are never revealed.
        """
        model = self.connection_session.get_connection_model(connection_id)
        if not model:
            raise Exception(f"Connection {connection_id} not found")
        if model.project.filter(is_sample=True).exists():
            return model.masked_connection_details
        return model.decrypted_connection_details

    def create_connection(self, connection_details: dict, force_create: bool = False) -> dict[str, Any]:
        datasource: str = connection_details["datasource_name"]
        connection_data: dict[str, Any] = connection_details["connection_details"]

        # Decrypt sensitive fields from frontend encrypted data using robust method
        decrypted_connection_data = decrypt_connection_details_robust(connection_data)

        # Test connection with decrypted data
        test_connection_data(datasource=datasource, connection_data=decrypted_connection_data)
        _conn_details = get_connection_data(datasource=datasource, connection_data=decrypted_connection_data)
        connection_details["connection_details"] = _conn_details

        connection_model: ConnectionDetails = self.connection_session.create_connection(
            connection_details=connection_details
        )

        response_data = {
            "id": connection_model.connection_id,
            "name": connection_model.connection_name,
            "description": connection_model.connection_description,
            "datasource_name": connection_model.datasource_name,
            "connection_details": connection_model.masked_connection_details,
            "created_by": connection_model.created_by,
            "last_modified_by": connection_model.last_modified_by,
        }
        return response_data

    @staticmethod
    def create_schema(connection_details: dict) -> None:
        datasource: str = connection_details["datasource_name"]
        connection_data: dict[str, Any] = connection_details["connection_details"]
        create_schema_if_not_exist(datasource=datasource, connection_data=connection_data)

    def update_connection(self, connection_id: str, connection_details: dict) -> dict[str, Any]:
        datasource: str = connection_details["datasource_name"]
        connection_data: dict[str, Any] = connection_details["connection_details"]

        # Decrypt sensitive fields from frontend encrypted data using robust method
        decrypted_connection_data = decrypt_connection_details_robust(connection_data)

        # Merge masked sentinels with stored real values
        stored_model = self.connection_session.get_connection_model(connection_id)
        stored_details = {}
        if stored_model:
            stored_details = stored_model.decrypted_connection_details
            decrypted_connection_data = _merge_with_stored(decrypted_connection_data, stored_details)

        # Skip connection test only for metadata-only updates where credentials are unchanged
        metadata_only = connection_details.pop("metadata_only", False)
        credentials_changed = _has_credentials_changed(decrypted_connection_data, stored_details)
        if not metadata_only or credentials_changed:
            test_connection_data(datasource=datasource, connection_data=decrypted_connection_data)
        _conn_details = get_connection_data(datasource=datasource, connection_data=decrypted_connection_data)
        connection_details["connection_details"] = _conn_details

        connection_model: ConnectionDetails = self.connection_session.update_connection(
            connection_id=connection_id, connection_details=connection_details
        )

        response_data = {
            "id": connection_model.connection_id,
            "name": connection_model.connection_name,
            "description": connection_model.connection_description,
            "datasource_name": connection_model.datasource_name,
            "connection_details": connection_model.masked_connection_details,
            "created_by": connection_model.created_by,
            "last_modified_by": connection_model.last_modified_by,
        }
        return response_data

    def test_connection_by_id(self, connection_id: str):
        connection_model = self.connection_session.get_connection_model(connection_id=connection_id)
        # Use decrypted details for testing
        test_connection_data(
            datasource=connection_model.datasource_name, connection_data=connection_model.decrypted_connection_details
        )

    def delete_connection(self, connection_id: str):
        self.connection_session.delete_connection(connection_id=connection_id)

    def delete_all_connections(self) -> dict[str, Any]:
        return self.connection_session.delete_all_connections()

    def get_connection_dependent_projects(self, connection_id) -> list[dict[str, Any]]:
        projects_list: list[dict[str, Any]] = self.connection_session.get_projects_by_connection(
            connection_id=connection_id
        )
        return projects_list

    def get_connection_dependent_environments(self, connection_id) -> list[dict[str, Any]]:
        environments_list: list[dict[str, Any]] = self.connection_session.get_environments_by_connection(
            connection_id=connection_id
        )
        return environments_list

    def get_connection_usage(self, connection_id) -> dict[str, Any]:
        """Get both projects and environments using this connection."""
        env_list: list[dict[str, Any]] = self.connection_session.get_environments_by_connection(
            connection_id=connection_id
        )
        proj_list: list[dict[str, Any]] = self.connection_session.get_projects_by_connection(
            connection_id=connection_id
        )
        response = {"projects": proj_list, "environment": env_list}
        return response

    def test_connection(self, datasource: str, connection_data: dict[str, Any], connection_id: str = None):
        # Decrypt sensitive fields from frontend encrypted data using robust method
        decrypted_connection_data = decrypt_connection_details_robust(connection_data)

        # If editing an existing connection, merge masked fields with stored values
        if connection_id:
            stored_model = self.connection_session.get_connection_model(connection_id)
            if stored_model:
                stored_details = stored_model.decrypted_connection_details
                decrypted_connection_data = _merge_with_stored(decrypted_connection_data, stored_details)

        test_connection_data(datasource=datasource, connection_data=decrypted_connection_data)
