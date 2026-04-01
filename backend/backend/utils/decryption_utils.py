"""Decryption utilities for handling encrypted data from frontend."""

import base64
import json
import logging
from typing import Dict, Any, Union

from backend.utils.rsa_encryption import decrypt_with_private_key, validate_encrypted_data, get_encryption_debug_info

# Sensitive fields that should be decrypted
SENSITIVE_FIELDS = {
    "password",
    "api_key",
    "access_key",
    "secret_key",
    "token",
    "connection_string",
    "passw",
    "connection_url",
    "key",
    "secret",
    "auth_token",
    "api_token",
    "private_key",
    "client_secret",
    "refresh_token",
    "bearer_token",
    "session_token",
    "encryption_key",
    "master_key",
    "app_secret",
    "webhook_secret",
    "signing_key",
    "encryption_secret",
    "auth_code",
    "verification_token",
    # BigQuery specific fields
    "client_email",
    "client_id",
    "private_key_id",
    "private_key",
    "project_id",
}


def decrypt_chunked_value(encrypted_value: str) -> str:
    """Decrypt a value that was encrypted using chunked encryption.

    Args:
        encrypted_value: The encrypted value (may be chunked)

    Returns:
        Decrypted value
    """
    try:
        # Check if this is a chunked value (contains '|' delimiter)
        if '|' in encrypted_value:
            # Split into chunks
            chunks = encrypted_value.split('|')

            # Decrypt each chunk
            decrypted_chunks = []
            for i, chunk in enumerate(chunks):
                decrypted_chunk = decrypt_with_private_key(chunk)
                if decrypted_chunk is None:
                    logging.error(f"Failed to decrypt chunk {i + 1}")
                    return encrypted_value  # Return original on error
                decrypted_chunks.append(decrypted_chunk)

            # Combine chunks
            result = ''.join(decrypted_chunks)
            return result
        else:
            # Not chunked, decrypt normally
            return decrypt_with_private_key(encrypted_value)
    except Exception as e:
        logging.error(f"Error decrypting chunked value: {e}")
        return encrypted_value  # Return original on error


def decrypt_bigquery_credentials(credentials_json: str) -> str:
    """Decrypt BigQuery credentials specifically.

    Args:
        credentials_json: The BigQuery credentials JSON string

    Returns:
        Decrypted credentials JSON string
    """
    try:
        # Parse the credentials JSON
        credentials = json.loads(credentials_json)

        # Decrypt sensitive fields within the credentials
        decrypted_credentials = credentials.copy()

        # List of sensitive fields in BigQuery service account JSON
        bigquery_sensitive_fields = [
            "private_key",
            "client_email",
            "client_id",
            "private_key_id",
            "project_id"
        ]

        for field in bigquery_sensitive_fields:
            if field in decrypted_credentials and isinstance(decrypted_credentials[field], str):
                try:
                    # Use chunked decryption for large fields like private_key
                    decrypted_value = decrypt_chunked_value(decrypted_credentials[field])
                    if decrypted_value is not None:
                        decrypted_credentials[field] = decrypted_value
                except Exception as e:
                    logging.warning(f"Failed to decrypt BigQuery field '{field}': {e}")
                    # Keep original value on error
                    pass

        # Return the decrypted credentials as a JSON string
        return json.dumps(decrypted_credentials)
    except Exception as e:
        logging.error(f"Error decrypting BigQuery credentials: {e}")
        return credentials_json  # Return original on error


def decrypt_sensitive_fields(data: Union[dict[str, Any], list, Any]) -> Union[dict[str, Any], list, Any]:
    """Recursively decrypt sensitive fields in data structures.

    Args:
        data: The data to decrypt (dict, list, or primitive value)

    Returns:
        The data with sensitive fields decrypted
    """
    if data is None:
        return data

    if isinstance(data, dict):
        return _decrypt_dict(data)
    elif isinstance(data, list):
        return _decrypt_list(data)
    else:
        return data


def _decrypt_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Decrypt sensitive fields in a dictionary."""
    if not data:
        return data

    decrypted_data = data.copy()

    for key, value in data.items():
        if isinstance(value, dict):
            # Recursively decrypt nested dictionaries
            decrypted_data[key] = _decrypt_dict(value)
        elif isinstance(value, list):
            # Recursively decrypt lists
            decrypted_data[key] = _decrypt_list(value)
        elif isinstance(value, str) and key.lower() in SENSITIVE_FIELDS:
            # Try to decrypt sensitive string fields
            try:
                decrypted_value = decrypt_with_private_key(value)
                if decrypted_value is not None:
                    decrypted_data[key] = decrypted_value
                else:
                    # If decryption fails, assume it's not encrypted (backward compatibility)
                    decrypted_data[key] = value
            except Exception as e:
                logging.exception(f"Error decrypting field '{key}'")
                # Keep original value on error
                decrypted_data[key] = value
        elif key == "credentials" and isinstance(value, str):
            # Special handling for BigQuery credentials
            try:
                # Check if it's valid JSON (BigQuery credentials)
                json.loads(value)
                decrypted_data[key] = decrypt_bigquery_credentials(value)
            except json.JSONDecodeError:
                # Not valid JSON, treat as regular sensitive field
                try:
                    decrypted_value = decrypt_with_private_key(value)
                    if decrypted_value is not None:
                        decrypted_data[key] = decrypted_value
                    else:
                        decrypted_data[key] = value
                except Exception as e:
                    logging.exception(f"Error decrypting credentials field")
                    decrypted_data[key] = value

    return decrypted_data


def _decrypt_list(data: list) -> list:
    """Decrypt sensitive fields in a list."""
    if not data:
        return data

    decrypted_list = []
    for item in data:
        if isinstance(item, dict):
            decrypted_list.append(_decrypt_dict(item))
        elif isinstance(item, list):
            decrypted_list.append(_decrypt_list(item))
        else:
            decrypted_list.append(item)

    return decrypted_list


def decrypt_connection_data(connection_data: dict[str, Any]) -> dict[str, Any]:
    """Decrypt sensitive fields in connection data.

    Args:
        connection_data: Connection data dictionary

    Returns:
        Connection data with sensitive fields decrypted
    """
    return decrypt_sensitive_fields(connection_data)


def decrypt_request_data(request_data: dict[str, Any]) -> dict[str, Any]:
    """Decrypt sensitive fields in request data.

    Args:
        request_data: Request data dictionary

    Returns:
        Request data with sensitive fields decrypted
    """
    return decrypt_sensitive_fields(request_data)


def is_encrypted_value(value: str) -> bool:
    """Check if a value appears to be encrypted.

    Args:
        value: The value to check

    Returns:
        True if the value appears to be encrypted, False otherwise
    """
    if not isinstance(value, str):
        return False

    # Check if it looks like base64 encoded encrypted data
    # Encrypted data is typically longer and contains base64 characters
    if len(value) > 100 and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=' for c in value):
        return True

    return False


def get_sensitive_fields_in_data(data: Union[dict[str, Any], list]) -> list:
    """Get list of sensitive fields found in the data.

    Args:
        data: The data to analyze

    Returns:
        List of sensitive field names found
    """
    sensitive_fields = []

    if isinstance(data, dict):
        for key, value in data.items():
            if key.lower() in SENSITIVE_FIELDS:
                sensitive_fields.append(key)
            elif isinstance(value, (dict, list)):
                sensitive_fields.extend(get_sensitive_fields_in_data(value))
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                sensitive_fields.extend(get_sensitive_fields_in_data(item))

    return list(set(sensitive_fields))  # Remove duplicates


def decrypt_with_logging(data: dict[str, Any], context: str = "unknown") -> dict[str, Any]:
    """Decrypt data with detailed logging for debugging.

    Args:
        data: The data to decrypt
        context: Context string for logging (e.g., "connection_creation", "test_connection")

    Returns:
        Decrypted data
    """
    logging.info(f"Starting decryption for context: {context}")

    # Find sensitive fields before decryption
    sensitive_fields = get_sensitive_fields_in_data(data)
    if sensitive_fields:
        logging.info(f"Found sensitive fields in {context}: {sensitive_fields}")

    # Decrypt the data
    decrypted_data = decrypt_sensitive_fields(data)

    # Log decryption results
    for field in sensitive_fields:
        if field in data and field in decrypted_data:
            original_value = data[field]
            decrypted_value = decrypted_data[field]

            if is_encrypted_value(original_value):
                if original_value != decrypted_value:
                    logging.info(f"Successfully decrypted field '{field}' in {context}")
                else:
                    logging.warning(f"Failed to decrypt field '{field}' in {context}, using original value")
            else:
                logging.debug(f"Field '{field}' was not encrypted in {context}")

    logging.info(f"Completed decryption for context: {context}")
    return decrypted_data


# Convenience functions for specific use cases
def decrypt_connection_creation_data(connection_details: dict[str, Any]) -> dict[str, Any]:
    """Decrypt data for connection creation."""
    return decrypt_with_logging(connection_details, "connection_creation")


def decrypt_connection_update_data(connection_details: dict[str, Any]) -> dict[str, Any]:
    """Decrypt data for connection update."""
    return decrypt_with_logging(connection_details, "connection_update")


def decrypt_test_connection_data(connection_data: dict[str, Any]) -> dict[str, Any]:
    """Decrypt data for test connection."""
    return decrypt_with_logging(connection_data, "test_connection")


def decrypt_environment_data(environment_data: dict[str, Any]) -> dict[str, Any]:
    """Decrypt data for environment creation/update."""
    return decrypt_with_logging(environment_data, "environment_management")


def decrypt_connection_details_safe(connection_details: dict[str, Any]) -> dict[str, Any]:
    """Safely decrypt connection_details with detailed error reporting.

    Args:
        connection_details: Connection details dictionary

    Returns:
        Connection details with sensitive fields decrypted
    """
    logging.info("Starting connection_details decryption...")

    if not connection_details:
        logging.warning("connection_details is empty or None")
        return connection_details

    logging.info(f"connection_details type: {type(connection_details)}")
    logging.info(f"connection_details keys: {list(connection_details.keys())}")

    try:
        # Find sensitive fields before decryption
        sensitive_fields = get_sensitive_fields_in_data(connection_details)
        logging.info(f"Found sensitive fields in connection_details: {sensitive_fields}")

        # Decrypt the data
        decrypted_data = decrypt_sensitive_fields(connection_details)

        # Log decryption results
        for field in sensitive_fields:
            if field in connection_details and field in decrypted_data:
                original_value = connection_details[field]
                decrypted_value = decrypted_data[field]

                if is_encrypted_value(original_value):
                    if original_value != decrypted_value:
                        logging.info(f"Successfully decrypted field '{field}' in connection_details")
                    else:
                        logging.warning(f"Failed to decrypt field '{field}' in connection_details, using original value")
                else:
                    logging.debug(f"Field '{field}' was not encrypted in connection_details")

        logging.info("Completed connection_details decryption")
        return decrypted_data

    except Exception as e:
        logging.error(f"Error during connection_details decryption: {e}")
        logging.error(f"connection_details content: {connection_details}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        # Return original data on error
        return connection_details


def decrypt_connection_details_robust(connection_details: dict[str, Any]) -> dict[str, Any]:
    """Robustly decrypt connection_details with comprehensive error handling.

    This function handles various scenarios:
    - Fully encrypted sensitive fields
    - Partially encrypted sensitive fields
    - Non-encrypted sensitive fields (backward compatibility)
    - Malformed encrypted data
    - BigQuery credentials with nested sensitive fields

    Args:
        connection_details: Connection details dictionary

    Returns:
        Connection details with sensitive fields decrypted
    """
    logging.info("Starting robust connection_details decryption...")

    if not connection_details:
        logging.warning("connection_details is empty or None")
        return connection_details

    logging.info(f"connection_details type: {type(connection_details)}")
    logging.info(f"connection_details keys: {list(connection_details.keys())}")

    try:
        # Create a copy to avoid modifying the original
        decrypted_data = connection_details.copy()

        # Special handling for BigQuery credentials field
        if "credentials" in connection_details and isinstance(connection_details["credentials"], str):
            try:
                # Check if it's valid JSON (BigQuery credentials)
                json.loads(connection_details["credentials"])
                logging.info("Detected BigQuery credentials, using special decryption...")
                decrypted_data["credentials"] = decrypt_bigquery_credentials(connection_details["credentials"])
                logging.info("Successfully decrypted BigQuery credentials")
            except json.JSONDecodeError:
                # Not valid JSON — may be RSA-encrypted or a masked sentinel.
                # Try RSA decryption; if that fails, keep original (merge will handle masked values).
                logging.debug("Credentials field is not valid JSON, trying RSA decryption")
                try:
                    decrypted_value = decrypt_chunked_value(connection_details["credentials"])
                    if decrypted_value is not None and decrypted_value != connection_details["credentials"]:
                        # RSA decrypted successfully — check if result is valid JSON
                        try:
                            json.loads(decrypted_value)
                            decrypted_data["credentials"] = decrypt_bigquery_credentials(decrypted_value)
                        except json.JSONDecodeError:
                            # Decrypted but not JSON (e.g. decrypted masked sentinel "********")
                            decrypted_data["credentials"] = decrypted_value
                    else:
                        decrypted_data["credentials"] = connection_details["credentials"]
                except Exception:
                    decrypted_data["credentials"] = connection_details["credentials"]
            except Exception as e:
                logging.error(f"Error decrypting BigQuery credentials: {e}")
                decrypted_data["credentials"] = connection_details["credentials"]

        # Find other sensitive fields
        sensitive_fields = get_sensitive_fields_in_data(decrypted_data)
        logging.info(f"Found sensitive fields: {sensitive_fields}")

        # Process each sensitive field (excluding credentials which was handled above)
        for field in sensitive_fields:
            if field in decrypted_data and field != "credentials":  # Skip credentials as it's already handled
                original_value = decrypted_data[field]

                if isinstance(original_value, str):
                    # Validate the encrypted data
                    validation = validate_encrypted_data(original_value)
                    if validation["errors"]:
                        logging.warning(f"Field '{field}' validation errors: {validation['errors']}")

                    # Check if it appears to be encrypted
                    if is_encrypted_value(original_value):
                        logging.info(f"Field '{field}' appears to be encrypted, attempting decryption...")

                        # Log detailed debug info for problematic fields
                        if validation["warnings"] or not validation["is_valid"]:
                            logging.debug(get_encryption_debug_info(original_value))

                        try:
                            decrypted_value = decrypt_with_private_key(original_value)
                            if decrypted_value is not None:
                                decrypted_data[field] = decrypted_value
                                logging.info(f"✅ Successfully decrypted field '{field}'")
                            else:
                                logging.warning(f"⚠️  Decryption returned None for field '{field}', keeping original")
                                decrypted_data[field] = original_value
                        except Exception as e:
                            logging.error(f"❌ Error decrypting field '{field}': {e}")
                            logging.debug(f"Field '{field}' problematic value: {original_value}")
                            # Keep original value on error
                            decrypted_data[field] = original_value
                    else:
                        logging.debug(f"Field '{field}' does not appear to be encrypted, keeping as-is")
                        decrypted_data[field] = original_value
                else:
                    logging.debug(f"Field '{field}' is not a string, keeping as-is")
                    decrypted_data[field] = original_value

        logging.info("Completed robust connection_details decryption")
        return decrypted_data

    except Exception as e:
        logging.error(f"❌ Critical error during connection_details decryption: {e}")
        logging.error(f"connection_details content: {connection_details}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        # Return original data on critical error
        return connection_details


def is_valid_encrypted_data(value: str) -> bool:
    """Check if a value is valid encrypted data.

    Args:
        value: The value to check

    Returns:
        True if the value appears to be valid encrypted data
    """
    if not isinstance(value, str):
        return False

    # Check if it's a reasonable length for encrypted data
    if len(value) < 100:
        return False

    # Check if it contains only base64 characters
    valid_chars = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=')
    if not all(c in valid_chars for c in value):
        return False

    # Check if it's properly padded base64
    try:
        # Try to decode as base64
        decoded = base64.b64decode(value)
        return len(decoded) > 0
    except Exception:
        return False


def decrypt_field_safely(field_name: str, field_value: str) -> str:
    """Safely decrypt a single field with comprehensive error handling.

    Args:
        field_name: Name of the field being decrypted
        field_value: Value to decrypt

    Returns:
        Decrypted value or original value if decryption fails
    """
    logging.debug(f"Attempting to decrypt field '{field_name}'")

    if not isinstance(field_value, str):
        logging.debug(f"Field '{field_name}' is not a string, returning as-is")
        return field_value

    # Check if it looks like encrypted data
    if not is_valid_encrypted_data(field_value):
        logging.debug(f"Field '{field_name}' does not appear to be valid encrypted data")
        return field_value

    # Try to decrypt
    try:
        decrypted_value = decrypt_with_private_key(field_value)
        if decrypted_value is not None:
            logging.debug(f"✅ Successfully decrypted field '{field_name}'")
            return decrypted_value
        else:
            logging.warning(f"⚠️  Decryption returned None for field '{field_name}'")
            return field_value
    except Exception as e:
        logging.error(f"❌ Error decrypting field '{field_name}': {e}")
        return field_value
