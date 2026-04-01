import logging
import os
import traceback
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml
from django.utils.text import slugify
from google.cloud import storage

from backend.errors.exceptions import UnhandledErrorMessage, VisitranCoreExceptions
from backend.utils.constants import FileConstants as Fc
from visitran.constants import CloudConstants
from visitran.errors import VisitranBaseExceptions

DB_TYPE_MAPPER: dict[str, str] | None = None


def handle_custom_exceptions(func) -> Any:
    """This decorator is used to handle any exceptions and creates a visitran
    cloud raised exceptions."""

    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except VisitranBaseExceptions as e:
            logging.error(f" -- Visitran backend exceptions {e.__class__.__name__} --")
            logging.error(f" {e.__str__()} ")
            raise VisitranCoreExceptions(error_message=e.__str__())
        except Exception as e:
            logging.critical(f" -- Unhandled exceptions {e.__class__.__name__} --")
            logging.critical(f" {e.__str__()} --")
            logging.critical(traceback.format_exc())
            # Some unhandled error where occurred
            raise UnhandledErrorMessage(error_obj=e)

    return wrapper


def get_project_base_path(project_name: str) -> str:
    # Validating the project to avoid path traversal
    project_slug: str = slugify(project_name)
    base_path: str = os.path.join(Fc.PROJECT_PATH, project_slug)
    return base_path


def download_from_gcs(gcs_url: str, destination_file_name: str):
    parsed_url = urlparse(gcs_url)
    client = storage.Client()
    bucket = client.bucket(CloudConstants.BUCKET_NAME)
    blob = bucket.blob(parsed_url.path.split(CloudConstants.BUCKET_NAME)[-1].lstrip("/"))
    # Ensure the destination directory exists
    os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
    blob.download_to_filename(destination_file_name)


def db_type_mapper() -> dict[str, str]:
    """
    String
    Number
    Boolean
    Time
    Date
    """
    global DB_TYPE_MAPPER
    if DB_TYPE_MAPPER is None:
        mapper_path = Path(__file__).parent / "dbtype_mapper.yaml"
        DB_TYPE_MAPPER = yaml.safe_load(mapper_path.read_text())
    return DB_TYPE_MAPPER


def convert_db_type_to_no_code_type(db_type: str) -> str:
    db_type = "".join([i for i in db_type if not i.isdigit() and i.isalnum()])
    db_type = db_type.lower()
    return db_type_mapper().get(db_type, "String")
