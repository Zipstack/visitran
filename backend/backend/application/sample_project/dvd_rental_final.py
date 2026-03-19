import os

from django.utils.text import slugify

from backend.application.sample_project.sample_project import SampleProject
from backend.utils.constants import DvdRentalProjectConstants


class DvdRentalProjectFinal(SampleProject):

    def __init__(self):
        super().__init__()
        self._base_path = None
        self._prefix = "Finalized"
        self.project_base_name = f"{DvdRentalProjectConstants.name} {self._prefix}"
        self._first_project_exist = self.project_exist(f"{DvdRentalProjectConstants.name} {self._prefix}")

    @property
    def master_db_name(self):
        return "dvd_rental_final_master"

    @property
    def base_path(self):
        if not self._base_path:
            self._base_path = os.path.join(slugify("backend"), "utils", "sample_project", "dvd_rental")
        return self._base_path

    @property
    def project_name(self):
        if not self._first_project_exist:
            return f"{DvdRentalProjectConstants.name} {self._prefix}"
        return f"{DvdRentalProjectConstants.name} {self._prefix} {self.timestamp_str}"

    @property
    def project_description(self):
        return DvdRentalProjectConstants.description

    @property
    def database_name(self) -> str:
        return self.sanitize_name(f"{self.org_id}_{self._user_id}_dvd_db_f_{self.timestamp_str}")

    @property
    def user_name(self) -> str:
        return f"dbuser_{self.org_id}_{self._user_id}_dvd_f"[:63]

    @property
    def postgres_connection_details(self):
        return {
            "name": self.connection_name,
            "description": DvdRentalProjectConstants.connection_desc,
            "datasource_name": DvdRentalProjectConstants.datasource,
            "connection_details": self.sample_connection,
        }

    @property
    def connection_name(self):
        return f"{DvdRentalProjectConstants.connection_name} {self._prefix} {self.timestamp_str}"

    @property
    def model_list(self) -> list[str]:
        return DvdRentalProjectConstants.model_list

    @property
    def csv_files(self) -> list[str]:
        return DvdRentalProjectConstants.csv_files
