import os

from django.utils.text import slugify

from backend.application.sample_project.sample_project import SampleProject
from backend.utils.constants import JaffleShopProjectConstants


class JaffleShopProjectFinal(SampleProject):

    def __init__(self):
        super().__init__()
        self._base_path = None
        self._prefix = "Finalized"
        self.project_base_name = f"{JaffleShopProjectConstants.name} {self._prefix}"
        self._first_project_exist = self.project_exist(f"{JaffleShopProjectConstants.name} {self._prefix}")

    @property
    def master_db_name(self):
        return "jaffleshop_final_master"

    @property
    def base_path(self):
        if not self._base_path:
            self._base_path = os.path.join(slugify("backend"), "utils", "sample_project", "jaffle_shop")
        return self._base_path

    @property
    def project_name(self):
        if not self._first_project_exist:
            return f"{JaffleShopProjectConstants.name} {self._prefix}"
        return f"{JaffleShopProjectConstants.name} {self._prefix} {self.timestamp_str}"

    @property
    def project_description(self):
        return f"{JaffleShopProjectConstants.description}"

    @property
    def database_name(self) -> str:
        return self.sanitize_name(f"{self.org_id}_{self._user_id}_js_db_f_{self.timestamp_str}")

    @property
    def user_name(self) -> str:
        return f"dbuser_{self.org_id}_{self._user_id}_js_f"[:63]

    @property
    def postgres_connection_details(self):
        return {
            "name": self.connection_name,
            "description": JaffleShopProjectConstants.connection_desc,
            "datasource_name": JaffleShopProjectConstants.datasource,
            "connection_details": self.sample_connection,
        }

    @property
    def connection_name(self):
        return f"{JaffleShopProjectConstants.connection_name} {self._prefix} {self.timestamp_str}"

    @property
    def model_list(self) -> list[str]:
        return JaffleShopProjectConstants.model_list

    @property
    def csv_files(self) -> list[str]:
        return JaffleShopProjectConstants.csv_files
