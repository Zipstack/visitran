import hashlib
import json
import logging
import os
import re
from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime

import psycopg2
from django.conf import settings
from django.core.files.base import File
from jinja2 import Environment, FileSystemLoader

from backend.application.context.application import ApplicationContext
from backend.application.context.connection import ConnectionContext
from backend.application.utils import get_filter
from backend.core.models.config_models import ConfigModels
from backend.core.models.connection_models import ConnectionDetails
from backend.core.models.dependent_models import DependentModels
from backend.core.models.project_details import ProjectDetails
from backend.errors import SampleProjectConnectionFailed, MasterDbNotExist
from backend.errors.exceptions import SampleProjectLimitExceed
from backend.server.settings.base import SAMPLE_CONNECTION
from backend.utils.tenant_context import get_current_tenant, get_current_user


class SampleProject(ABC):

    def __init__(self):
        self._template_environment = None
        self._project_context = None
        self._application_context = None
        self._sample_seed_path = None
        self._sample_model_path = None
        self._sample_py_path = None
        self._sample_template_path = None
        self.sample_connection = deepcopy(SAMPLE_CONNECTION)
        self._postgres_connection = None
        self._org_id: str = re.sub(r"[^A-Za-z0-9_]", "_", get_current_tenant().lower() or "default_org").strip("_")
        self._user_id: str = re.sub(r"[^A-Za-z0-9_]", "_", get_current_user().get("username")).strip("_")
        self.password = self.create_password(self._org_id)
        self._clone_db = True
        self.timestamp_str = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        self.project_limit = settings.SAMPLE_PROJECT_LIMIT
        self.project_base_name = None

    @property
    @abstractmethod
    def master_db_name(self):
        pass

    @property
    @abstractmethod
    def base_path(self):
        pass

    @property
    def seed_path(self):
        if not self._sample_seed_path:
            self._sample_seed_path = os.path.join(self.base_path, "seed_files")
        return self._sample_seed_path

    @property
    def model_path(self):
        if not self._sample_model_path:
            self._sample_model_path = os.path.join(self.base_path, "model_files")
        return self._sample_model_path

    @property
    def model_py_path(self):
        if not self._sample_py_path:
            self._sample_py_path = os.path.join(self.base_path, "model_py_files")
        return self._sample_py_path

    @property
    def model_template_path(self):
        if not self._sample_template_path:
            self._sample_template_path = os.path.join(self.base_path, "model_templates")
        return self._sample_template_path

    @property
    def template_environment(self):
        if not self._template_environment:
            self._template_environment = Environment(loader=FileSystemLoader(self.model_template_path))
        return self._template_environment

    @property
    def org_id(self):
        return re.sub(r"[^A-Za-z0-9_]", "_", get_current_tenant().lower() or "default_org").strip("_")

    @property
    @abstractmethod
    def database_name(self) -> str:
        pass

    @property
    @abstractmethod
    def user_name(self) -> str:
        pass

    @staticmethod
    def create_password(org_id: str):
        hash_object = hashlib.sha256(org_id.encode())
        hex_dig = hash_object.hexdigest()
        return hex_dig[:10]

    @property
    @abstractmethod
    def project_name(self) -> str:
        pass

    @property
    @abstractmethod
    def project_description(self) -> str:
        pass

    @property
    @abstractmethod
    def postgres_connection_details(self) -> dict[str, str]:
        pass

    @property
    @abstractmethod
    def connection_name(self):
        pass

    @property
    @abstractmethod
    def csv_files(self):
        pass

    @property
    @abstractmethod
    def model_list(self):
        pass

    @property
    def schema_name(self):
        return "raw"

    @property
    def postgres_connection(self):
        if not self._postgres_connection:
            self._postgres_connection = psycopg2.connect(
                host=self.sample_connection["host"],
                port=self.sample_connection["port"],
                user=self.sample_connection["user"],
                password=self.sample_connection["passw"],
                dbname=self.sample_connection["dbname"],
            )
            # Required to execute DROP DATABASE
            self._postgres_connection.autocommit = True
        return self._postgres_connection

    def execute_sql_queries(self, statements: list[str]):
        """This method is used to execute the sql queries."""
        try:
            cursor = self.postgres_connection.cursor()
            for statement in statements:
                cursor.execute(statement)
            cursor.close()
        except psycopg2.Error as e:
            if self.master_db_name in str(e):
                raise MasterDbNotExist()
            logging.error(f"Error on querying the database --> {e}")
            raise SampleProjectConnectionFailed()

    def _grant_schema_permissions_on_new_db(self):
        """Grant schema permissions on the newly cloned database.

        This must be executed on the NEW database, not the admin
        database.
        """
        new_db_connection = None
        try:
            new_db_connection = psycopg2.connect(
                host=self.sample_connection["host"],
                port=self.sample_connection["port"],
                user=self.sample_connection["user"],
                password=self.sample_connection["passw"],
                dbname=self.database_name,  # Connect to the NEW database
            )
            new_db_connection.autocommit = True
            cursor = new_db_connection.cursor()

            schemas = ["raw", "dev", "stg", "prod"]
            for schema in schemas:
                cursor.execute(f"GRANT USAGE ON SCHEMA {schema} TO {self.user_name};")
                cursor.execute(
                    f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {self.user_name};"
                )
                cursor.execute(
                    f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} "
                    f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {self.user_name};"
                )
            # Transfer schema ownership for non-source schemas so the project user
            # can DROP and recreate tables during transformation runs.
            # Schema owners can drop any object within their schema in PostgreSQL.
            # raw schema stays read-only (source data from template).
            for schema in ["dev", "stg", "prod"]:
                cursor.execute(f"ALTER SCHEMA {schema} OWNER TO {self.user_name};")

            cursor.close()
            logging.info(f"Schema permissions granted on database {self.database_name}")
        except psycopg2.Error as e:
            logging.error(f"Error granting schema permissions on new database: {e}")
            raise SampleProjectConnectionFailed()
        finally:
            if new_db_connection:
                new_db_connection.close()

    def close_postgres_connection(self):
        """Properly close the postgres connection."""
        if self._postgres_connection:
            try:
                self._postgres_connection.close()
                logging.info("Postgres connection closed successfully")
            except psycopg2.Error as e:
                logging.warning(f"Error closing postgres connection: {e}")
            finally:
                self._postgres_connection = None

    def load_app_context(self):
        """This method is used to load the application context."""
        if self.project_context:
            self._application_context = ApplicationContext(project_id=self.project_context.project_id)

    @property
    def app_context(self) -> ApplicationContext:
        """This property is used to get the application context."""
        if not self._application_context:
            self.load_app_context()
        return self._application_context

    @property
    def project_context(self) -> ProjectDetails:
        """This property is used to get the project context."""
        if not self._project_context:
            project_filter = {"project_name": self.project_name, "is_sample": True}
            project_filter.update(get_filter())
            pd: ProjectDetails = ProjectDetails.objects.filter(**project_filter).first()
            if pd:
                self._project_context = pd
        return self._project_context

    def load_sample_project(self) -> dict[str, str]:
        """This method is used to load the sample project.

        This will clear the existing project and database and create a
        new one.
        """
        self.check_project_limit()
        try:
            # Check if the database already exists
            # self.clear_existing_project()
            # self.clear_existing_db()
            self.create_new_database()
            sample_project_details = self.create_project_connection()
            self.create_schemas()
            self.upload_and_run_csv()
            self.create_and_load_models()
            logging.info("sample project created successfully")
            return sample_project_details
        finally:
            self.close_postgres_connection()

    def clear_existing_project(self):
        # Clearing the project if exists
        filter_criteria = {"project_name": self.project_name, "is_sample": True}
        filter_criteria.update(get_filter())
        pd: ProjectDetails = ProjectDetails.objects.filter(**filter_criteria).first()
        if pd:
            logging.info(
                f"Sample project with name {self.project_name} for User {filter_criteria['created_by__username']} is being deleted"
            )
            pd.delete()
        logging.info("existing sample project deleted")

    def clear_existing_db(self):
        # Clearing the connection if exists
        connection_filter = {"connection_name": self.connection_name}
        connection_filter.update(get_filter())
        cd: ConnectionDetails = ConnectionDetails.objects.filter(**connection_filter).first()
        if cd:
            cd.delete()

        # Clearing existing users and databases
        terminate_session = (
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity " f"WHERE datname = '{self.database_name}';"
        )
        drop_database = f"drop database if exists {self.database_name};"
        drop_user = f"drop user if exists {self.user_name};"
        sql_statements = [
            terminate_session,
            # revoke_privilege,
            drop_database,
            drop_user,
        ]
        self.execute_sql_queries(statements=sql_statements)
        logging.info("existing sample db and user deleted")

    def create_new_database(self):
        """This method is used to create a new database."""
        create_db_query = f"CREATE DATABASE {self.database_name};"
        user_check_query = f"SELECT 1 FROM pg_roles WHERE rolname = {self.user_name}"
        create_user_query = f"CREATE USER {self.user_name} WITH ENCRYPTED PASSWORD '{self.password}';"
        grant_role_query = f"GRANT ALL PRIVILEGES ON DATABASE {self.database_name} TO {self.user_name};"
        statements = [create_db_query]
        if not self.user_exist():
            statements.append(create_user_query)
        statements.append(grant_role_query)
        if self._clone_db:
            logging.info(f"creating(cloning) new sample db with the name - {self.database_name}")
            create_template_db_query = f"CREATE DATABASE {self.database_name} TEMPLATE {self.master_db_name};"
            statements[0] = create_template_db_query
        try:
            self.execute_sql_queries(statements=statements)
            logging.info(
                f"new sample db and user created with the name - {self.user_name} database - {self.database_name}"
            )
            # GRANT schema permissions on the NEW database (not the admin DB)
            if self._clone_db:
                self._grant_schema_permissions_on_new_db()
        except MasterDbNotExist as e:
            if not self._clone_db:
                logging.info(f"Error creating database {self.database_name}: {e}. All retries completed")
                raise e
            logging.info(f"Error creating database {self.database_name}: {e}. Retrying with empty template...")
            self._clone_db = False
            self.create_new_database()
            # self.grant_permissions()
            logging.info(f"Error creating database {self.database_name}: {e}. Retrying...")

    def user_exist(self) -> bool:
        try:
            cursor = self.postgres_connection.cursor()
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (self.user_name,))
            user_exist = cursor.fetchone() is not None
            cursor.close()
            logging.info(f"checking user already exist, result : {user_exist}")
            return user_exist
        except psycopg2.Error as e:
            logging.critical(f"Exception while checking user")
            return False

    def grant_permissions(self):
        """This method is used to grant the permissions to the user and
        database."""
        statements = [
            f"CREATE USER {self.user_name} WITH ENCRYPTED PASSWORD '{self.password}';",
            f"GRANT ALL PRIVILEGES ON DATABASE {self.database_name} TO {self.user_name};",
        ]
        self.execute_sql_queries(statements=statements)
        logging.info(f"new sample db and user created with the name - {self.user_name} database - {self.database_name}")

    def check_project_limit(self):
        filter_criteria = {"is_sample": True, "project_name__startswith": self.project_base_name}
        filter_criteria.update(get_filter())
        sample_project_count = ProjectDetails.objects.filter(**filter_criteria).count()
        if sample_project_count >= self.project_limit:
            raise SampleProjectLimitExceed(
                project_base_name=self.project_base_name,
                sample_project_count=sample_project_count,
                sample_project_limit=self.project_limit,
            )

    def create_project_connection(self):
        """This method is used to create the project connection."""
        self.sample_connection["passw"] = self.password
        self.sample_connection["dbname"] = self.database_name
        self.sample_connection["user"] = self.user_name
        self.sample_connection["schema"] = self.schema_name

        connection_filter = {
            "connection_name": self.postgres_connection_details["name"],
        }
        connection_filter.update(get_filter())

        con_context = ConnectionContext()
        con_exist = ConnectionDetails.objects.filter(**connection_filter).first()
        if con_exist:
            logging.info(f"sample connection {con_exist} exists, proceeding to delete ")
            con_exist.delete()
            logging.info(f"existing sample connection {con_exist} deleted")

        connection_data = con_context.create_connection(connection_details=self.postgres_connection_details)
        connection_instance_id = connection_data.get("id")
        connection_instance = ConnectionDetails.objects.filter(connection_id=connection_instance_id).first()
        logging.info("Create connection is success")

        pd = ProjectDetails(
            project_name=self.project_name,
            project_description=self.project_description,
            connection_model=connection_instance,
            created_by=get_current_user(),
            is_sample=True,
        )
        pd.save()
        project_details = self.app_context.get_project_details()
        self.close_postgres_connection()
        return project_details

    def create_schemas(self):
        """Create required schemas in the newly created database."""
        if self._clone_db:
            return
        try:
            statements = []
            # Create schemas using raw SQL
            schemas = ["raw", "dev", "stg", "prod"]
            for schema in schemas:
                statements.append(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
                statements.append(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            self.execute_sql_queries(statements=statements)
            logging.info("schemas created successfully")
        except Exception as e:
            logging.info(f"Error creating schemas: {e}")

    def upload_and_run_csv(self):
        csv_files = self.csv_files

        for csv_file in csv_files:
            file = os.path.join(self.seed_path, csv_file)
            if not os.path.exists(file):
                logging.info({"error": "File not found"})
            with open(file, "rb") as file_content:
                # Extract the filename from the path
                file_name = os.path.basename(file)

                # Wrap the file content in Django's File class to handle it efficiently
                uploaded_file = File(file_content)

                # Upload the file using the app's method
                self.app_context.upload_a_file(file_name=file_name, file_content=uploaded_file)
        logging.info("CSV files uploaded successfully")
        if not self._clone_db:
            seed = {"runAll": True, "schema_name": self.schema_name}
            self.app_context.execute_visitran_seed_command(seed)
            logging.info("seed command is executed")

    def create_and_load_models(self):
        model_list = self.model_list

        for model_name in model_list:
            logging.info(f"starting execution for {model_name}")
            model_path = os.path.join(self.model_path, model_name)
            with open(f"{model_path}.json") as model_file:
                # Extract the filename from the path
                config_model_data = json.load(model_file)

            template = self.template_environment.get_template(f"{model_name}.jinja")
            output = template.render({"model_path": self._project_context.project_py_name})

            config_model = ConfigModels(
                project_instance=self.project_context,
                model_name=config_model_data["model_name"],
                model_data=config_model_data["model_data"],
            )
            config_model.save()
            self.app_context.session.update_model_content(model_name=model_name, model_content=output)

            for dependent_data in config_model_data["dependent_models"]:
                dp = DependentModels(
                    project_instance=self.project_context,
                    model=config_model,
                    transformation_id=dependent_data["transformation_id"],
                    model_data=dependent_data["model_data"],
                )
                dp.save()
            # add node to model graph and update model reference
            self.app_context.add_node_to_model_graph(model_name)
            self.app_context.update_sample_project_model_graph(config_model_data["model_data"], model_name)

        # IMPORTANT: Use app_context's project_instance to preserve the model graph
        # that was saved during add_node_to_model_graph/update_sample_project_model_graph.
        # Using self.project_context.save() would overwrite the graph with stale data.
        self.app_context.session.project_instance.is_completed = True
        self.app_context.session.project_instance.save()
        logging.info("Created and loaded all models successfully")

    def sanitize_name(self, name):
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name).lower()

        if not re.match(r"^[a-zA-Z_]", sanitized):
            sanitized = f"_{sanitized}"

        # Create a short hash from the full original input (8 chars)
        suffix = hashlib.sha256(name.encode()).hexdigest()[:8]

        # Leave space for underscore and hash suffix (9 total)
        base = sanitized[: 63 - 9]  # 63 - 1 (_) - 8 (hash)

        return f"{base}_{suffix}"

    def project_exist(self, project_name: str):
        try:
            project_filter = {"project_name": project_name, "is_sample": True}
            project_filter.update(get_filter())
            return ProjectDetails.objects.filter(**project_filter).exists()
        except:
            return False
