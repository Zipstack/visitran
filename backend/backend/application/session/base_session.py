import json
import logging
import os
import shutil
import sys
from typing import List, Any, Optional

from django.core.exceptions import ValidationError

from backend.application.model_graph import ProjectModelGraph
from backend.application.utils import get_filter
from backend.core.models.config_models import ConfigModels
from backend.core.models.csv_models import CSVModels
from backend.core.models.project_details import ProjectDetails
from backend.core.redis_client import RedisClient
from backend.errors.exceptions import CSVFileNotExists, ProjectNotExist, ModelNotExists
from backend.utils.constants import FileConstants as Fc
from backend.utils.tenant_context import get_current_tenant

CACHE_TTL_SECONDS = 1800  # 30 minutes idle timeout


class BaseSession:
    def __init__(self, project_id: str):
        self._tenant_id = get_current_tenant()
        self.redis_client = RedisClient()
        filter_condition = get_filter()
        filter_condition["project_uuid"] = project_id
        try:
            self._project_instance = ProjectDetails.objects.get(**filter_condition)
            self._project_model_graph = ProjectModelGraph()
            self._project_model_graph.deserialize(self._project_instance.project_model_graph)
        except (ValidationError, ProjectDetails.DoesNotExist):
            raise ProjectNotExist(project_id=project_id)

    # --------------- CACHE: Helper methods (namespaced keys, JSON storage) ---------------
    @property
    def tenant_id(self) -> str:
        return self._tenant_id or "default_org"

    @property
    def project_id(self) -> str:
        return self.project_instance.project_id

    def _cache_key(self, *parts: str) -> str:
        # Keys: org:<tenant>:proj:<project>:<...>
        safe_parts = [str(p).replace(" ", "_") for p in parts]
        return f"org:{self.tenant_id}:proj:{self.project_id}:" + ":".join(safe_parts)

    def _cache_get(self, key: str) -> Optional[Any]:
        try:
            raw = self.redis_client.get(key)
            if raw is None:
                return None
            # sliding TTL: refresh on read
            self.redis_client.expire(key, CACHE_TTL_SECONDS)
            return json.loads(raw)
        except Exception:
            logging.exception("Cache get failed for key=%s", key)
            return None

    def _cache_set(self, key: str, value: Any, ttl: int = CACHE_TTL_SECONDS) -> None:
        try:
            self.redis_client.set(key, json.dumps(value), ex=ttl)
        except Exception:
            logging.exception("Cache set failed for key=%s", key)

    def _cache_delete(self, *keys: str) -> None:
        try:
            if not keys:
                return
            self.redis_client.delete(*keys)
        except Exception:
            logging.exception("Cache delete failed keys=%s", keys)

    # Bulk invalidation helpers
    def _invalidate_models_cache(self) -> None:
        # delete known keys; if pattern delete is available in RedisClient, use it
        keys = [
            self._cache_key("models", "all"),
            self._cache_key("models", "names"),
        ]
        self._cache_delete(*keys)

    def _invalidate_model_key(self, model_name: str) -> None:
        self._cache_delete(self._cache_key("model", model_name))

    def _invalidate_csv_cache(self, csv_name: Optional[str] = None) -> None:
        keys = [self._cache_key("csv", "all")]
        if csv_name:
            keys.append(self._cache_key("csv", "file", csv_name))
            keys.append(self._cache_key("csv", "exists", csv_name))
        self._cache_delete(*keys)

    def _invalidate_connection_dependent_keys(self) -> None:
        # For warehouse browsing in VisitranBackendContext
        keys = [
            self._cache_key("schemas", "list"),
            self._cache_key("tables", "list", "default"),
            # table lists are schema-scoped; we'll do best-effort invalidation for common names.
            # If RedisClient supports pattern delete, that should be used here.
        ]
        self._cache_delete(*keys)

    # ------------------------------ Existing properties ------------------------------
    @property
    def project_instance(self) -> ProjectDetails:
        return self._project_instance

    @property
    def project_py_name(self) -> str:
        return self.project_instance.project_py_name

    @property
    def project_path(self) -> str:
        return self.tenant_id + os.path.sep + Fc.PROJECT_PATH + os.path.sep

    @property
    def project_py_path(self) -> str:
        return self.project_path + self.project_id

    @property
    def model_path_prefix(self) -> str:
        model_path = os.path.join(self.project_py_path, self.project_py_name, Fc.MODELS)
        return str(model_path)

    @property
    def model_graph(self) -> ProjectModelGraph:
        return self._project_model_graph

    @property
    def sys_path(self) -> str:
        return os.getcwd() + os.path.sep + self.project_py_path

    def add_sys_path(self) -> None:
        logging.info(f"Adding sys path - {self.sys_path}")
        sys.path.append(self.sys_path)

    def sync_file_models(self):
        folder_path = os.path.join(self.sys_path, self.project_py_name)
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            shutil.rmtree(folder_path)

    def remove_sys_path(self) -> None:
        logging.info(f"Removing sys path - {self.sys_path}")
        if self.sys_path in sys.path:
            sys.path.remove(self.sys_path)
        logging.info(f" All the sys path - {sys.path}")

    # ------------------------------ Cached fetches ------------------------------
    def fetch_all_models(self, fetch_all=False) -> List[ConfigModels]:
        cache_key = self._cache_key("models", "all" if fetch_all else "active")
        cached = self._cache_get(cache_key)
        if cached is not None:
            logging.info(f"Fetching all models from cache: {cache_key}")
            # TODO - Need to fetch from the cache
            pass

        if fetch_all:
            all_models = self.project_instance.config_model.all()
        else:
            all_models = self.project_instance.config_model.filter(
                project_instance=self.project_instance, model_py_content__isnull=False
            ).exclude(model_py_content__exact="")

        logging.info(f" Models fetched from session in the project - {self.project_id}: {all_models}")

        # Serialize minimal fields for cache consumers (not replacing return path)
        serial = [
            {
                "model_name": m.model_name,
                "has_py": bool(m.model_py_content),
                "model_id": str(m.model_id),
            }
            for m in all_models
        ]
        self._cache_set(cache_key, serial)
        return list(all_models)

    def fetch_model(self, model_name: str) -> ConfigModels:
        logging.info(f" Model fetched from session in the project - {self.project_id}: {model_name}")
        cache_key = self._cache_key("model", model_name)
        cached = self._cache_get(cache_key)
        if cached is not None:
            logging.info("Cache hit: %s", cache_key)
            # To keep no DB calls after first, we cannot reconstruct ORM instance safely.
            # But callers expect ORM in many places. For compatibility, on first DB fetch we store a
            # slim snapshot and also the database primary key so we can refetch minimally if absolutely needed.
            # Since many call sites require full ORM, we must return ORM here; thus cache helps avoid recomputations,
            # but DB call still needed to return ORM unless we refactor callers.
            # Compromise: We will still avoid DB by trusting the cache and refetch only when file paths are needed.
            # Given current code heavily uses ORM, we will do DB fetch here but leverage cache in higher-level operations.
            pass

        try:
            obj = self.project_instance.config_model.get(model_name=model_name)
        except ConfigModels.DoesNotExist:
            raise ModelNotExists(model_name=model_name)

        # Cache minimal fields for other consumers
        snapshot = {
            "model_name": obj.model_name,
            "model_id": str(obj.model_id),
            "has_py": bool(obj.model_py_content),
        }
        self._cache_set(cache_key, snapshot)
        return obj

    def fetch_model_if_exists(self, model_name: str) -> Optional[ConfigModels]:
        cache_key = self._cache_key("model", "exists", model_name)
        cached = self._cache_get(cache_key)
        if cached is not None:
            logging.info("Cache hit: %s", cache_key)
            return self.project_instance.config_model.filter(model_name=model_name).first()

        config_model: ConfigModels = self.project_instance.config_model.filter(model_name=model_name).first()
        self._cache_set(cache_key, bool(config_model))
        return config_model

    def fetch_all_csv_files(self) -> List[CSVModels]:
        cache_key = self._cache_key("csv", "all")
        cached = self._cache_get(cache_key)
        if cached is not None:
            logging.info("Cache hit: %s", cache_key)
            # Return ORM list still expected; need DB unless callers accept dicts.
            # Keep DB call, but we keep a cache for other consumers.
            pass

        csvs = self.project_instance.csv_model.all()
        serial = [{"csv_name": c.csv_name, "csv_id": str(c.csv_id)} for c in csvs]
        self._cache_set(cache_key, serial)
        return list(csvs)

    def fetch_csv_model(self, csv_name: str) -> CSVModels:
        cache_key = self._cache_key("csv", "file", csv_name)
        cached = self._cache_get(cache_key)
        if cached is not None:
            logging.info("Cache hit: %s", cache_key)
        try:
            return self.project_instance.csv_model.get(csv_name=csv_name)
        except CSVModels.DoesNotExist:
            raise CSVFileNotExists(csv_name=csv_name)

    def fetch_csv_model_if_exists(self, csv_name: str) -> Optional[CSVModels]:
        cache_key = self._cache_key("csv", "exists", csv_name)
        cached = self._cache_get(cache_key)
        if cached is not None:
            logging.info("Cache hit: %s", cache_key)
            return self.project_instance.csv_model.filter(csv_name=csv_name).first() if cached else None

        model = self.project_instance.csv_model.filter(csv_name=csv_name).first()
        self._cache_set(cache_key, bool(model))
        return model
