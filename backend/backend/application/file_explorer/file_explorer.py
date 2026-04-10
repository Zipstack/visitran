from typing import Any, List, Dict
from collections import deque

from backend.application.session.session import Session
from backend.core.models.csv_models import CSVModels
from backend.core.models.project_details import ProjectDetails


def topological_sort_models(models_with_refs: List[Dict[str, Any]]) -> List[str]:
    """
    Sort models by execution order (DAG order) using topological sort.

    Models with no dependencies come first, followed by models that depend on them.
    This ensures the file explorer shows models in the order they would be executed.

    Args:
        models_with_refs: List of dicts with 'model_name' and 'references' keys
                         where 'references' is a list of model names this model depends on

    Returns:
        List of model names sorted in execution order (dependencies first)
    """
    if not models_with_refs:
        return []

    # Build adjacency list and in-degree count
    # model_name -> list of models that depend on it
    graph: Dict[str, List[str]] = {}
    in_degree: Dict[str, int] = {}
    all_model_names = set()

    for item in models_with_refs:
        model_name = item['model_name']
        all_model_names.add(model_name)
        graph.setdefault(model_name, [])
        in_degree.setdefault(model_name, 0)

    # Build edges: if model A references model B, then B -> A (B must come before A)
    for item in models_with_refs:
        model_name = item['model_name']
        references = item.get('references', []) or []

        for ref in references:
            # Only consider references that are in our model set (ignore raw/seed tables)
            if ref in all_model_names:
                graph.setdefault(ref, []).append(model_name)
                in_degree[model_name] = in_degree.get(model_name, 0) + 1

    # Kahn's algorithm for topological sort
    # Start with models that have no dependencies (in_degree == 0)
    queue = deque([m for m in all_model_names if in_degree.get(m, 0) == 0])
    sorted_models: List[str] = []

    while queue:
        model = queue.popleft()
        sorted_models.append(model)

        # Reduce in-degree for all models that depend on this one
        for dependent in graph.get(model, []):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # If there's a cycle, some models won't be in sorted_models
    # Add remaining models at the end (fallback for cycles)
    remaining = [m for m in all_model_names if m not in sorted_models]
    sorted_models.extend(remaining)

    return sorted_models


class FileExplorer:
    def __init__(self, project_instance: ProjectDetails) -> None:
        self._project_instance = project_instance

    @property
    def project_name(self) -> str:
        return self._project_instance.project_name

    def load_models(self, session: Session):
        # Fetch all models with their references for DAG ordering
        all_models = session.fetch_all_models(fetch_all=True)

        # Build list with model names and their references
        models_with_refs: List[Dict[str, Any]] = []
        for model in all_models:
            references = model.model_data.get("reference", []) or []
            models_with_refs.append({
                "model_name": model.model_name,
                "references": references
            })

        # Sort models by execution order (DAG order)
        sorted_model_names = topological_sort_models(models_with_refs)

        # Build a lookup from model name -> model object for status fields
        model_lookup = {m.model_name: m for m in all_models}

        # Build the model structure in sorted order
        no_code_model_structure = []
        for no_code_model_name in sorted_model_names:
            model = model_lookup.get(no_code_model_name)
            model_data = {
                "extension": no_code_model_name,
                "title": no_code_model_name,
                "key": f"{self.project_name}/models/no_code/{no_code_model_name}",
                "is_folder": False,
                "type": "NO_CODE_MODEL",
                "run_status": getattr(model, "run_status", None),
                "failure_reason": getattr(model, "failure_reason", None),
                "last_run_at": model.last_run_at.isoformat() if getattr(model, "last_run_at", None) else None,
                "run_duration": getattr(model, "run_duration", None),
            }
            no_code_model_structure.append(model_data)
        model_structure: dict[str, Any] = {
            "title": "models",
            "key": f"{self.project_name}/models",
            "is_folder": True,
            "type": "ROOT_MODEL",
            "children": [
                {
                    "title": "no_code",
                    "key": f"{self.project_name}/models/no_code",
                    "is_folder": True,
                    "type": "NO_CODE",
                    "children": no_code_model_structure,
                }
            ],
        }
        return model_structure

    def load_csv(self, session: Session):
        csv_models: List[CSVModels] = session.fetch_all_csv_files()
        seed_file_structure = []
        for csv_model in csv_models:
            seed_file_structure.append(
                {
                    "extension": "csv",
                    "title": csv_model.csv_name,
                    "key": f"{self.project_name}/seeds/{csv_model.csv_name}",
                    "is_folder": False,
                    "type": "SEED_CSV_FILE",
                    "status": csv_model.status,
                    "seed_table_exists": csv_model.table_exists,
                    "table_name": csv_model.table_name if csv_model and csv_model.table_name else "",
                    "table_schema": csv_model.table_schema if csv_model and csv_model.table_schema else "",
                }
            )
        seed_structure: dict[str, Any] = {
            "title": "seeds",
            "key": f"{self.project_name}/seeds",
            "is_folder": True,
            "type": "ROOT_SEED",
            "children": seed_file_structure,
        }
        return seed_structure

    def load_children_structure(self, session) -> List[Dict[str, Any]]:
        return [self.load_models(session), self.load_csv(session)]

    def get_project_file_structure(self, session: Session) -> dict[str, Any]:
        project_structure = {
            "title": self.project_name,
            "key": self.project_name,
            "is_folder": True,
            "type": "ROOT",
            "children": self.load_children_structure(session),
        }
        return project_structure
