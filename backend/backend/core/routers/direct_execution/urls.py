"""
URL patterns for Direct Execution API.

Provides routing for:
- VTR-060: Model Execution API - POST /api/v1/visitran/{org_id}/project/{project_id}/execute
- VTR-061: SQL Validation Results API - GET /api/v1/validation/results
- VTR-062: DAG Execution API - POST /api/v1/visitran/{org_id}/project/{project_id}/dag/execute
- VTR-063: Model Registry Inspection - GET /api/v1/registry/*

URL patterns are split into:
- urlpatterns: Root-level endpoints (feature-flags, validation, registry)
- project_urlpatterns: Project-scoped endpoints (execute, dag) - requires project_id
"""

from django.urls import path

from backend.core.routers.direct_execution.views import (
    # VTR-060: Model Execution
    execute_model,
    # VTR-062: DAG Execution
    execute_dag,
    get_dag_status,
    get_dag_results,
    # VTR-063: Registry Inspection
    list_registry_models,
    get_registry_dag,
    get_registry_model_detail,
)

# =============================================================================
# VTR-060: Model Execution API with Error Handling (Project-scoped)
# =============================================================================

EXECUTE_MODEL = path(
    "",  # Will be at /project/{project_id}/execute
    execute_model,
    name="execute-model",
)

# =============================================================================
# VTR-061: SQL Validation Results API
# =============================================================================

# =============================================================================
# VTR-062: DAG Execution API (Project-scoped)
# =============================================================================

EXECUTE_DAG = path(
    "dag",
    execute_dag,
    name="execute-dag",
)

GET_DAG_STATUS = path(
    "dag/status/<str:execution_id>",
    get_dag_status,
    name="get-dag-status",
)

GET_DAG_RESULTS = path(
    "dag/results/<str:execution_id>",
    get_dag_results,
    name="get-dag-results",
)

# =============================================================================
# VTR-063: Model Registry Inspection Endpoints
# =============================================================================

LIST_REGISTRY_MODELS = path(
    "registry/models",
    list_registry_models,
    name="list-registry-models",
)

GET_REGISTRY_DAG = path(
    "registry/dag/<str:schema>",
    get_registry_dag,
    name="get-registry-dag",
)

GET_REGISTRY_MODEL_DETAIL = path(
    "registry/models/<str:schema>/<str:model>",
    get_registry_model_detail,
    name="get-registry-model-detail",
)

# =============================================================================
# URL Patterns Export
# =============================================================================

urlpatterns = [
    # VTR-060: Model Execution
    EXECUTE_MODEL,
    # VTR-062: DAG Execution
    EXECUTE_DAG,
    GET_DAG_STATUS,
    GET_DAG_RESULTS,
    # VTR-063: Registry Inspection
    LIST_REGISTRY_MODELS,
    GET_REGISTRY_DAG,
    GET_REGISTRY_MODEL_DETAIL,
]
