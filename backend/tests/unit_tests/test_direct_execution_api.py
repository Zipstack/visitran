"""
Unit tests for Direct Execution API endpoints.

Tests for:
- VTR-060: Model Execution API with Error Handling
- VTR-061: SQL Validation Results API
- VTR-062: DAG Execution API
- VTR-063: Model Registry Inspection Endpoints
- VTR-064: Error Handler Middleware
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from django.test import RequestFactory
from rest_framework import status

from backend.application.config_parser.feature_flags import (
    ExecutionMode,
    FeatureFlags,
)
from backend.application.config_parser.model_registry import (
    ModelRegistry,
    ModelMetadata,
    ExecutionStatus,
)
from backend.core.routers.direct_execution.views import (
    execute_model,
    list_validation_results,
    get_model_validation_results,
    execute_dag,
    get_dag_status,
    get_dag_results,
    list_registry_models,
    get_registry_dag,
    get_registry_model_detail,
)


@pytest.fixture
def request_factory():
    """Create a Django request factory."""
    return RequestFactory()


@pytest.fixture
def mock_registry():
    """Create a mock ModelRegistry."""
    with patch.object(ModelRegistry, "_instance", None):
        with patch.object(ModelRegistry, "_initialized", False):
            registry = ModelRegistry()
            yield registry
            ModelRegistry.reset_instance()


@pytest.fixture
def mock_feature_flags():
    """Reset feature flags between tests."""
    FeatureFlags.reset()
    yield FeatureFlags()
    FeatureFlags.reset()


# =============================================================================
# VTR-060: Model Execution API Tests
# =============================================================================


class TestExecuteModelAPI:
    """Tests for POST /api/v1/models/{schema}/{model}/execute"""

    def test_execute_model_feature_flag_disabled(
        self,
        request_factory,
        mock_feature_flags,
    ):
        """Test that 403 is returned when feature flag is disabled."""
        with FeatureFlags.override(enable_direct_execution=False):
            request = request_factory.post(
                "/api/v1/models/public/orders/execute",
                data={},
                content_type="application/json",
            )
            request.data = {}

            response = execute_model(request, schema="public", model="orders")

            assert response.status_code == status.HTTP_403_FORBIDDEN
            data = json.loads(response.content)
            assert data["error_code"] == "DIRECT_EXECUTION_DISABLED"

    def test_execute_model_with_override_flag(
        self,
        request_factory,
        mock_registry,
        mock_feature_flags,
    ):
        """Test that override_feature_flag bypasses the check."""
        with FeatureFlags.override(enable_direct_execution=False):
            request = request_factory.post(
                "/api/v1/models/public/orders/execute",
                data={"override_feature_flag": True},
                content_type="application/json",
            )
            request.data = {"override_feature_flag": True}

            response = execute_model(request, schema="public", model="orders")

            # Should get 400 (model not found) not 403 (disabled)
            assert response.status_code == status.HTTP_400_BAD_REQUEST
            data = json.loads(response.content)
            assert data["error_code"] == "MODEL_NOT_FOUND"

    def test_execute_model_not_found(
        self,
        request_factory,
        mock_registry,
        mock_feature_flags,
    ):
        """Test that 400 is returned for non-existent model."""
        with FeatureFlags.override(enable_direct_execution=True):
            request = request_factory.post(
                "/api/v1/models/public/nonexistent/execute",
                data={},
                content_type="application/json",
            )
            request.data = {}

            response = execute_model(request, schema="public", model="nonexistent")

            assert response.status_code == status.HTTP_400_BAD_REQUEST
            data = json.loads(response.content)
            assert data["error_code"] == "MODEL_NOT_FOUND"
            assert data["model"] == "public.nonexistent"
            assert "suggested_fix" in data

    def test_execute_model_success(
        self,
        request_factory,
        mock_registry,
        mock_feature_flags,
    ):
        """Test successful model execution."""
        # Create a mock config
        mock_config = MagicMock()
        mock_config.source_schema_name = "public"
        mock_config.source_table_name = "source_orders"
        mock_config.get_compiled_sql = MagicMock(return_value="SELECT * FROM orders")

        # Register the mock
        mock_registry.register(
            schema="public",
            model="orders",
            config=mock_config,
        )

        with FeatureFlags.override(enable_direct_execution=True):
            with patch(
                "backend.core.routers.direct_execution.views.IbisBuilder"
            ) as mock_builder:
                # Mock the IbisBuilder
                mock_result = MagicMock()
                mock_result.sql = "SELECT * FROM public.source_orders"
                mock_builder.return_value.compile_transformation.return_value = mock_result

                request = request_factory.post(
                    "/api/v1/models/public/orders/execute",
                    data={},
                    content_type="application/json",
                )
                request.data = {}

                response = execute_model(request, schema="public", model="orders")

                assert response.status_code == status.HTTP_200_OK
                data = json.loads(response.content)
                assert data["schema"] == "public"
                assert data["model"] == "orders"
                assert "execution_id" in data
                assert "generated_sql" in data
                assert "execution_time_ms" in data


# =============================================================================
# VTR-061: SQL Validation Results API Tests
# =============================================================================


class TestValidationResultsAPI:
    """Tests for GET /api/v1/validation/results"""

    @patch("backend.core.routers.direct_execution.views.SQLValidationResult")
    def test_list_validation_results_success(
        self,
        mock_model,
        request_factory,
    ):
        """Test successful listing of validation results."""
        # Create mock queryset
        mock_queryset = MagicMock()
        mock_queryset.count.return_value = 2
        mock_queryset.order_by.return_value.__getitem__.return_value = [
            MagicMock(
                model_name="public.orders",
                legacy_sql="SELECT 1",
                direct_sql="SELECT 1",
                match_status=True,
                diff_output="",
                validated_at=datetime.utcnow(),
                execution_id="test-exec-1",
            ),
            MagicMock(
                model_name="public.customers",
                legacy_sql="SELECT 2",
                direct_sql="SELECT 3",
                match_status=False,
                diff_output="- SELECT 2\n+ SELECT 3",
                validated_at=datetime.utcnow(),
                execution_id="test-exec-2",
            ),
        ]
        mock_model.objects.all.return_value = mock_queryset
        mock_queryset.filter.return_value = mock_queryset

        request = request_factory.get("/api/v1/validation/results")
        request.query_params = {}

        response = list_validation_results(request)

        assert response.status_code == status.HTTP_200_OK
        data = json.loads(response.content)
        assert "results" in data
        assert "pagination" in data
        assert data["pagination"]["total_count"] == 2

    def test_list_validation_results_invalid_pagination(self, request_factory):
        """Test that invalid pagination returns 400."""
        request = request_factory.get("/api/v1/validation/results?page=abc")
        request.query_params = {"page": "abc"}

        response = list_validation_results(request)

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        data = json.loads(response.content)
        assert data["error_code"] == "INVALID_PARAMS"

    def test_list_validation_results_status_filter(self, request_factory):
        """Test that invalid status filter returns 400."""
        request = request_factory.get("/api/v1/validation/results?status=invalid")
        request.query_params = {"status": "invalid"}

        response = list_validation_results(request)

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        data = json.loads(response.content)
        assert data["error_code"] == "INVALID_STATUS"


class TestModelValidationResultsAPI:
    """Tests for GET /api/v1/validation/results/{model_name}"""

    @patch("backend.core.routers.direct_execution.views.SQLValidationResult")
    def test_get_model_validation_not_found(
        self,
        mock_model,
        request_factory,
    ):
        """Test 404 when no results exist for model."""
        mock_queryset = MagicMock()
        mock_queryset.count.return_value = 0
        mock_model.objects.filter.return_value = mock_queryset

        request = request_factory.get("/api/v1/validation/results/nonexistent")
        request.query_params = {}

        response = get_model_validation_results(request, model_name="nonexistent")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        data = json.loads(response.content)
        assert data["error_code"] == "NOT_FOUND"


# =============================================================================
# VTR-062: DAG Execution API Tests
# =============================================================================


class TestDAGExecutionAPI:
    """Tests for POST /api/v1/dag/execute"""

    def test_execute_dag_invalid_mode(self, request_factory):
        """Test that invalid execution mode returns 400."""
        request = request_factory.post(
            "/api/v1/dag/execute",
            data={"execution_mode": "invalid"},
            content_type="application/json",
        )
        request.data = {"execution_mode": "invalid"}

        response = execute_dag(request)

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        data = json.loads(response.content)
        assert data["error_code"] == "INVALID_EXECUTION_MODE"

    @patch("backend.core.routers.direct_execution.views.DAGBuilder")
    def test_execute_dag_empty(self, mock_builder, request_factory):
        """Test execution with empty DAG."""
        mock_builder.return_value.dag = None

        request = request_factory.post(
            "/api/v1/dag/execute",
            data={"execution_mode": "direct"},
            content_type="application/json",
        )
        request.data = {"execution_mode": "direct"}

        response = execute_dag(request)

        assert response.status_code == status.HTTP_200_OK
        data = json.loads(response.content)
        assert data["total_models"] == 0
        assert data["message"] == "No models in DAG to execute"


class TestDAGStatusAPI:
    """Tests for GET /api/v1/dag/status/{execution_id}"""

    def test_get_status_not_found(self, request_factory):
        """Test 404 when execution ID not found."""
        request = request_factory.get(
            f"/api/v1/dag/status/{uuid.uuid4()}"
        )

        response = get_dag_status(request, execution_id=str(uuid.uuid4()))

        assert response.status_code == status.HTTP_404_NOT_FOUND
        data = json.loads(response.content)
        assert data["error_code"] == "NOT_FOUND"


class TestDAGResultsAPI:
    """Tests for GET /api/v1/dag/results/{execution_id}"""

    def test_get_results_not_found(self, request_factory):
        """Test 404 when execution ID not found."""
        request = request_factory.get(
            f"/api/v1/dag/results/{uuid.uuid4()}"
        )

        response = get_dag_results(request, execution_id=str(uuid.uuid4()))

        assert response.status_code == status.HTTP_404_NOT_FOUND


# =============================================================================
# VTR-063: Model Registry Inspection Tests
# =============================================================================


class TestRegistryModelsAPI:
    """Tests for GET /api/v1/registry/models"""

    def test_list_registry_models_empty(self, request_factory, mock_registry):
        """Test listing models when registry is empty."""
        request = request_factory.get("/api/v1/registry/models")

        response = list_registry_models(request)

        assert response.status_code == status.HTTP_200_OK
        data = json.loads(response.content)
        assert data["models"] == []
        assert data["total_count"] == 0

    def test_list_registry_models_with_models(self, request_factory, mock_registry):
        """Test listing models when registry has models."""
        mock_config = MagicMock()
        mock_registry.register(
            schema="public",
            model="orders",
            config=mock_config,
        )

        request = request_factory.get("/api/v1/registry/models")

        response = list_registry_models(request)

        assert response.status_code == status.HTTP_200_OK
        data = json.loads(response.content)
        assert data["total_count"] == 1
        assert len(data["models"]) == 1
        assert data["models"][0]["key"] == "public.orders"


class TestRegistryDAGAPI:
    """Tests for GET /api/v1/registry/dag/{schema}"""

    @patch("backend.core.routers.direct_execution.views.DAGBuilder")
    def test_get_dag_no_dag_available(self, mock_builder, request_factory):
        """Test 404 when no DAG has been built."""
        mock_builder.return_value.dag = None

        request = request_factory.get("/api/v1/registry/dag/public")

        response = get_registry_dag(request, schema="public")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        data = json.loads(response.content)
        assert data["error_code"] == "DAG_NOT_FOUND"


class TestRegistryModelDetailAPI:
    """Tests for GET /api/v1/registry/models/{schema}/{model}"""

    def test_get_model_detail_not_found(self, request_factory, mock_registry):
        """Test 404 when model not found."""
        request = request_factory.get("/api/v1/registry/models/public/nonexistent")

        response = get_registry_model_detail(
            request, schema="public", model="nonexistent"
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND
        data = json.loads(response.content)
        assert data["error_code"] == "NOT_FOUND"

    def test_get_model_detail_success(self, request_factory, mock_registry):
        """Test successful model detail retrieval."""
        mock_config = MagicMock()
        mock_config.destination_schema_name = "public"
        mock_config.destination_table_name = "orders"
        mock_config.source_schema_name = "raw"
        mock_config.source_table_name = "orders_raw"
        mock_config.materialization = "TABLE"
        mock_config.get_source_location = MagicMock(
            return_value={"file": "models/orders.yaml", "line": 10, "column": 1}
        )

        mock_registry.register(
            schema="public",
            model="orders",
            config=mock_config,
        )

        request = request_factory.get("/api/v1/registry/models/public/orders")

        response = get_registry_model_detail(request, schema="public", model="orders")

        assert response.status_code == status.HTTP_200_OK
        data = json.loads(response.content)
        assert data["schema"] == "public"
        assert data["model"] == "orders"
        assert data["qualified_name"] == "public.orders"
        assert "metadata" in data
        assert "config" in data


# =============================================================================
# VTR-064: Error Handler Middleware Tests
# =============================================================================


# =============================================================================
# Integration Tests
# =============================================================================


class TestAPIIntegration:
    """Integration tests for the complete API flow."""

    def test_model_execution_flow(
        self,
        request_factory,
        mock_registry,
        mock_feature_flags,
    ):
        """Test the complete model execution flow."""
        # 1. Register a model
        mock_config = MagicMock()
        mock_config.source_schema_name = "public"
        mock_config.source_table_name = "source"
        mock_config.destination_schema_name = "public"
        mock_config.destination_table_name = "orders"
        mock_config.materialization = "TABLE"
        mock_config.get_source_location = MagicMock(
            return_value={"file": "orders.yaml", "line": 1}
        )

        mock_registry.register(
            schema="public",
            model="orders",
            config=mock_config,
        )

        # 2. Verify model appears in registry list
        request = request_factory.get("/api/v1/registry/models")
        response = list_registry_models(request)
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["total_count"] == 1

        # 3. Get model details
        request = request_factory.get("/api/v1/registry/models/public/orders")
        response = get_registry_model_detail(request, schema="public", model="orders")
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["qualified_name"] == "public.orders"

    def test_feature_flag_override_flow(
        self,
        request_factory,
        mock_registry,
        mock_feature_flags,
    ):
        """Test that feature flag override works correctly."""
        mock_config = MagicMock()
        mock_registry.register(schema="public", model="test", config=mock_config)

        # With flag disabled and no override -> 403
        with FeatureFlags.override(enable_direct_execution=False):
            request = request_factory.post("/api/v1/models/public/test/execute")
            request.data = {}
            response = execute_model(request, schema="public", model="test")
            assert response.status_code == 403

        # With flag disabled but override=true -> proceeds (to next error)
        with FeatureFlags.override(enable_direct_execution=False):
            request = request_factory.post("/api/v1/models/public/test/execute")
            request.data = {"override_feature_flag": True}

            with patch(
                "backend.core.routers.direct_execution.views.IbisBuilder"
            ) as mock_builder:
                mock_result = MagicMock()
                mock_result.sql = "SELECT 1"
                mock_builder.return_value.compile_transformation.return_value = mock_result

                response = execute_model(request, schema="public", model="test")
                # Should proceed past the feature flag check
                assert response.status_code != 403
