"""Unit tests for ModelGraph lineage functionality."""

import uuid
from unittest.mock import MagicMock, patch, PropertyMock

import pytest


class MockModel:
    """Mock model for testing."""

    def __init__(self, name: str, data: dict):
        self.model_name = name
        self.model_data = data


class TestModelGraphLineage:
    """Tests for ModelGraph.get_lineage_relation()."""

    @pytest.fixture
    def mock_models(self):
        """Create mock models with various relationships.

        Graph structure:
        - raw.raw_customers (source table)
        - raw.raw_orders (join table)
        - model_a: reads from raw_customers, joins with raw_orders
        - model_b: references model_a
        - model_c: reads from raw_customers, unions with model_a
        """
        return [
            MockModel(
                "model_a",
                {
                    "source": {"schema_name": "raw", "table_name": "raw_customers"},
                    "transform": {
                        "step_1": {
                            "type": "join",
                            "join": {
                                "tables": [
                                    {
                                        "joined_table": {
                                            "schema_name": "raw",
                                            "table_name": "raw_orders",
                                        }
                                    }
                                ]
                            },
                        }
                    },
                },
            ),
            MockModel(
                "model_b",
                {
                    "source": {"schema_name": "staging", "table_name": "model_a"},
                    "reference": ["model_a"],
                },
            ),
            MockModel(
                "model_c",
                {
                    "source": {"schema_name": "raw", "table_name": "raw_customers"},
                    "transform": {
                        "step_1": {
                            "type": "union",
                            "union": {"branches": [{"schema": "staging", "table": "model_a"}]},
                        }
                    },
                },
            ),
        ]

    @pytest.fixture
    def mock_session(self, mock_models):
        """Create a mock session."""
        session = MagicMock()
        session.fetch_all_models.return_value = mock_models
        session.fetch_model_data.return_value = {}
        return session

    def test_lineage_creates_source_nodes(self, mock_session, mock_models):
        """Test that source tables are created as input nodes."""
        from backend.application.context.model_graph import ModelGraph

        with patch.object(ModelGraph, "__init__", lambda self, **kwargs: None):
            mg = ModelGraph()
            mg._session = mock_session

            result = mg.get_lineage_relation()

            # Check nodes were created
            node_labels = [n["data"]["label"] for n in result["nodes"]]

            # Models should exist
            assert "model_a" in node_labels
            assert "model_b" in node_labels
            assert "model_c" in node_labels

            # Source tables should exist as input nodes
            assert "raw.raw_customers" in node_labels
            assert "raw.raw_orders" in node_labels

    def test_lineage_creates_edges_from_source(self, mock_session, mock_models):
        """Test that edges are created from source tables to models."""
        from backend.application.context.model_graph import ModelGraph

        with patch.object(ModelGraph, "__init__", lambda self, **kwargs: None):
            mg = ModelGraph()
            mg._session = mock_session

            result = mg.get_lineage_relation()

            # Build id to label map
            id_to_label = {str(n["id"]): n["data"]["label"] for n in result["nodes"]}

            # Convert edges to readable format
            edges = []
            for e in result["edges"]:
                src = id_to_label.get(str(e["source"]))
                tgt = id_to_label.get(str(e["target"]))
                edges.append((src, tgt))

            # Check source -> model edges
            assert ("raw.raw_customers", "model_a") in edges
            assert ("raw.raw_customers", "model_c") in edges

    def test_lineage_creates_edges_from_joins(self, mock_session, mock_models):
        """Test that edges are created from JOIN tables."""
        from backend.application.context.model_graph import ModelGraph

        with patch.object(ModelGraph, "__init__", lambda self, **kwargs: None):
            mg = ModelGraph()
            mg._session = mock_session

            result = mg.get_lineage_relation()

            # Build id to label map
            id_to_label = {str(n["id"]): n["data"]["label"] for n in result["nodes"]}

            # Convert edges to readable format
            edges = []
            for e in result["edges"]:
                src = id_to_label.get(str(e["source"]))
                tgt = id_to_label.get(str(e["target"]))
                edges.append((src, tgt))

            # Check join table -> model edge
            assert ("raw.raw_orders", "model_a") in edges

    def test_lineage_creates_edges_from_unions(self, mock_session, mock_models):
        """Test that edges are created from UNION branches."""
        from backend.application.context.model_graph import ModelGraph

        with patch.object(ModelGraph, "__init__", lambda self, **kwargs: None):
            mg = ModelGraph()
            mg._session = mock_session

            result = mg.get_lineage_relation()

            # Build id to label map
            id_to_label = {str(n["id"]): n["data"]["label"] for n in result["nodes"]}

            # Convert edges to readable format
            edges = []
            for e in result["edges"]:
                src = id_to_label.get(str(e["source"]))
                tgt = id_to_label.get(str(e["target"]))
                edges.append((src, tgt))

            # Check union branch -> model edge
            assert ("staging.model_a", "model_c") in edges

    def test_lineage_creates_edges_from_references(self, mock_session, mock_models):
        """Test that edges are created from explicit references."""
        from backend.application.context.model_graph import ModelGraph

        with patch.object(ModelGraph, "__init__", lambda self, **kwargs: None):
            mg = ModelGraph()
            mg._session = mock_session

            result = mg.get_lineage_relation()

            # Build id to label map
            id_to_label = {str(n["id"]): n["data"]["label"] for n in result["nodes"]}

            # Convert edges to readable format
            edges = []
            for e in result["edges"]:
                src = id_to_label.get(str(e["source"]))
                tgt = id_to_label.get(str(e["target"]))
                edges.append((src, tgt))

            # Check reference -> model edge
            assert ("model_a", "model_b") in edges

    def test_lineage_handles_missing_references_gracefully(self, mock_session):
        """Test that missing references don't cause errors."""
        from backend.application.context.model_graph import ModelGraph

        # Model with reference to non-existent model
        mock_session.fetch_all_models.return_value = [
            MockModel(
                "model_x",
                {
                    "source": {"schema_name": "raw", "table_name": "table_x"},
                    "reference": ["non_existent_model"],
                },
            )
        ]

        with patch.object(ModelGraph, "__init__", lambda self, **kwargs: None):
            mg = ModelGraph()
            mg._session = mock_session

            # Should not raise an error
            result = mg.get_lineage_relation()

            # Should still have nodes
            assert len(result["nodes"]) > 0

    def test_lineage_marks_input_nodes(self, mock_session, mock_models):
        """Test that source tables are marked as input nodes."""
        from backend.application.context.model_graph import ModelGraph

        with patch.object(ModelGraph, "__init__", lambda self, **kwargs: None):
            mg = ModelGraph()
            mg._session = mock_session

            result = mg.get_lineage_relation()

            # Find source table nodes
            for node in result["nodes"]:
                if node["data"]["label"] in ["raw.raw_customers", "raw.raw_orders"]:
                    assert node.get("type") == "input", f"{node['data']['label']} should be input type"

    def test_lineage_marks_output_nodes(self, mock_session):
        """Test that leaf models (not referenced by others) are marked as output."""
        from backend.application.context.model_graph import ModelGraph

        # Simple chain: source -> model_a -> model_b (leaf)
        mock_session.fetch_all_models.return_value = [
            MockModel(
                "model_a",
                {"source": {"schema_name": "raw", "table_name": "source_table"}},
            ),
            MockModel(
                "model_b",
                {
                    "source": {"schema_name": "raw", "table_name": "model_a"},
                    "reference": ["model_a"],
                },
            ),
        ]

        with patch.object(ModelGraph, "__init__", lambda self, **kwargs: None):
            mg = ModelGraph()
            mg._session = mock_session

            result = mg.get_lineage_relation()

            # Find model_b node (leaf - not referenced by anyone)
            for node in result["nodes"]:
                if node["data"]["label"] == "model_b":
                    assert node.get("type") == "output", "model_b should be output type"
