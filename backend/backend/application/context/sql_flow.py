"""
SQL Flow Generator - Table-Level Lineage Visualization

Generates a clean lineage visualization showing table relationships
across all models. Shows data flow from source tables through
intermediate models to final outputs.
"""

import logging
from typing import Any, Dict, List, Set

from backend.application.context.base_context import BaseContext
from backend.core.models.dependent_models import DependentModels


class SQLFlowGenerator(BaseContext):
    """Generates table-level lineage graph from model definitions.

    Shows:
    - Source tables (raw database tables)
    - Model tables (intermediate transformations)
    - Final output tables (terminal models)
    - Data flow connections between tables
    """

    def __init__(self, project_id: str):
        super().__init__(project_id=project_id)
        self.nodes: dict[str, dict] = {}  # table_key -> node
        self.edges: list[dict] = []
        self.join_targets: set[str] = set()  # Tables used as JOIN targets or referenced
        self.schemas: set[str] = set()  # Track all schemas encountered
        # Track model name -> output table key mapping for reference resolution
        self.model_to_output: dict[str, str] = {}
        # Track model references for building inheritance edges
        self.model_references: dict[str, list[str]] = {}
        # Track model output -> source table key for column inheritance
        self.model_source_map: dict[str, str] = {}
        # Track table key -> compiled SQL for model outputs
        self.model_sql_map: dict[str, str] = {}

    def generate_flow(self) -> dict[str, Any]:
        """Main entry point. Returns nodes and edges for SQL Flow
        visualization.

        Returns:
            Dictionary containing:
            - nodes: List of table card nodes with columns
            - edges: List of edges connecting tables
            - stats: Summary statistics
        """
        models = self.session.fetch_all_models()

        # Pass 1: Collect all tables and relationships
        for model in models:
            model_data = model.model_data
            if not model_data:
                continue

            model_name = model.model_name

            # Add source table
            source = model_data.get("source", {})
            source_key = self._get_table_key(source)
            if source_key:
                self._add_node(source_key, source, is_source=True)

            # Add output table (model destination)
            dest = model_data.get("model", {})
            dest_key = self._get_table_key(dest)
            if dest_key:
                self._add_node(dest_key, dest, model_name=model_name)
                self.model_to_output[model_name] = dest_key
                if source_key:
                    self.model_source_map[dest_key] = source_key

                # Fetch compiled SQL for this model
                sql = self._fetch_model_sql(model)
                if sql:
                    self.model_sql_map[dest_key] = sql

            # Track model references for Pass 2
            references = model_data.get("reference", [])
            if references:
                self.model_references[model_name] = references

            # Process JOINs - add joined tables and edges
            self._process_joins(model_data, model_name, source_key, dest_key)

            # Process UNIONs
            self._process_unions(model_data, model_name)

            # Create edge from source table to output table
            if source_key and dest_key and source_key != dest_key:
                self._add_edge(source_key, dest_key, model_name, "source")

        # Pass 2: Process model references
        self._process_model_references()

        # Pass 3: Classify nodes, finalize columns, and add SQL
        self._classify_nodes()
        self._finalize_columns()
        self._add_sql_to_nodes()

        # Calculate stats
        tables_by_schema = {}
        for node in self.nodes.values():
            schema = node["data"]["schema"] or "default"
            tables_by_schema[schema] = tables_by_schema.get(schema, 0) + 1

        return {
            "nodes": list(self.nodes.values()),
            "edges": self.edges,
            "stats": {
                "totalTables": len(self.nodes),
                "totalConnections": len(self.edges),
                "sourceTablesCount": sum(1 for n in self.nodes.values() if n["data"]["tableType"] == "source"),
                "modelTablesCount": sum(1 for n in self.nodes.values() if n["data"]["tableType"] == "model"),
                "schemas": sorted(list(self.schemas)) if self.schemas else ["default"],
                "tablesBySchema": tables_by_schema,
            },
        }

    def _get_table_key(self, table_data: dict) -> str | None:
        """Generate a unique key for a table from schema.table_name."""
        schema = table_data.get("schema_name", "")
        table = table_data.get("table_name", "")
        if schema and table:
            return f"{schema}.{table}"
        elif table:
            return table
        return None

    def _add_node(
        self,
        key: str,
        table_data: dict,
        is_source: bool = False,
        model_name: str = None,
    ):
        """Add or update a table node."""
        schema = table_data.get("schema_name", "")
        if schema:
            self.schemas.add(schema)

        if key not in self.nodes:
            self.nodes[key] = {
                "id": key,
                "type": "tableCard",
                "data": {
                    "label": table_data.get("table_name", key.split(".")[-1]),
                    "schema": schema,
                    "columns": [],
                    "modelName": model_name,
                    "isSource": is_source,
                    "tableType": "source",  # Will be classified later
                },
                "position": {"x": 0, "y": 0},
            }
        else:
            # Update model info if provided
            if model_name and not self.nodes[key]["data"]["modelName"]:
                self.nodes[key]["data"]["modelName"] = model_name
                self.nodes[key]["data"]["isSource"] = False

    def _process_joins(self, model_data: dict, model_name: str, source_key: str, dest_key: str):
        """Extract JOIN relationships and create edges."""
        transform = model_data.get("transform", {})
        if not isinstance(transform, dict):
            return

        for step_name, step_data in transform.items():
            join_list = []

            # Format 1: Direct join list
            if step_name == "join" and isinstance(step_data, list):
                join_list = step_data
            # Format 2: Step with type: join
            elif isinstance(step_data, dict) and step_data.get("type") == "join":
                join_data = step_data.get("join", {})
                if isinstance(join_data, list):
                    join_list = join_data
                elif isinstance(join_data, dict):
                    join_list = join_data.get("tables", [])

            for join_item in join_list:
                if not isinstance(join_item, dict):
                    continue

                # Get joined table
                joined_table = join_item.get("destination", {}) or join_item.get("joined_table", {})
                if not joined_table:
                    continue

                joined_schema = joined_table.get("schema_name", "")
                joined_table_name = joined_table.get("table_name", "")

                # Infer schema from source if not specified
                if not joined_schema and source_key and "." in source_key:
                    joined_schema = source_key.split(".")[0]

                if not joined_table_name:
                    continue

                joined_key = f"{joined_schema}.{joined_table_name}" if joined_schema else joined_table_name

                # Add joined table as node
                self._add_node(
                    joined_key,
                    {"schema_name": joined_schema, "table_name": joined_table_name},
                    is_source=True,
                )

                # Track as join target
                self.join_targets.add(joined_key)

                # Create edge from joined table to output
                if dest_key and joined_key != dest_key:
                    self._add_edge(joined_key, dest_key, model_name, "join")

    def _process_unions(self, model_data: dict, model_name: str):
        """Extract UNION relationships and create edges."""
        transform = model_data.get("transform", {})
        if not isinstance(transform, dict):
            return

        dest = model_data.get("model", {})
        dest_key = self._get_table_key(dest)

        for step_name, step_data in transform.items():
            if not isinstance(step_data, dict) or step_data.get("type") != "union":
                continue

            union_data = step_data.get("union", {})
            if not isinstance(union_data, dict):
                continue

            # Branch-based format
            for branch in union_data.get("branches", []):
                if isinstance(branch, dict):
                    schema = branch.get("schema", "")
                    table = branch.get("table", "")
                    if table:
                        table_key = f"{schema}.{table}" if schema else table
                        self._add_node(table_key, {"schema_name": schema, "table_name": table}, is_source=True)
                        if dest_key:
                            self._add_edge(table_key, dest_key, model_name, "union")

            # Table-based format
            for table in union_data.get("tables", []):
                if isinstance(table, dict):
                    schema = table.get("merge_schema", model_data.get("source", {}).get("schema_name", ""))
                    table_name = table.get("merge_table")
                    if table_name:
                        table_key = f"{schema}.{table_name}" if schema else table_name
                        self._add_node(table_key, {"schema_name": schema, "table_name": table_name}, is_source=True)
                        if dest_key:
                            self._add_edge(table_key, dest_key, model_name, "union")

    def _add_edge(self, source_key: str, target_key: str, model_name: str, edge_type: str = "flow"):
        """Add a data flow edge between tables."""
        edge_id = f"{source_key}->{target_key}:{edge_type}"

        # Check for duplicates
        if any(e["id"] == edge_id for e in self.edges):
            return

        self.edges.append(
            {
                "id": edge_id,
                "source": source_key,
                "target": target_key,
                "sourceHandle": "default",
                "targetHandle": "default",
                "data": {
                    "edgeType": edge_type,
                    "modelName": model_name,
                },
            }
        )

    def _process_model_references(self):
        """Create edges for model references (when one model references
        another)."""
        for model_name, references in self.model_references.items():
            current_output = self.model_to_output.get(model_name)
            if not current_output:
                continue

            for ref_model_name in references:
                ref_output = self.model_to_output.get(ref_model_name)
                if ref_output and ref_output != current_output:
                    self.join_targets.add(ref_output)
                    self._add_edge(ref_output, current_output, model_name, "reference")

    def _classify_nodes(self):
        """Classify nodes as source, model, or terminal.

        - source: Raw database tables (purple border)
        - model: Intermediate models (blue border)
        - terminal: Final output tables (green border)
        """
        model_outputs = {key for key, node in self.nodes.items() if node["data"]["modelName"]}

        for key, node in self.nodes.items():
            if node["data"]["isSource"] and key not in model_outputs:
                node["data"]["tableType"] = "source"
            elif key in model_outputs and key not in self.join_targets:
                node["data"]["tableType"] = "terminal"
            else:
                node["data"]["tableType"] = "model"

    def _fetch_table_columns(self, schema_name: str, table_name: str) -> list[dict]:
        """Fetch columns for a table from the database."""
        try:
            columns = self.visitran_context.get_table_columns_with_type(schema_name=schema_name, table_name=table_name)
            return [
                {"name": col.get("column_name", ""), "type": col.get("column_dbtype", "")}
                for col in columns
                if col.get("column_name")
            ]
        except Exception as e:
            logging.warning(f"Failed to fetch columns for {schema_name}.{table_name}: {e}")
            return []

    def _get_columns_for_table(self, key: str, visited: set[str] = None) -> list[dict]:
        """Get columns for a table, with fallback to source table for model
        outputs."""
        if visited is None:
            visited = set()
        if key in visited:
            return []
        visited.add(key)

        node = self.nodes.get(key)
        if not node:
            return []

        schema = node["data"]["schema"]
        table_name = node["data"]["label"]

        # Try database first
        columns = self._fetch_table_columns(schema, table_name)
        if columns:
            return columns

        # Fallback to source table
        source_key = self.model_source_map.get(key)
        if source_key:
            return self._get_columns_for_table(source_key, visited)

        return []

    def _finalize_columns(self):
        """Fetch and set columns for all nodes."""
        for key, node in self.nodes.items():
            columns = self._get_columns_for_table(key)
            node["data"]["columns"] = [{"name": col["name"], "type": col.get("type", "")} for col in columns]

    def _fetch_model_sql(self, model) -> str | None:
        """Fetch compiled SQL for a model from DependentModels."""
        try:
            dependent_model = self.session.project_instance.dependent_model.get(model=model, transformation_id="sql")
            sql_data = dependent_model.model_data
            if isinstance(sql_data, dict):
                return sql_data.get("sql", "")
            return sql_data if isinstance(sql_data, str) else None
        except DependentModels.DoesNotExist:
            logging.debug(f"No SQL found for model {model.model_name}")
            return None
        except Exception as e:
            logging.warning(f"Failed to fetch SQL for model {model.model_name}: {e}")
            return None

    def _add_sql_to_nodes(self):
        """Add compiled SQL to model nodes."""
        for key, node in self.nodes.items():
            sql = self.model_sql_map.get(key)
            node["data"]["sql"] = sql  # Will be None for source tables
