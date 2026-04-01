from collections import defaultdict
from functools import lru_cache
from typing import Any, Dict, Set


class ValidateReferences:
    def __init__(self, model_dict: dict[str, set[str]], model_name: str):
        self.model_dict = model_dict
        self.model_name = model_name
        self.children = defaultdict(set)
        self.indirect_relations = set()
        self.current_references = self.model_dict.get(self.model_name, [])
        self.all_ancestors = set()
        self.all_descendants = set()
        self._analysed = False

    # Get all ancestors i.e., children of a model recursively
    def get_ancestors(self, model) -> set:
        ancestors = set()
        stack = [model]
        while stack:
            curr = stack.pop()
            if curr in self.children:
                for parent in self.children[curr]:
                    if parent not in ancestors:
                        ancestors.add(parent)
                        stack.append(parent)
        return ancestors

    # Get all descendants i.e., parents of a model recursively
    def get_descendants(self, model) -> set:
        descendants = set()
        stack = [model]
        while stack:
            curr = stack.pop()
            if curr in self.model_dict:
                for child in self.model_dict[curr]:
                    if child not in descendants:
                        descendants.add(child)
                        stack.append(child)
        return descendants

    def _analyse_models(self):
        """Given a list of models and a list of model names, return the names
        of models that are valid references for the given models.

        A model is valid if it is not in the input list, is not already
        in the reference list of the given models, and is not an
        ancestor or descendant of the given models (to prevent circular
        reference).
        :return: A list of model names
        """

        # Check if model_name exists
        if self.model_name not in self.model_dict:
            raise ValueError(f"Model '{self.model_name}' does not exist in the model list.")

        # Build parent-child relationships
        self.children = defaultdict(set)
        for parent, refs in self.model_dict.items():
            for ref in refs:
                self.children[ref].add(parent)

        # Get all ancestors and descendants of the model, including indirect relations
        self.all_ancestors = self.get_ancestors(self.model_name)
        self.all_descendants = self.get_descendants(self.model_name)

        ancestors_relations = set()
        for ref in self.all_ancestors:
            ancestors_relations.update(self.get_ancestors(ref))
            ancestors_relations.update(self.get_descendants(ref))
            self.indirect_relations.update(ancestors_relations)
            for ancestor_ref in ancestors_relations:
                self.indirect_relations.update(self.get_ancestors(ancestor_ref))
                self.indirect_relations.update(self.get_descendants(ancestor_ref))

        # Filter out models that have any ancestor or descendant relationship to already referenced models
        for ref in self.current_references:
            self.indirect_relations.update(self.get_ancestors(ref))
            self.indirect_relations.update(self.get_descendants(ref))

        # Marking the flag to true to stop iteration on frequent calls.
        self._analysed = True

    def get_valid_references(self) -> list[str]:

        # Analysing the models references if the analysis is not already done.
        if not self._analysed:
            self._analyse_models()

        # Valid models are those that are not already in the reference list,
        # are not the input model, are not an ancestor or descendant, and have no indirect relation
        valid_models = [
            candidate
            for candidate in self.model_dict
            if candidate != self.model_name
            and candidate not in self.current_references
            and candidate not in self.all_ancestors
            and candidate not in self.all_descendants
            and candidate not in self.indirect_relations
        ]

        return valid_models

    def get_child_references(self):
        # Analysing the models references if the analysis is not already done.
        if not self._analysed:
            self._analyse_models()

        return self.all_ancestors

    def get_parent_references(self):
        try:
            # Analysing the models references if the analysis is not already done.
            if not self._analysed:
                self._analyse_models()

            return self.all_descendants
        except ValueError:
            return []

    def get_invalid_references(self) -> list[str]:
        # Get valid models
        valid_models = self.get_valid_references()

        # All models in the list except valid ones are invalid
        all_models = set(self.model_dict.keys())
        invalid_models = list(all_models - set(valid_models))

        return invalid_models

    def validate_table_usage_references(self, new_model_data: dict[str, Any], session):
        """Validate and update references based on table usage.

        CRITICAL: The source table's model (if any) MUST be the FIRST reference
        because the first reference becomes the parent class in the generated code.

        This method:
        1. Finds which model produces the source table (source_model)
        2. Finds which models produce JOIN/UNION tables
        3. Ensures source_model is FIRST in reference list (becomes parent class)
        4. Adds JOIN/UNION models after source_model

        Corner cases handled:
        - Source is raw DB table (no model): parent = VisitranModel, JOIN models imported
        - Source matches model A, JOIN matches model B: parent = A, both imported
        - Self-join (source and JOIN same table): parent = A, single import
        - Multiple JOINs: parent = source model, all JOIN models imported
        """
        new_source_schema = new_model_data["source"]["schema_name"]
        new_source_table = new_model_data["source"]["table_name"]

        # Extract tables from JOINs and UNIONs
        join_union_tables = self._extract_join_tables(new_model_data)
        join_union_tables.extend(self._extract_union_tables(new_model_data))

        # Build a map of (schema, table) -> model_name for all models
        table_to_model: dict[tuple, str] = {}
        for model_name in self.model_dict.keys():
            if model_name == self.model_name:
                continue
            try:
                model_data = session.fetch_model(model_name).model_data
                model_schema = model_data["model"]["schema_name"]
                model_table = model_data["model"]["table_name"]
                table_to_model[(model_schema, model_table)] = model_name
            except Exception:
                continue

        # Find which model produces our SOURCE table (will be parent class)
        source_table_model = table_to_model.get((new_source_schema, new_source_table))

        # Set source_model in model_data for reference.py to use
        # This explicitly tracks which model should be the parent class
        if source_table_model:
            new_model_data["source_model"] = source_table_model
        else:
            # Source table is a raw DB table, not a model output
            # Parent class should be VisitranModel (handled by reference.py)
            new_model_data["source_model"] = None

        # Find which models produce our JOIN/UNION tables
        join_union_models = []
        for schema, table in join_union_tables:
            model = table_to_model.get((schema, table))
            # Don't add if it's the source model (avoid duplicate) or already added
            if model and model != source_table_model and model not in join_union_models:
                join_union_models.append(model)

        # Get current references (user may have manually added some)
        current_references = list(new_model_data.get("reference", []))

        # Build the new reference list with correct ordering:
        # 1. Source table's model FIRST (becomes parent class)
        # 2. Existing references (preserving user's order, excluding source if already there)
        # 3. New JOIN/UNION models
        new_references = []

        # 1. Source model MUST be first (if it exists)
        if source_table_model:
            new_references.append(source_table_model)

        # 2. Add existing references (preserving order, avoiding duplicates)
        for ref in current_references:
            if ref not in new_references:
                new_references.append(ref)

        # 3. Add JOIN/UNION models (if not already present)
        for model in join_union_models:
            if model not in new_references:
                new_references.append(model)

        # Update the model data and internal state
        new_model_data["reference"] = new_references
        self.model_dict[self.model_name] = set(new_references)

    def _extract_join_tables(self, model_data: dict[str, Any]) -> list[tuple[str, str]]:
        """Extract all tables used in JOIN transformations."""
        tables = []
        transform = model_data.get("transform", {})
        if not isinstance(transform, dict):
            return tables
        for step_name, step_data in transform.items():
            if not isinstance(step_data, dict):
                continue
            if step_data.get("type") == "join":
                join_list = step_data.get("join", [])
                if not isinstance(join_list, list):
                    continue
                for join_item in join_list:
                    if not isinstance(join_item, dict):
                        continue
                    joined_table = join_item.get("joined_table", {})
                    if not isinstance(joined_table, dict):
                        continue
                    schema = joined_table.get("schema_name")
                    table = joined_table.get("table_name")
                    if schema and table:
                        tables.append((schema, table))
        return tables

    def _extract_union_tables(self, model_data: dict[str, Any]) -> list[tuple[str, str]]:
        """Extract all tables used in UNION transformations."""
        tables = []
        transform = model_data.get("transform", {})
        if not isinstance(transform, dict):
            return tables
        for step_name, step_data in transform.items():
            if not isinstance(step_data, dict):
                continue
            if step_data.get("type") == "union":
                union_data = step_data.get("union", {})
                if not isinstance(union_data, dict):
                    continue
                # Branch-based format (new)
                branches = union_data.get("branches", [])
                if isinstance(branches, list):
                    for branch in branches:
                        if isinstance(branch, dict):
                            schema = branch.get("schema")
                            table = branch.get("table")
                            if schema and table:
                                tables.append((schema, table))
                # Table-based format (legacy)
                union_tables = union_data.get("tables", [])
                if isinstance(union_tables, list):
                    for union_table in union_tables:
                        if isinstance(union_table, dict):
                            merge_table = union_table.get("merge_table")
                            merge_schema = union_table.get(
                                "merge_schema", model_data.get("source", {}).get("schema_name")
                            )
                            if merge_schema and merge_table:
                                tables.append((merge_schema, merge_table))
        return tables

    def detect_and_fix_mro_issues(self) -> dict[str, set[str]]:
        """Remove redundant transitive dependencies from self.model_dict.

        If a model A appears both directly in a model's base set and
        indirectly through another base B (i.e. B -> ... -> A), then A
        is removed from that model's base set.
        """

        @lru_cache(maxsize=None)
        def get_all_bases(cls_name: str) -> set[str]:
            """Return the full transitive closure of base models for
            cls_name."""
            bases = self.model_dict.get(cls_name, set())

            # Normalise in case someone stored a list/tuple
            if not isinstance(bases, (set, frozenset)):
                bases = set(bases)

            all_bases: set[str] = set(bases)
            for base in bases:
                all_bases |= get_all_bases(base)
            return all_bases

        # Work from a snapshot so we can mutate self.model_dict safely
        for name, bases in list(self.model_dict.items()):
            # Normalise to a list for deterministic processing
            direct_bases = list(bases)

            if len(direct_bases) <= 1:
                # Nothing to prune if 0 or 1 direct base
                continue

            pruned_bases: set[str] = set()
            for base in direct_bases:
                # Base is redundant if it is implied by any other base
                is_redundant = any(base in get_all_bases(other) for other in direct_bases if other != base)
                if not is_redundant:
                    pruned_bases.add(base)

            self.model_dict[name] = pruned_bases

        return self.model_dict
