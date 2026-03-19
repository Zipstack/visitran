from backend.application.interpreter.transformations.base_transformation import BaseTransformation
from backend.application.utils import get_class_name


class ReferenceTransformation(BaseTransformation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parent_class = "VisitranModel"

    @property
    def parent_class(self) -> str:
        return self._parent_class

    def _parse_references(self) -> str:
        """
        Parse reference models and determine the parent class for inheritance.

        CRITICAL: The parent class is determined by `source_model`, NOT by the first
        reference in the list. This is important because:

        1. `source_model` explicitly tracks which model produces the source table
        2. If source is a raw DB table (source_model=None), parent = VisitranModel
        3. JOIN/UNION models are imported but should NOT be the parent class

        This prevents the bug where a JOIN model becomes the parent class when
        the source table is actually from a different model or raw DB table.

        All referenced models are imported for use in transformations (JOIN, UNION, etc.),
        but only the source_model is used for class inheritance.

        NOTE: source_model might be removed from reference list by MRO pruning
        (detect_and_fix_mro_issues) if it's implied by another reference's transitive
        dependencies. We MUST still import it since it's the parent class.
        """
        self._parent_class = "VisitranModel"
        imported_models = set()

        # Import all referenced models (needed for JOINs, UNIONs, etc.)
        if self.config_parser.reference:
            for model in self.config_parser.reference:
                class_name = get_class_name(model)
                model_name = model.replace(" ", "_")
                self.add_headers(f"from {self.visitran_context.project_py_name}.models.{model_name} import {class_name}")
                imported_models.add(model)

        # Determine parent class based on source_model (not first reference)
        # source_model is set by validate_table_usage_references() when
        # the source table matches another model's destination
        source_model = self.config_parser.source_model
        if source_model:
            # Source table is produced by another model - use it as parent
            self._parent_class = get_class_name(source_model)

            # CRITICAL: source_model might have been pruned from reference list
            # by MRO optimization, but we MUST import it since it's the parent class
            if source_model not in imported_models:
                model_name = source_model.replace(" ", "_")
                self.add_headers(f"from {self.visitran_context.project_py_name}.models.{model_name} import {self._parent_class}")
        # else: source_model is None, meaning source is a raw DB table
        # Keep parent as VisitranModel (read from database via source_table_obj)

        return self._parent_class

    def transform(self) -> str:
        return self._parse_references()
