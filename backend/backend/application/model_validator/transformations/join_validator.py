import logging

from backend.application.model_validator.transformations.base_validator import Validator

logger = logging.getLogger(__name__)


class JoinValidator(Validator):

    def _get_columns_for_removed_tables(self, removed_tables: set) -> list[str]:
        """Get columns from removed join tables by querying the database schema.

        Fallback when DependentModels runtime data is unavailable.
        """
        columns = []
        for schema_name, table_name in removed_tables:
            try:
                table_columns = self.visitran_context.get_table_columns_with_type(
                    schema_name=schema_name or "", table_name=table_name
                )
                columns.extend(col["column_name"] for col in table_columns)
            except Exception:
                logger.warning(
                    "Could not fetch columns for removed join table %s.%s",
                    schema_name, table_name,
                )
        return columns

    def validate_updated_transform(self) -> list[str]:
        # Determine which join tables were removed between old and new config
        old_join_parsers = self.old_parser.get_joins()
        new_join_parsers = self.current_parser.get_joins()

        old_join_tables = {
            (p.rhs_schema_name, p.rhs_table_name) for p in old_join_parsers
        }
        new_join_tables = {
            (p.rhs_schema_name, p.rhs_table_name) for p in new_join_parsers
        }

        removed_tables = old_join_tables - new_join_tables
        if not removed_tables:
            return []

        # Try runtime snapshot first (populated when model was executed)
        transform_id = self.old_parser.transform_id
        pre_join_data = self.session.get_model_dependency_data(
            model_name=self.model_name,
            transformation_id=f"{transform_id}",
            default={},
        )
        post_join_data = self.session.get_model_dependency_data(
            model_name=self.model_name,
            transformation_id=f"{transform_id}_transformed",
            default={},
        )

        pre_columns = set(pre_join_data.get("column_names", []))
        post_columns = set(post_join_data.get("column_names", []))

        join_added_columns = post_columns - pre_columns
        if join_added_columns:
            return list(join_added_columns)

        # Fallback: query the database for columns of the removed join tables
        return self._get_columns_for_removed_tables(removed_tables)

    def validate_deleted_transform(self):
        # Try runtime snapshot first
        old_columns_details = self.session.get_model_dependency_data(
            model_name=self.model_name,
            transformation_id=f"{self.old_parser.transform_id}",
            default={}
        )
        old_columns = old_columns_details.get("column_names") or []
        new_columns_details = self.session.get_model_dependency_data(
            model_name=self.model_name,
            transformation_id=f"{self.old_parser.transform_id}_transformed",
            default={}
        )
        new_columns = new_columns_details.get("column_names") or []
        runtime_added = [column for column in new_columns if column not in old_columns]
        if runtime_added:
            return runtime_added

        # Fallback: query database for all join table columns
        removed_tables = {
            (p.rhs_schema_name, p.rhs_table_name)
            for p in self.old_parser.get_joins()
        }
        return self._get_columns_for_removed_tables(removed_tables)

    def check_column_usage(self, columns: list[str]) -> list[str]:
        # Collect all columns referenced by this join: join keys + filter criteria
        used_columns = set(self.current_parser.join_columns)
        for join_parser in self.current_parser.get_joins():
            # Also include RHS join key column in case it matches
            if join_parser.rhs_column_name:
                used_columns.add(join_parser.rhs_column_name)
            # Include columns used in join filter criteria
            for col_name in join_parser.join_filter.column_names:
                used_columns.add(col_name)
        return [column for column in columns if column in used_columns]
