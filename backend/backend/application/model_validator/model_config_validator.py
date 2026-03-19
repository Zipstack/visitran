from backend.application.model_validator.transformations.base_validator import Validator
from backend.errors import SourceTableDoesNotExist, DestinationTableAlreadyExist, ReferenceNotFound
from backend.errors.visitran_backend_base_exceptions import VisitranBackendBaseException


class ModelConfigValidator(Validator):

    def validate_source_config(self) -> None | tuple[str, str]:
        """
        Validates the source configuration by checking the existence of the
        specified source table in the database. If the table does not exist,
        raises an appropriate exception.

        :raises SourceTableDoesNotExist: If the source table specified in the
            configuration does not exist in the database and not in any
            destination configuration in all models
        """
        src_table_name = self.current_parser.source_table_name
        src_schema_name = self.current_parser.source_schema_name
        if not self._visitran_context.db_adapter.db_connection.is_table_exists(
                table_name=src_table_name,
                schema_name=src_schema_name,
        ):
            table_not_exist_flag: bool = True
            for parser in self.all_parsers:
                if (
                        parser.destination_schema_name == src_schema_name
                        and parser.destination_table_name == src_table_name
                ):
                    table_not_exist_flag = False
            if table_not_exist_flag:
                raise SourceTableDoesNotExist(
                    schema_name=self.current_parser.source_schema_name,
                    table_name=self.current_parser.source_table_name,
                    model_name=self.current_parser.model_name
                )

        if self.old_parser:
            try:
                old_dest_schema = self.old_parser.destination_schema_name
                old_dest_table = self.old_parser.destination_table_name
                if src_table_name != old_dest_table or src_schema_name != old_dest_schema:
                    return old_dest_schema, old_dest_table
            except VisitranBackendBaseException:
                pass

        return None

    def validate_destination_config(self) -> None | tuple[str, str]:
        """
        Validates the destination table configuration for all parsers to ensure that no conflicting
        destination schema or table names exist.

        :raises DestinationTableAlreadyExist:
            If there is a conflict between destination schema or table names in the
            configuration.
        """
        new_dest_schema = self.current_parser.destination_schema_name
        new_dest_table = self.current_parser.destination_table_name
        for parser in self.all_parsers:
            if parser.destination_schema_name == new_dest_schema and parser.destination_table_name == new_dest_table:
                raise DestinationTableAlreadyExist(
                    schema_name=parser.destination_schema_name,
                    table_name=parser.destination_table_name,
                    current_model_name=self.current_parser.model_name,
                    conflicting_model_name=parser.model_name
                )

        if self.old_parser:
            try:
                old_dest_schema = self.old_parser.destination_schema_name
                old_dest_table = self.old_parser.destination_table_name
                if new_dest_schema != old_dest_schema or new_dest_table != old_dest_table:
                    return old_dest_schema, old_dest_table
                return old_dest_schema, old_dest_table
            except VisitranBackendBaseException:
                pass

        return None

    def validate_reference_config(self) -> None:
        reference_list: list[str] = self.current_parser.reference
        all_models = self._session.fetch_all_models(fetch_all=True)
        model_names = [model.model_name for model in all_models]
        if not all(ref in model_names for ref in reference_list):
            raise ReferenceNotFound(
                missing_references=[ref for ref in reference_list if ref not in model_names],
            )
