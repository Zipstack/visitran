from backend.application.config_parser.constants import AGGREGATE_DICT
from backend.application.interpreter.constants import ConditionTypes, Operators
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class HavingTransformation(BaseTransformation):
    def transform(self) -> str:
        if not self.config_parser.transform_parser.aggregate:
            return ""
        having_parser = self.config_parser.transform_parser.aggregate.having
        having_string: str = ""
        if having_parser and having_parser.conditions:
            conditions = having_parser.conditions
            for count, condition in enumerate(conditions):
                source_pointer = "_"
                lhs_name = condition.lhs_column.column_name
                function_name = AGGREGATE_DICT.get(condition.lhs_column.function)
                having_string += f"( {source_pointer}.{lhs_name}.{function_name}()"
                lhs_type = condition.lhs_column.data_type
                rhs_value = condition.rhs_value
                rhs_value = rhs_value[0] if rhs_value.__len__() >= 1 else ""
                if lhs_type == "String":
                    rhs_value = f"'{rhs_value}'"
                operation_type = Operators.get_operator_type(condition.operator, value=rhs_value)
                having_string += operation_type

                if count < conditions.__len__() - 1:
                    having_string += f" ) {ConditionTypes.get_condition_type(conditions[count+1].condition_type)} "
            having_string = f".having({having_string}))"
        return having_string
