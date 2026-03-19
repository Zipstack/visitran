from backend.application.interpreter.constants import ConditionTypes, Operators
from backend.application.interpreter.transformations.base_transformation import BaseTransformation


class AggregationFilterTransformation(BaseTransformation):
    def transform(self):
        filter_parser = self.config_parser.transform_parser.aggregate.filter
        filter_string = ""
        conditions = filter_parser.conditions

        for count, condition in enumerate(conditions):
            source_pointer = "_"
            lhs_name = condition.lhs_column.column_name

            filter_string += f"( {source_pointer}.{lhs_name}"
            lhs_type = condition.lhs_column.data_type
            rhs_value = condition.rhs_value
            rhs_value = rhs_value[0] if rhs_value.__len__() >= 1 else ""
            if lhs_type == "String":
                rhs_value = f"'{rhs_value}'"
            operation_type = Operators.get_operator_type(condition.operator, value=rhs_value)
            filter_string += operation_type

            if count < conditions.__len__() - 1:
                # Use the NEXT condition's type, as it specifies how it connects to current
                filter_string += f" ) {ConditionTypes.get_condition_type(conditions[count+1].condition_type)} "

        if filter_string:
            filter_string = f".filter({filter_string}))"

        return filter_string
