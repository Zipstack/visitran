from typing import Any

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.transformation_parsers.combine_parser import CombineColumnParser
from backend.application.config_parser.transformation_parsers.distinct_parser import DistinctParser
from backend.application.config_parser.transformation_parsers.filter_parser import FilterParser
from backend.application.config_parser.transformation_parsers.find_and_replace_parser import FindAndReplaceParser
from backend.application.config_parser.transformation_parsers.groups_and_aggregation_parser import (
    GroupsAndAggregationParser
)
from backend.application.config_parser.transformation_parsers.join_parser import JoinParsers
from backend.application.config_parser.transformation_parsers.pivot_parser import PivotParser
from backend.application.config_parser.transformation_parsers.rename_parser import RenameParsers
from backend.application.config_parser.transformation_parsers.synthesize_parser import SynthesizeParser
from backend.application.config_parser.transformation_parsers.union_parser import UnionParsers
from backend.application.config_parser.transformation_parsers.window_parser import WindowParser


class TransformationParser(BaseParser):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._transforms_dict: dict[str, BaseParser] = {}
        self._transforms: list[BaseParser] = []

    @staticmethod
    def _transform_type_mapper(transform_type: str) -> type[BaseParser]:
        transform_type_mapper = {
            "join": JoinParsers,
            "union": UnionParsers,
            "pivot": PivotParser,
            "filter": FilterParser,
            "rename_column": RenameParsers,
            "combine_columns": CombineColumnParser,
            "synthesize": SynthesizeParser,
            "groups_and_aggregation": GroupsAndAggregationParser,
            "find_and_replace": FindAndReplaceParser,
            "distinct": DistinctParser,
            "window": WindowParser,
        }
        return transform_type_mapper[transform_type]

    def _create_transform_parser(
            self,
            transform_id: str,
            transform_payload: dict[str, Any]
    ) -> BaseParser:
        config_data = transform_payload.get(transform_id)
        if not config_data:
            raise ValueError(f"Transformation with ID {transform_id} not found in the transformation configuration.")

        transform_type = config_data.get("type")
        parser_class = self._transform_type_mapper(transform_type)
        transform_data = config_data[transform_type]
        transform_data["transformation_type"] = transform_type
        transform_data["transformation_id"] = transform_id
        return parser_class(
            config_data=transform_data
        )

    @property
    def transform_orders(self) -> list[str]:
        """Returns list of transformation ID 's"""
        return self.get("transform_order", [])

    def get_transforms(self) -> list[BaseParser]:
        """
        Generate and yield transformation parsers in the order defined by the configuration.
    
        This method processes the `transform_order` list and corresponding `transform` dictionary 
        from the configuration to create parser instances of appropriate types for each transformation.
    
        - It iterates through the `transform_order` to ensure transformations are applied sequentially.
        - For each transformation, it determines the type and maps it to the corresponding parser class.
        - Certain transformation types (`combine_columns`, `group`, `find_and_replace`, and `distinct`) 
          require special handling for their configuration. These are instantiated with a modified 
          configuration structure.
        - Other transformations are instantiated normally with their respective configuration data.
    
        Yields:
            BaseParser: An instance of the transformation parser for each transformation in the order 
            defined by `transform_order`.
        """
        if self._transforms:
            return self._transforms
        transforms = self.get("transform", {})
        for transform_id in self.transform_orders:
            if transform_id not in self._transforms_dict:
                transform_parser: BaseParser = self._create_transform_parser(
                    transform_id=transform_id,
                    transform_payload=transforms
                )
                self._transforms_dict[transform_id] = transform_parser
                self._transforms.append(transform_parser)
        return self._transforms

    def get_transformation_parser(self, transformation_id: str) -> BaseParser | None:
        transform_payload: dict[str, Any] = self.get("transform", {})
        if transformation_id in self._transforms_dict.keys():
            return self._transforms_dict[transformation_id]
        if transformation_id not in transform_payload:
            return None
        self.get_transforms()
        return self._transforms_dict[transformation_id]
