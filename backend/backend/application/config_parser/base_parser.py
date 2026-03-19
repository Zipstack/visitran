from typing import Any


class BaseParser:
    def __init__(self, config_data: dict[str, Any]):
        self._config_data = config_data
        self._transform_id: str = self._config_data.get("transformation_id")
        self._transform_type: str = self._config_data.get("transformation_type")

    def get(self, key: str, default: Any = None) -> Any:
        return self._config_data.get(key, default)

    def __getitem__(self, item):
        return self._config_data[item]

    @property
    def transform_type(self) -> str:
        return self._transform_type

    @property
    def transform_id(self) -> str:
        return self._transform_id
