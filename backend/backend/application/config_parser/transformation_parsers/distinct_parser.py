from backend.application.config_parser.base_parser import BaseParser


class DistinctParser(BaseParser):
    @property
    def columns(self) -> list[str]:
        return self.get("columns", [])
