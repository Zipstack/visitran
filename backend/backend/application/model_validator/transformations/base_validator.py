from typing import TypeVar

from backend.application.config_parser.base_parser import BaseParser
from backend.application.config_parser.config_parser import ConfigParser
from backend.application.session.session import Session
from backend.application.visitran_backend_context import VisitranBackendContext

ParserType = TypeVar('ParserType', bound=BaseParser)

class Validator:
    def __init__(
        self,
        session: Session,
        visitran_context: VisitranBackendContext,
        model_name: str = None,
        current_parser: ParserType=None,
        old_parser: ParserType=None,
        **kwargs
    ):
        """
        Initializes a new instance of the class.

        This constructor sets up the initial state of the object by initializing its
        attributes based on the provided parameters. It prepares parsers for configuration
        management, establishes a session for handling backend operations, and integrates
        the visitran context for the application runtime.
        """
        self._all_parsers: list[ConfigParser] = []
        self._session = session
        self._visitran_context = visitran_context
        self.model_name = model_name
        self._current_parser = current_parser
        self._old_parser = old_parser

    @property
    def session(self) -> Session:
        return self._session

    @property
    def visitran_context(self) -> VisitranBackendContext:
        return self._visitran_context

    @property
    def current_parser(self) -> ParserType:
        return self._current_parser

    @property
    def old_parser(self) -> ParserType | None:
        return self._old_parser

    @property
    def all_parsers(self) -> list[ConfigParser]:
        """
        Fetches and caches all parsers associated with models fetched from the session. If
        the parser corresponding to the current `model_name` exists, it is excluded.

        :return: A list of ConfigParser objects representing the parsers for the models
            excluding the parser for the current `model_name`.
        :rtype: list[ConfigParser]
        """
        if not self._all_parsers:
            all_models = self._session.fetch_all_models()
            for model in all_models:
                if model.model_name == self.model_name:
                    continue
                _parser = ConfigParser(model_data=model.model_data, file_name=model.model_name)
                self._all_parsers.append(_parser)
        return self._all_parsers

    def validate_new_transform(self) -> list[str]:
        raise NotImplementedError

    def validate_updated_transform(self) -> list[str]:
        raise NotImplementedError

    def validate_deleted_transform(self) -> list[str]:
        raise NotImplementedError

    def check_column_usage(self, columns: list[str]) -> list[str]:
        raise NotImplementedError

