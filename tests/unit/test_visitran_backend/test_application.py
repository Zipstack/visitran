import pytest
from pytest_mock.plugin import MockerFixture

from backend.application.application import ApplicationContext


@pytest.mark.unit
@pytest.mark.minimal_core
class TestApplication:
    def __init__(self):
        super().__init__()

    def test_schemas(self, mocker: MockerFixture):
        mock_context = mocker.MagicMock()

        app_context = ApplicationContext(mock_context)
        schemas = app_context.get_all_schemas()
        assert schemas

    def test_tables(self, mocker: MockerFixture):
        mock_context = mocker.MagicMock()

        app_context = ApplicationContext(mock_context)
        tables = app_context.get_all_tables(schema_name="default")
        assert tables

    def test_get_table_columns(self, mocker: MockerFixture):
        mock_context = mocker.MagicMock()

        app_context = ApplicationContext(mock_context)
        cols = app_context.get_table_columns(schema_name="default", table_name="test")
        assert cols
