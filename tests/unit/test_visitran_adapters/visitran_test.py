import sys
from pathlib import Path
from unittest import mock

import pytest
from pytest_mock.plugin import MockerFixture

from visitran import ModelNotFoundError, NodeExecuteError
from visitran.events.types import ImportModelsFailed
from visitran.visitran import Visitran


def check_substring_in_list(substring: str, list_of_strings: list[str]) -> bool:
    return any(substring in s for s in list_of_strings)


@pytest.mark.unit
@pytest.mark.minimal_core
class TestVisitranClass:
    def test_module_exception(self, mocker: MockerFixture) -> None:
        mock_context = mocker.MagicMock()

        visitran_obj = Visitran(mock_context)
        context = visitran_obj.context
        context.get_model_files.return_value = ["not_valid"]  # type: ignore[attr-defined]

        with pytest.raises(ModelNotFoundError) as errinfo:
            visitran_obj.search_n_run_models()

        assert errinfo.value.args == ("not_valid",)

    def test_continue_test_module_exception(self, mocker: MockerFixture) -> None:
        mock_context = mocker.MagicMock()

        visitran_obj = Visitran(mock_context)
        context = visitran_obj.context
        context.get_test_files.return_value = ["not_valid"]  # type: ignore[attr-defined]

        import visitran

        spy_fire_event: mock.MagicMock = mocker.spy(visitran.visitran, "fire_event")

        visitran_obj.search_n_run_tests()
        spy_fire_event.assert_called_with(
            ImportModelsFailed(
                file_name="not_valid",
                err="The module not_valid is not found in the configured path",
            )
        )

    def test_snapshot_module_exception(self, mocker: MockerFixture) -> None:
        mock_context = mocker.MagicMock()

        visitran_obj = Visitran(mock_context)
        context = visitran_obj.context
        context.get_snapshot_files.return_value = ["not_valid"]  # type: ignore

        import visitran

        spy_fire_event: mock.MagicMock = mocker.spy(visitran.visitran, "fire_event")

        with pytest.raises(ModelNotFoundError) as errinfo:
            visitran_obj.run_snapshot()

        assert errinfo.value.args == ("not_valid",)

        spy_fire_event.assert_called_with(ImportModelsFailed(file_name="not_valid", err="No module named 'not_valid'"))

    def test_node_execute_exception(
        self,
        mocker: MockerFixture,
        shared_datadir: Path,
    ) -> None:
        mock_context = mocker.MagicMock()

        visitran_obj = Visitran(mock_context)
        model_files = [
            "dummyproject.models.full_code.blue.Blue",
            "dummyproject.models.full_code.cyan.Cyan",
            "dummyproject.models.full_code.green.Green",
            "dummyproject.models.full_code.magenta.Magenta",
            "dummyproject.models.full_code.red.Red",
            "dummyproject.models.full_code.white_cr.WhiteCR",
            "dummyproject.models.full_code.white_rgb.WhiteRGB",
        ]
        context = visitran_obj.context
        context.get_model_files.return_value = model_files  # type: ignore
        context.get_includes = []  # type: ignore
        context.get_excludes = []  # type: ignore
        sys.path.append(str(shared_datadir))

        try:

            visitran_obj.search_n_run_models()

            assert len(visitran_obj.models) == 7
            assert all(check_substring_in_list(item, list(visitran_obj.models.keys())) for item in model_files)

            visitran_obj.build_dag()
            visitran_obj.sort_dag()

            assert len(visitran_obj.sorted_dag_nodes) == 7

            node_list = visitran_obj.dag.nodes

            assert ["blue", "green", "cyan", "red", "magenta", "white_cr", "white_rgb"] == [
                node_list[x]["model_object"].destination_table_name for x in visitran_obj.sorted_dag_nodes
            ]
            adapter = visitran_obj.db_adapter
            adapter.run_model.side_effect = Exception("test")  # type: ignore[attr-defined]

            with pytest.raises(NodeExecuteError) as errinfo:
                visitran_obj.execute_graph()

            assert "schema_name" in errinfo.value._msg_args
            assert "table_name" in errinfo.value._msg_args
            assert "test" in errinfo.value._msg_args["error_message"]
        finally:
            sys.path.remove(str(shared_datadir))
