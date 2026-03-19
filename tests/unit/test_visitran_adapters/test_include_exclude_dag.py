import networkx as nx
import pytest
from networkx.classes.digraph import DiGraph as Di

from visitran import NodeIncludedIsExcludedError
from visitran.utils import select_dag_based_on_include_exclude as selected_nodes


def graph_unequal_err_string(e: nx.DiGraph, a: nx.DiGraph) -> str:  # pragma: no cover
    return f"E: {e} | A:{a}"


def graph_not_dag_err_string(g: nx.DiGraph) -> str:  # pragma: no cover
    return f"Graph is not DAG: {g}"


def get_dag1() -> nx.DiGraph:
    dag = nx.DiGraph()
    dag.add_edge("B", "C")
    dag.add_edge("C", "D")
    dag.add_edge("A", "B")
    dag.add_edge("D", "E")
    dag.add_edge("K", "D")
    dag.add_edge("6", "E")
    dag.add_edge("5", "6")
    dag.add_edge("4", "5")
    dag.add_edge("3", "4")
    dag.add_edge("2", "3")
    dag.add_edge("1", "2")
    dag.add_edge("0", "1")

    return dag


def get_dag2() -> nx.DiGraph:
    dag = nx.DiGraph()
    dag.add_edge("A", "B")
    dag.add_edge("B", "C")
    dag.add_edge("C", "D")
    dag.add_edge("D", "P")
    dag.add_edge("K", "C")
    dag.add_edge("Y", "C")
    dag.add_edge("J", "Z")
    dag.add_edge("Z", "D")

    return dag


def get_dag3() -> nx.DiGraph:
    dag = nx.DiGraph()
    dag.add_edges_from([("A", "B"), ("A", "C"), ("B", "D"), ("C", "D"), ("D", "E"), ("E", "F")])
    return dag


def get_dag4() -> nx.DiGraph:
    dag = nx.DiGraph()
    dag.add_edges_from([("A", "B"), ("B", "C"), ("C", "D"), ("D", "E"), ("E", "F")])
    return dag


def get_dag5() -> nx.DiGraph:
    dag = nx.DiGraph()
    dag.add_edges_from([("A", "B"), ("B", "C"), ("D", "E"), ("E", "F"), ("F", "G")])
    return dag


def get_dag6() -> nx.DiGraph:
    dag = nx.DiGraph()
    dag.add_node("A")
    dag.add_node("B")
    dag.add_node("C")
    dag.add_node("D")
    return dag


@pytest.mark.unit
@pytest.mark.minimal_core
class TestSelectedNodesWithDAG1:
    dag = get_dag1()

    def test_selected_nodes_include_leaf(self) -> None:
        dag = self.__class__.dag

        include = ["5", "4", "1"]
        exclude = ["E", "D", "K"]
        dagout: Di = selected_nodes(dag, include, exclude)

        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("0", "1"), ("1", "2"), ("2", "3"), ("3", "4"), ("4", "5")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_same_include_exclude(self) -> None:
        dag = self.__class__.dag

        include = ["0"]
        exclude = ["0"]
        pytest.raises(NodeIncludedIsExcludedError, selected_nodes, dag, include, exclude)

    def test_selected_nodes_include_0_only(self) -> None:
        dag = self.__class__.dag

        include = ["0"]
        exclude = ["E"]
        dagout: Di = selected_nodes(dag, include, exclude)

        expected_dag = nx.DiGraph()
        expected_dag.add_node("0")
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)


@pytest.mark.unit
@pytest.mark.minimal_core
class TestSelectedNodesWithDAG2:
    dag = get_dag2()

    def test_selected_nodes_include_highly_connected_node_c(self) -> None:
        dag = self.__class__.dag
        include = ["C"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("A", "B"), ("B", "C"), ("K", "C"), ("Y", "C")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_exclude_highly_connected_node_c(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["C"]
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("A", "B"), ("J", "Z")])
        expected_dag.add_node("K")
        expected_dag.add_node("Y")
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_exclude_highly_connected_node_d(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["D"]
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("A", "B"), ("B", "C"), ("Y", "C"), ("K", "C"), ("J", "Z")])
        expected_dag.add_node("K")
        expected_dag.add_node("Y")
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)


@pytest.mark.unit
@pytest.mark.minimal_core
class TestSelectedNodesWithDAG3:
    dag = get_dag3()

    def test_selected_nodes_with_leaf_included(self) -> None:
        dag = self.__class__.dag

        include = ["F"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = dag
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_merge_with_missing_branch(self) -> None:
        dag = self.__class__.dag
        include = ["D"]
        exclude = ["B"]
        pytest.raises(NodeIncludedIsExcludedError, selected_nodes, dag, include, exclude)

    def test_selected_nodes_exclude_parallel_node(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["C"]
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("A", "B")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)


@pytest.mark.unit
@pytest.mark.minimal_core
class TestSelectedNodesWithDAG4:
    dag = get_dag4()

    def test_selected_nodes_with_root_n_mid_included(self) -> None:
        dag = self.__class__.dag
        include = ["A", "C"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("A", "B"), ("B", "C")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_root_n_leaf_included(self) -> None:
        dag = self.__class__.dag

        include = ["A", "F"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = dag
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_root_mid_leaf_included(self) -> None:
        dag = self.__class__.dag

        include = ["A", "D", "F"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = dag
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_leaf_root_included(self) -> None:
        dag = self.__class__.dag

        include = ["F", "A"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = dag
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_mid_leaf_included(self) -> None:
        dag = self.__class__.dag
        include = ["C", "F"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = dag
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_conflicting_include_exclude(self) -> None:
        dag = self.__class__.dag
        include = ["F"]
        exclude = ["A"]
        pytest.raises(NodeIncludedIsExcludedError, selected_nodes, dag, include, exclude)

    def test_selected_nodes_exclude_root_leaf(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["A", "F"]
        dagout: Di = selected_nodes(dag, include, exclude)
        assert nx.is_empty(dagout)

    def test_selected_nodes_exclude_mid_leaf(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["F", "C"]
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("A", "B")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)


@pytest.mark.unit
@pytest.mark.minimal_core
class TestSelectedNodesWithDAG5:
    dag = get_dag5()

    def test_selected_nodes_with_empyt_include_exclude(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = dag
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_root_node_included(self) -> None:
        dag = self.__class__.dag
        include = ["A"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_node("A")
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_middle_node_included(self) -> None:
        dag = self.__class__.dag
        include = ["B"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("A", "B")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_leaf_node_included(self) -> None:
        dag = self.__class__.dag
        include = ["C"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("A", "B"), ("B", "C")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_invalid_include_node(self) -> None:
        dag = self.__class__.dag
        include = ["Z"]
        exclude: list[str] = []
        pytest.raises(nx.exception.NetworkXError, selected_nodes, dag, include, exclude)

    def test_selected_nodes_with_other_root_node_included(self) -> None:
        dag = self.__class__.dag
        include = ["D"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_node("D")
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_root_node_excluded(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["A"]
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("D", "E"), ("E", "F"), ("F", "G")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_middle_node_excluded(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["B"]
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_node("A")
        expected_dag.add_edges_from([("D", "E"), ("E", "F"), ("F", "G")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_leaf_node_excluded(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["C"]
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("A", "B"), ("D", "E"), ("E", "F"), ("F", "G")])

        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_invalid_exclude_node(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["Z"]
        pytest.raises(nx.exception.NetworkXError, selected_nodes, dag, include, exclude)

    def test_selected_nodes_with_other_root_node_excluded(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["D"]
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_edges_from([("A", "B"), ("B", "C")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)


@pytest.mark.unit
@pytest.mark.minimal_core
class TestSelectedNodesWithDAG6:
    dag = get_dag6()

    def test_selected_nodes_with_empyt_include_exclude(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = dag
        # expected_dag.add_edges_from([("A", "B"), ("B", "C")])
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_empyt_exclude(self) -> None:
        dag = self.__class__.dag
        include = ["A", "B"]
        exclude: list[str] = []
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_node("A")
        expected_dag.add_node("B")
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_empyt_include(self) -> None:
        dag = self.__class__.dag
        include: list[str] = []
        exclude = ["A", "B"]
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_node("C")
        expected_dag.add_node("D")
        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)

    def test_selected_nodes_with_include_exclude(self) -> None:
        dag = self.__class__.dag
        include = ["C"]
        exclude = ["A", "B"]
        dagout: Di = selected_nodes(dag, include, exclude)
        expected_dag = nx.DiGraph()
        expected_dag.add_node("C")

        assert nx.is_isomorphic(expected_dag, dagout), graph_unequal_err_string(expected_dag, dagout)
        assert nx.is_directed_acyclic_graph(dagout), graph_not_dag_err_string(dagout)
