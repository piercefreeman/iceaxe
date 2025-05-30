import pytest

from iceaxe.migrations.action_sorter import ActionTopologicalSorter
from iceaxe.schemas.actions import DatabaseActions
from iceaxe.schemas.db_stubs import DBObject


class MockNode(DBObject):
    name: str
    table_name: str = "None"

    model_config = {
        "frozen": True,
    }

    def representation(self):
        return f"MockNode({self.name}, {self.table_name})"

    async def create(self, actor: DatabaseActions):
        pass

    async def migrate(self, previous: DBObject, actor: DatabaseActions):
        pass

    async def destroy(self, actor: DatabaseActions):
        pass

    def __hash__(self):
        return hash(self.representation())


def custom_topological_sort(graph_edges):
    sorter = ActionTopologicalSorter(graph_edges)
    sorted_objects = sorter.sort()
    return {obj: i for i, obj in enumerate(sorted_objects)}


def test_simple_dag():
    A = MockNode(name="A")
    B = MockNode(name="B")
    C = MockNode(name="C")
    D = MockNode(name="D")
    graph = {D: [B, C], C: [A], B: [A], A: []}
    result = custom_topological_sort(graph)
    assert list(result.keys()) == [A, C, B, D]


def test_disconnected_graph():
    A = MockNode(name="A")
    B = MockNode(name="B")
    C = MockNode(name="C")
    D = MockNode(name="D")
    E = MockNode(name="E")
    graph = {B: [A], A: [], D: [C], C: [], E: []}
    result = custom_topological_sort(graph)
    assert set(result.keys()) == {A, B, C, D, E}
    assert result[A] < result[B]
    assert result[C] < result[D]


def test_single_table_grouping():
    A = MockNode(name="A", table_name="table1")
    B = MockNode(name="B", table_name="table1")
    C = MockNode(name="C", table_name="table1")
    graph = {C: [], B: [C], A: [B]}
    result = custom_topological_sort(graph)
    assert list(result.keys()) == [C, B, A]


def test_multiple_table_grouping():
    A = MockNode(name="A", table_name="table1")
    B = MockNode(name="B", table_name="table1")
    C = MockNode(name="C", table_name="table2")
    D = MockNode(name="D", table_name="table2")
    E = MockNode(name="E", table_name="table3")
    graph = {E: [], D: [], C: [D, E], A: [C], B: [C]}
    result = custom_topological_sort(graph)
    assert set(result.keys()) == {A, B, C, D, E}
    assert result[C] < result[A] and result[C] < result[B]
    assert result[D] < result[C] and result[E] < result[C]


def test_cross_table_references():
    A = MockNode(name="A", table_name="table1")
    B = MockNode(name="B", table_name="table2")
    C = MockNode(name="C", table_name="table1")
    D = MockNode(name="D", table_name="table2")
    graph = {D: [], C: [D], B: [C], A: [B]}
    result = custom_topological_sort(graph)
    assert list(result.keys()) == [D, C, B, A]


def test_nodes_without_table_name():
    A = MockNode(name="A", table_name="table1")
    B = MockNode(name="B")
    C = MockNode(name="C", table_name="table2")
    D = MockNode(name="D")
    graph = {D: [], C: [D], B: [C], A: [B]}
    result = custom_topological_sort(graph)
    assert list(result.keys()) == [D, C, B, A]


def test_complex_graph():
    A = MockNode(name="A", table_name="table1")
    B = MockNode(name="B", table_name="table1")
    C = MockNode(name="C", table_name="table2")
    D = MockNode(name="D", table_name="table2")
    E = MockNode(name="E", table_name="table3")
    F = MockNode(name="F")
    G = MockNode(name="G", table_name="table3")
    graph = {G: [], F: [G], E: [G], D: [F], C: [E, F], A: [C, D], B: [C]}
    result = custom_topological_sort(graph)
    assert set(result.keys()) == {A, B, C, D, E, F, G}
    assert result[C] < result[A] and result[D] < result[A]
    assert result[C] < result[B]
    assert result[E] < result[C] and result[F] < result[C]
    assert result[F] < result[D]
    assert result[G] < result[E] and result[G] < result[F]


def test_cyclic_graph():
    A = MockNode(name="A")
    B = MockNode(name="B")
    C = MockNode(name="C")
    graph = {A: [B], B: [C], C: [A]}
    with pytest.raises(ValueError, match="Graph contains a cycle"):
        custom_topological_sort(graph)


def test_empty_graph():
    graph = {}
    result = custom_topological_sort(graph)
    assert result == {}


def test_single_node_graph():
    A = MockNode(name="A")
    graph = {A: []}
    result = custom_topological_sort(graph)
    assert result == {A: 0}


def test_all_nodes_same_table():
    A = MockNode(name="A", table_name="table1")
    B = MockNode(name="B", table_name="table1")
    C = MockNode(name="C", table_name="table1")
    D = MockNode(name="D", table_name="table1")
    graph = {D: [], B: [D], C: [D], A: [B, C]}
    result = custom_topological_sort(graph)
    assert list(result.keys()) == [D, B, C, A]


def test_mixed_node_types():
    A = MockNode(name="A", table_name="table1")
    B = MockNode(name="B")
    C = MockNode(name="C", table_name="table2")
    D = MockNode(name="D", table_name="table3")
    graph = {D: [], C: [D], B: [C], A: [B]}
    result = custom_topological_sort(graph)
    assert list(result.keys()) == [D, C, B, A]


def test_large_graph_performance():
    import random
    import string
    import time

    def generate_large_graph(size):
        nodes = [MockNode(name=c) for c in string.ascii_uppercase] + [
            MockNode(name=f"N{i}") for i in range(size - 26)
        ]
        graph = {node: set() for node in nodes}
        for i, node in enumerate(nodes):
            graph[node] = set(random.sample(nodes[i + 1 :], min(5, len(nodes) - i - 1)))
        return graph

    large_graph = generate_large_graph(1000)
    start_time = time.time()
    result = custom_topological_sort(large_graph)
    end_time = time.time()
    assert len(result) == 1000
    assert end_time - start_time < 5


def test_graph_with_isolated_nodes():
    A = MockNode(name="A")
    B = MockNode(name="B")
    C = MockNode(name="C")
    D = MockNode(name="D")
    E = MockNode(name="E")
    graph = {A: [B], B: [], C: [], D: [E], E: []}
    result = custom_topological_sort(graph)
    assert set(result.keys()) == {A, B, C, D, E}
    assert result[B] < result[A]
    assert result[E] < result[D]


@pytest.mark.parametrize(
    "graph, expected_order",
    [
        (
            {
                MockNode(name="C"): set(),
                MockNode(name="B"): {MockNode(name="C")},
                MockNode(name="A"): {MockNode(name="B")},
            },
            ["C", "B", "A"],
        ),
        (
            {
                MockNode(name="D"): set(),
                MockNode(name="B"): {MockNode(name="D")},
                MockNode(name="C"): {MockNode(name="D")},
                MockNode(name="A"): {MockNode(name="B"), MockNode(name="C")},
            },
            ["D", "B", "C", "A"],
        ),
    ],
)
def test_various_graph_structures(graph, expected_order):
    result = custom_topological_sort(graph)
    assert [node.name for node in result.keys()] == expected_order


def test_consistent_results():
    """
    Test for consistent results with same input

    """
    A = MockNode(name="A", table_name="table1")
    B = MockNode(name="B", table_name="table1")
    C = MockNode(name="C", table_name="table2")
    D = MockNode(name="D", table_name="table2")
    graph = {D: [], C: [D], B: [D], A: [B, C]}
    result1 = custom_topological_sort(graph)
    result2 = custom_topological_sort(graph)
    assert list(result1.keys()) == list(result2.keys())
