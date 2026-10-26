from collections import defaultdict

from iceaxe.schemas.db_stubs import DBObject


class ActionTopologicalSorter:
    """
    Extends Python's native TopologicalSorter to group nodes by table_name. This provides
    better semantic grouping within the migrations since most actions are oriented by their
    parent table.

    - Places cross-table dependencies and non-table actions (like types) according
        to their default DAG order.
    - Tables are processed in alphabetical order of their names.

    """

    def __init__(self, graph: dict[DBObject, list[DBObject]]):
        self.graph = graph
        self.in_degree = defaultdict(int)
        self.nodes = set(graph.keys())

        for node, dependencies in list(graph.items()):
            for dep in dict.fromkeys(dependencies):
                self.in_degree[node] += 1
                if dep not in self.nodes:
                    self.nodes.add(dep)
                    self.graph[dep] = []

        # Order based on the original yield / creation order of the nodes
        self.node_to_ordering = {node: i for i, node in enumerate(self.graph.keys())}

    def sort(self):
        result = []
        root_nodes_queued = sorted(
            [node for node in self.nodes if self.in_degree[node] == 0],
            key=self.node_key,
        )
        if not root_nodes_queued and self.nodes:
            raise ValueError("Graph contains a cycle")
        elif not root_nodes_queued:
            return []

        # Sort by the table name and then by the node representation
        root_nodes_queued.sort(key=self.node_key)

        # Always put the non-table actions first (things like global types)
        non_table_nodes = [
            node for node in root_nodes_queued if not hasattr(node, "table_name")
        ]
        root_nodes_queued = [
            node for node in root_nodes_queued if node not in non_table_nodes
        ]
        root_nodes_queued = non_table_nodes + root_nodes_queued

        queue = [root_nodes_queued.pop(0)]
        processed = set()

        while True:
            if not queue:
                # Pop another root node, if available
                if root_nodes_queued:
                    queue.append(root_nodes_queued.pop(0))
                    continue
                else:
                    # If no more root nodes are available and no more work to be done
                    break

            current_node = queue.pop(0)

            result.append(current_node)
            processed.add(current_node)

            # Newly unblocked nodes, since we've resolved their dependencies
            # with the current processing
            new_ready = []
            for dependent, deps in self.graph.items():
                if current_node in deps and dependent not in processed:
                    self.in_degree[dependent] -= 1
                    if self.in_degree[dependent] == 0:
                        new_ready.append(dependent)

            # Add newly ready nodes to queue in sorted order
            queue.extend(
                sorted(new_ready, key=lambda node: self.node_to_ordering[node])
            )

        if len(result) != len(self.nodes):
            raise ValueError("Graph contains a cycle")

        return result

    @staticmethod
    def node_key(node: DBObject):
        # Not all objects specify a table_name, but if they do we want to explicitly
        # sort before the representation
        table_name = getattr(node, "table_name", "")
        return (table_name, node.representation())
