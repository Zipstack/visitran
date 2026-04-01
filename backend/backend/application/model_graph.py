import networkx as nx


class ProjectModelGraph:
    def __init__(self):
        # Create a directed graph
        self.graph = nx.DiGraph()

    def add_edge(self, parent, child):
        # Add an edge and implicitly add nodes if they don't exist
        self.graph.add_edge(parent, child)

    def add_node(self, node_id, **attributes):
        # Add a node with additional attributes (e.g., name, etc.)
        self.graph.add_node(node_id, **attributes)

    def remove_edge(self, parent, child):
        # Remove an edge between parent and child
        if self.graph.has_edge(parent, child):
            self.graph.remove_edge(parent, child)
        else:
            raise ValueError(f"Edge from {parent} to {child} does not exist")

    def get_children(self, node, all_levels=False):

        if node not in self.graph:
            return []  # return empty list if graph is invalid

            # Get all children of the given node
        if all_levels:
            # Get all descendants at all levels
            children = list(nx.descendants(self.graph, node))
        else:
            # Get only immediate children (successors)
            children = list(self.graph.successors(node))

            # If no children found, return an empty list
        if not children:
            print(f"Node {node} has no children.")

        return children

    def get_parents(self, node, all_levels=False):

        if node not in self.graph:
            return []  # return empty list if graph is invalid

        if all_levels:
            ancestors = set()

            def traverse(n):
                for parent in self.graph.predecessors(n):
                    if parent not in ancestors:
                        ancestors.add(parent)
                        traverse(parent)  # Recursively find higher ancestors

            traverse(node)
            return list(ancestors)
        else:
            return list(self.graph.predecessors(node))

    def get_node_attributes(self, node):
        # Get all attributes of a given node
        if node in self.graph:
            return self.graph.nodes[node]
        else:
            raise ValueError(f"Node {node} does not exist in the graph")

    def update_node_attributes(self, node, **attributes):
        # Update the attributes of a given node
        if node in self.graph:
            self.graph.nodes[node].update(attributes)
        else:
            raise ValueError(f"Node {node} does not exist in the graph")

    def get_isolated_nodes(self):
        # Get all isolated nodes (nodes with no incoming or outgoing edges)
        return [node for node in self.graph.nodes if self.graph.degree(node) == 0]

    def remove_node(self, node):
        # Remove a node and its associated edges
        if node in self.graph:
            self.graph.remove_node(node)
        else:
            raise ValueError(f"Node {node} does not exist in the graph")

    def serialize(self):
        # Serialize the graph to a format that can be saved or transmitted (e.g., JSON)
        return nx.node_link_data(self.graph)

    def deserialize(self, data):
        if not data or "nodes" not in data or "links" not in data:
            print("Empty or invalid data. Creating an empty graph.")
            return nx.DiGraph()  #
        # Deserialize from the saved format back to a graph
        self.graph = nx.node_link_graph(data)

    def is_empty(self):
        """Check if the graph has any nodes."""
        return self.graph.number_of_nodes() == 0
