import logging
import warnings
from typing import Dict

import networkx as nx


# TODO Is this entire class necessary? Wouldn't it be smarter to just use a nx.networkx instance?


class Topology:
    """Unifies the functions to deal with **Complex Networks** as a network topology within of the simulator.

    In addition, it facilitates its creation, and assignment of attributes.
    """

    LINK_BW = "BW"  # Link feature: Bandwidth
    LINK_PR = "PR"  # Link feature: Propagation delay
    # LINK_LATENCY = "LATENCY"  # A edge or a network link has a Bandwidth"

    NODE_IPT = "IPT"  # Node feature: Instructions per Simulation Time

    def __init__(self, logger=None):  # TODO Remove logger  G: nx.Graph,
        self.G = None # G

        # TODO VERSION 2. THIS VALUE SHOULD BE REMOVED
        # INSTEAD USE NX.G. attributes
        self.nodeAttributes = {}

        self.logger = logger or logging.getLogger(__name__)

    def _init_uptimes(self):  # TODO What is this used for?
        for key in self.nodeAttributes:
            self.nodeAttributes[key]["uptime"] = (0, None)

    @property
    def edges(self):
        return self.G.edges

    def load(self, data: Dict):
        """Generates the topology from a JSON file"""
        self.G = nx.Graph()
        for edge in data["link"]:
            self.G.add_edge(edge["s"], edge["d"], BW=edge[self.LINK_BW], PR=edge[self.LINK_PR])

        # TODO This part can be removed in next versions
        for node in data["entity"]:
            self.nodeAttributes[node["id"]] = node
        # end remove

        # Correct way to use custom and mandatory topology attributes
        valuesIPT = {node["id"]: (node["IPT"] if "IPT" in node else 0) for node in data["entity"]}
        valuesRAM = {node["id"]: (node["RAM"] if "RAM" in node else 0) for node in data["entity"]}

        nx.set_node_attributes(self.G, values=valuesIPT, name="IPT")
        nx.set_node_attributes(self.G, values=valuesRAM, name="RAM")

        self._init_uptimes()

    def load_graphml(self, filename):
        warnings.warn(
            "The load_graphml function is deprecated and " "will be removed in version 2.0.0. " "Use NX.READ_GRAPHML function instead.",
            FutureWarning,
            stacklevel=8,
        )
        self.G = nx.read_graphml(filename)

        nx.set_edge_attributes(self.G, values={k: {"BW": 1, "PR": 1} for k in self.G.edges()})
        nx.set_node_attributes(self.G, values={k: {"IPT": 1} for k in self.G.nodes()})

        for k in self.G.nodes():
            self.nodeAttributes[k] = self.G.node[k]  # it has "id" att. TODO IMPROVE

    def get_nodes_att(self):
        """
        Returns:
            A dictionary with the features of the nodes
        """
        return self.nodeAttributes

    def find_IDs(self, value):
        """Search for nodes with the same attributes that value

        Args:
             value (dict). example value = {"model": "m-"}. Only one key is admitted

        Returns:
            A list with the ID of each node that have the same attribute that the value.value
        """
        keyS = list(value.keys())[0]

        result = []
        for key in list(self.nodeAttributes.keys()):
            val = self.nodeAttributes[key]
            if keyS in val:
                if value[keyS] == val[keyS]:
                    result.append(key)
        return result

    def add_node(self, nodes, edges=None):  # TODO edges unused
        """Add a list of nodes in the topology

        Args:
            nodes (list): a list of identifiers
            edges (list): a list of destination edges
        """
        id_ = len(self.G) + 1
        self.G.add_node(id_)
        self.G.add_edges_from(list(zip(nodes, [id_] * len(nodes))))
        return id_

    def remove_node(self, id_node: int):
        """Remove a node of the topology

        Args:
            id_node: Node identifier
        """
        self.G.remove_node(id_node)


def draw_png(network: nx.networkx, filepath: str):
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(nrows=1, ncols=1)
    pos = nx.spring_layout(network)
    nx.draw(network, pos)
    labels = nx.draw_networkx_labels(network, pos)
    fig.savefig(filepath)  # save the figure to file
    plt.close(fig)  # close the figure
