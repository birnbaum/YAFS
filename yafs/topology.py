import logging
import warnings
from typing import Dict

import networkx as nx


logger = logging.getLogger(__name__)


# TODO Is this entire class necessary? Wouldn't it be smarter to just use a nx.networkx instance?
class Topology:
    """Unifies the functions to deal with **Complex Networks** as a network topology within of the simulator.

    In addition, it facilitates its creation, and assignment of attributes.
    """

    LINK_BW = "BW"  # Link feature: Bandwidth
    LINK_PR = "PR"  # Link feature: Propagation delay
    NODE_IPT = "IPT"  # Node feature: Instructions per Simulation Time

    def __init__(self, G: nx.Graph):
        self.G = G
        # self._init_uptimes()

    def _init_uptimes(self):  # TODO What is this used for? -> Something about Watts, should probably not live here
        for key in self.G.nodes:
            self.G.nodes[key]["uptime"] = (0, None)

    def add_node(self, nodes, edges=None):  # TODO edges unused
        """Add a list of nodes in the topology

        Args:
            nodes (list): a list of identifiers
            edges (list): a list of destination edges
        """
        id_ = len(self.G) + 1
        self.G.add_node(id_)
        self.G.add_edges_from(list(zip(nodes, [id_] * len(nodes))))

    def remove_node(self, id_node: int):
        """Remove a node of the topology

        Args:
            id_node: Node identifier
        """
        self.G.remove_node(id_node)


def load_yafs_json(data: Dict) -> nx.Graph:
    """Generates the topology from a JSON file

    Proprietary YAFS format - will be removed in the future
    Deprecated: Use any supported graph format: https://networkx.github.io/documentation/networkx-1.10/reference/readwrite.html
    """
    warnings.warn("Proprietary YAFS format - will be removed in the future", DeprecationWarning)
    G = nx.Graph()
    for entity in data["entity"]:
        G.add_node(entity["id"], **entity)
    for edge in data["link"]:
        G.add_edge(edge["s"], edge["d"], BW=edge[Topology.LINK_BW], PR=edge[Topology.LINK_PR])
    return G


def draw_png(network: nx.networkx, filepath: str):
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(nrows=1, ncols=1)
    pos = nx.spring_layout(network)
    nx.draw(network, pos)
    labels = nx.draw_networkx_labels(network, pos)
    fig.savefig(filepath)  # save the figure to file
    plt.close(fig)  # close the figure
