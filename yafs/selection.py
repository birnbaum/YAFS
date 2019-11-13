import logging
import random
from abc import ABC, abstractmethod
from typing import Tuple, List, Optional, Any

import networkx as nx

from yafs.application import Message

logger = logging.getLogger(__name__)


class Selection(ABC):
    """Provides the route among topology entities for that a message reach the destiny module, it can also be seen as an orchestration algorithm."""

    @abstractmethod
    def get_path(self, G: nx.Graph, message: Message, src_node: Any, dst_node: Any) -> List[Any]:
        """Computes the message paths from the source node to each destination node.

        Returns:
            List of paths for each provided destination node
        """


class RandomPath(Selection):
    """Among all the possible options, it returns a random path."""

    def get_path(self, G: nx.Graph, message: Message, src_node: Any, dst_node: Any) -> List[Any]:
        return random.choice(list(nx.all_simple_paths(G, source=src_node, target=dst_node)))


class ShortestPath(Selection):
    """Returns the shortest path from node `src_node` to `dst_node` in the network `G`"""

    def get_path(self, G: nx.Graph, message: Message, src_node: Any, dst_node: Any) -> List[Any]:
        return nx.shortest_path(G, source=src_node, target=dst_node)


class DeviceSpeedAwareRouting(Selection):
    def __init__(self):
        self.cache = {}
        self.cache_size = -1  # Number of nodes in the cache. If the number changes, the cache is getting cleared.

    @staticmethod
    def _best_dst_process(src_node, dst_nodes, G, message):
        tuples = [(nx.shortest_path(G, src_node, n), n) for n in dst_nodes]
        return min(tuples, key=lambda path, _: len(path))

    @staticmethod
    def _DSAR(src_node, dst_nodes, G, message):
        def _speed(path, _):
            network_time = 0
            for node in range(len(path) - 1):
                edge = G.edges[(path[node], path[node + 1])]
                network_time += edge[Topology.LINK_PR] + (message.bytes / edge[Topology.LINK_BW])
            dst_node = G.nodes[path[-1]]
            processing_time = message.instructions / float(dst_node["IPT"])
            return network_time + processing_time
        tuples = [(nx.shortest_path(G, src_node, n), n) for n in dst_nodes]
        return min(tuples, key=_speed)

    def get_path(self, G, message, src_node, dst_nodes):
        if self.cache_size != len(G):
            self.cache_size = len(G)
            self.cache = {}

        if (src_node, dst_nodes) not in list(self.cache.keys()):
            self.cache[src_node, dst_nodes] = self._DSAR(src_node, dst_nodes, G, message)
            self.cache[src_node, dst_nodes] = self._best_dst_process(src_node, dst_nodes, G, message)

        path, des = self.cache[src_node, dst_nodes]
        return [path], [des]

    def get_path_from_failure(self, sim, message, link, alloc_DES, alloc_module, ctime):
        index = message.path.index(link[0])
        if index == len(message.path):
            # The node who serves ... not possible case  # TODO Rework
            return [], []
        else:
            node_src = message.path[index]  # In this point to the other entity the system fail
            path, des = self.get_path(sim, message.app_name, message, node_src, alloc_DES, alloc_module)
            if len(path[0]) > 0:
                conc_path = message.path[0 : message.path.index(path[0][0])] + path[0]
                message.next_dst = node_src  # TODO Not sure whether Selections should change messages...
                return [conc_path], des
            else:
                return [], []
