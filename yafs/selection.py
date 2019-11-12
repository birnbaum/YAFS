import logging
import random
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Tuple, List, Optional

import networkx as nx

from yafs.application import Message
from yafs.topology import Topology

logger = logging.getLogger(__name__)


class Selection(ABC):
    """Provides the route among topology entities for that a message reach the destiny module, it can also be seen as an orchestration algorithm."""

    @abstractmethod
    def get_paths(self, G: nx.Graph, message: Message, src_node: int, dst_nodes: List[int]) -> List[List[int]]:
        """Computes the message paths from the source node to each destination node.

        Returns:
            List of paths for each provided destination node
        """

    # TODO Find a generic way on how to make this work
    def get_path_from_failure(self, sim, message, link, alloc_DES, alloc_module, ctime) -> Tuple[List[int], Optional[int]]:
        """Called when some link of a message path is broken or unavailable. A new one from that point should be calculated."""
        raise NotImplementedError("This selection algorithm does not support `get_path_from_failure()`")


class RandomPath(Selection):
    """Among all the possible options, it returns a random path."""

    def get_paths(self, G: nx.Graph, message: Message, src_node: int, dst_nodes: List[int]) -> List[List[int]]:
        paths = []
        for dst_node in dst_nodes:
            paths.append([random.choice(list(nx.all_simple_paths(G, source=src_node, target=dst_node)))])
        return paths


class ShortestPath(Selection):
    """Returns the shortest path from node `src_node` to `dst_node` in the network `G`"""

    def get_paths(self, G: nx.Graph, message: Message, src_node: int, dst_nodes: List[int]) -> List[List[int]]:
        paths = []
        for dst_node in dst_nodes:
            return [nx.shortest_path(G, source=src_node, target=dst_node)]
        return paths


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

    def get_paths(self, G, message, src_node, dst_nodes):
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
            path, des = self.get_paths(sim, message.app_name, message, node_src, alloc_DES, alloc_module)
            if len(path[0]) > 0:
                conc_path = message.path[0 : message.path.index(path[0][0])] + path[0]
                message.next_dst = node_src  # TODO Not sure whether Selections should change messages...
                return [conc_path], des
            else:
                return [], []


class CloudPathRR(Selection):  # TODO Refactor to use new interface

    def __init__(self):
        self.rr = defaultdict(int)  # for a each type of service, we have a mod-counter

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module):
        node_src = topology_src
        processes = alloc_module[app_name][message.dst]  # returns an array with all DES process serving

        if message.dst not in list(self.rr.keys()):
            self.rr[message.dst] = 0
        logger.debug(f"Searching path for node {node_src}. Request service: {message.dst}.")

        next_DES_dst = processes[self.rr[message.dst]]
        dst_node = alloc_DES[next_DES_dst]

        path = list(nx.shortest_path(sim.topology.G, source=node_src, target=dst_node))
        bestPath = [path]
        bestDES = [next_DES_dst]
        self.rr[message.dst] = (self.rr[message.dst] + 1) % len(processes)
        return bestPath, bestDES


class BroadPath(Selection):  # TODO Refactor to use new interface
    def __init__(self):
        self.most_near_calculator_to_client = {}
        self.invalid_cache_value = -1

    def _nearest_path(self, node_src, alloc_DES, sim, DES_dst):
        """
        This functions caches the minimun path among client-devices and fog-devices-Module Calculator and it chooses the best calculator process deployed in that node
        """
        # By Placement policy we know that:
        minLenPath = float("inf")
        minPath = []
        bestDES = []
        for dev in DES_dst:
            node_dst = alloc_DES[dev]
            path = list(nx.shortest_path(sim.topology.G, source=node_src, target=node_dst))
            if len(path) < minLenPath:
                minLenPath = len(path)
                minPath = path
                bestDES = dev
        return minPath, bestDES

    def get_paths(self, sim, app_name, message, topology_src, alloc_DES, alloc_module):
        """
        Get the path between a node of the topology and a module deployed in a node. Furthermore it chooses the process deployed in that node.

        """
        node_src = topology_src  # TOPOLOGY SOURCE where the message is generated
        DES_dst = alloc_module[app_name][message.dst]
        if self.invalid_cache_value == len(DES_dst):  # Cache updated
            if node_src not in list(self.most_near_calculator_to_client.keys()):
                # This value is not in the cache
                self.most_near_calculator_to_client[node_src] = self._nearest_path(node_src, alloc_DES, sim, DES_dst)
            path, des = self.most_near_calculator_to_client[node_src]
        else:
            self.invalid_cache_value = len(DES_dst)
            self.most_near_calculator_to_client = {}  # reset previous path-cached values
            # This value is not in the cache
            self.most_near_calculator_to_client[node_src] = self._nearest_path(node_src, alloc_DES, sim, DES_dst)
            path, des = self.most_near_calculator_to_client[node_src]
        return [path], [des]


class MinShortPath(Selection):  # TODO Refactor to use new interface
    # Always it choices the first DES process, It means only one controller.
    # This implementations is so simple, please see the VRGameFog-IfogSim-WL Selection placement to understand better the selection process

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module):
        node_src = topology_src  # TOPOLOGY SOURCE where the message is generated
        DES_dst = alloc_module[app_name][message.dst]
        minLenPath = float("inf")
        minPath = []
        bestDES = 0
        for des in DES_dst:
            node_dst = sim.alloc_DES[des]
            path = list(nx.shortest_path(sim.topology.G, source=node_src, target=node_dst))
            if len(path) < minLenPath:
                minLenPath = len(path)
                minPath = [path]
                bestDES = [des]
        return minPath, bestDES


class MinPathRoundRobin(Selection):  # TODO Refactor to use new interface
    def __init__(self):
        self.rr = {}  # for a each type of service, we have a mod-counter

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module):
        """
        Computes the minimun path among the source elemento of the topology and the localizations of the module

        Return the path and the identifier of the module deployed in the last element of that path
        """
        node_src = topology_src
        processes = alloc_module[app_name][message.dst]  # returns an array with all DES process serving

        if message.dst not in list(self.rr.keys()):
            self.rr[message.dst] = 0
        logger.debug(f"Searching path for node {node_src}. Request service: {message.dst}.")

        bestPath = []
        bestDES = []

        for ix, des in enumerate(processes):
            if message.name == "M.A":
                if self.rr[message.dst] == ix:
                    dst_node = alloc_DES[des]

                    path = list(nx.shortest_path(sim.topology.G, source=node_src, target=dst_node))

                    bestPath = [path]
                    bestDES = [des]

                    self.rr[message.dst] = (self.rr[message.dst] + 1) % len(processes)
                    break
            else:  # message.name == "M.B"

                dst_node = alloc_DES[des]

                path = list(nx.shortest_path(sim.topology.G, source=node_src, target=dst_node))
                if message.broadcasting:
                    bestPath.append(path)
                    bestDES.append(des)
                else:
                    bestPath = [path]
                    bestDES = [des]

        return bestPath, bestDES
