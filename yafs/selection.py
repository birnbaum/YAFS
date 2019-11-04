import logging
import random
from abc import ABC, abstractmethod
from typing import Tuple

import networkx as nx

from yafs.topology import Topology

logger = logging.getLogger(__name__)


class Selection(ABC):
    """Provides the route among topology entities for that a message reach the destiny module, it can also be seen as an orchestration algorithm."""

    def __init__(self):
        self.transmit = 0.0
        self.lat_acc = 0.0
        self.propagation = 0.0

    @abstractmethod
    def get_path(self, sim: "Simulation", app_name: str, message, topology_src, alloc_DES, alloc_module, traffic, from_des) -> Tuple:  # TODO Why does this know about the simulation?
        """Provides the route to follow the message within the topology to reach the destination module,.
        
        both empty arrays implies that the message will not send to the destination.  # TODO ???
        
        # TODO Missing documentation

        Returns:
            - Path among nodes
            - Identifier of the module
        """
        logger.debug("Selection")
        """ Define Selection """
        path = []
        ids = []

        """ END Selection """
        return path, ids

    def get_path_from_failure(self, sim, message, link, alloc_DES, alloc_module, traffic, ctime, from_des):
        """Called when some link of a message path is broken or unavailable. A new one from that point should be calculated.

        .. attention:: this function is optional  # TODO ???

        Args:
            sim:
            message:
            link:
            alloc_DES:
            alloc_module:
            traffic:
            ctime:
            from_des:

        Returns:
            # TODO ???
        """
        """ Define Selection """
        path = []
        ids = []

        """ END Selection """
        return path, ids


class RandomPath(Selection):
    """Among all the possible options, it returns a random path."""

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module, traffic, from_des):
        paths = []
        src_node = topology_src
        process_ids = alloc_module[message.app_name][message.dst]
        for process_id in process_ids:
            dst_node = alloc_module[process_id]
            pathX = list(nx.all_simple_paths(sim.topology.G, source=src_node, target=dst_node))
            one = random.randint(0, len(pathX) - 1)
            paths.append(pathX[one])
        return paths, process_ids


class FirstShortestPath(Selection):  # MinimunPath??
    """Among all possible shorter paths, returns the first.

    TODO Write docstring from following snippets collected around the codebase:
    - Their "selector" is actually the shortest way, there is not type of orchestration algorithm.
    - Computes the minimum path among the source elemento of the topology and the localizations of the module
      Return the path and the identifier of the module deployed in the last element of that path
    """

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module, traffic, from_des):
        node_src = topology_src  # TOPOLOGY SOURCE where the message is generated
        DES_dst = alloc_module[app_name][message.dst]

        print("GET PATH")
        print(("\tNode _ src (id_topology): %i" % node_src))
        print(("\tRequest service: %s " % message.dst))
        print(("\tProcess serving that service: %s " % DES_dst))

        # Among all possible path we choose the smallest
        bestPath = []
        bestDES = []
        for des in DES_dst:
            dst_node = alloc_DES[des]
            print(("\t\t Looking the path to id_node: %i" % dst_node))
            path = list(nx.shortest_path(sim.topology.G, source=node_src, target=dst_node))
            bestPath = [path]
            bestDES = [des]

        return bestPath, bestDES


class CloudPathRR(Selection):

    def __init__(self):
        super().__init__()
        self.rr = {}  # for a each type of service, we have a mod-counter

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module, traffic, from_des):
        node_src = topology_src
        DES_dst = alloc_module[app_name][message.dst]  # returns an array with all DES process serving

        if message.dst not in list(self.rr.keys()):
            self.rr[message.dst] = 0

        # print "GET PATH"
        # print "\tNode _ src (id_topology): %i" % node_src
        # print "\tRequest service: %s " % (message.dst)
        # print "\tProcess serving that service: %s (pos ID: %i)" % (DES_dst, self.rr[message.dst])

        next_DES_dst = DES_dst[self.rr[message.dst]]
        dst_node = alloc_DES[next_DES_dst]

        path = list(nx.shortest_path(sim.topology.G, source=node_src, target=dst_node))
        bestPath = [path]
        bestDES = [next_DES_dst]
        self.rr[message.dst] = (self.rr[message.dst] + 1) % len(DES_dst)
        return bestPath, bestDES


class BroadPath(Selection):
    def __init__(self):
        super().__init__()
        self.most_near_calculator_to_client = {}
        self.invalid_cache_value = -1

    def compute_most_near(self, node_src, alloc_DES, sim, DES_dst):
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

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module, traffic, from_des):
        """
        Get the path between a node of the topology and a module deployed in a node. Furthermore it chooses the process deployed in that node.

        """
        node_src = topology_src  # TOPOLOGY SOURCE where the message is generated

        # print "Node (Topo id): %s" %node_src
        # print "Service DST: %s "%message.dst
        DES_dst = alloc_module[app_name][message.dst]

        # print "DES DST: %s" % DES_dst

        if self.invalid_cache_value == len(DES_dst):  # Cache updated

            if node_src not in list(self.most_near_calculator_to_client.keys()):
                # This value is not in the cache
                self.most_near_calculator_to_client[node_src] = self.compute_most_near(node_src, alloc_DES, sim, DES_dst)

            path, des = self.most_near_calculator_to_client[node_src]

            # print "\t NEW DES_DST: %s" % DES_dst
            # print "PATH ",path
            # print "DES  ",des

            return [path], [des]

        else:
            self.invalid_cache_value = len(DES_dst)
            # print "\t Invalid cached "
            # print "\t NEW DES_DST: %s" %DES_dst
            self.most_near_calculator_to_client = {}  # reset previous path-cached values

            # This value is not in the cache
            self.most_near_calculator_to_client[node_src] = self.compute_most_near(node_src, alloc_DES, sim, DES_dst)

            path, des = self.most_near_calculator_to_client[node_src]

            # print "\t NEW DES_DST: %s" % DES_dst
            # print "PATH ",path
            # print "DES  ",des

            return [path], [des]


class MinShortPath(Selection):
    def __init__(self):
        super(MinShortPath, self).__init__()
        # Always it choices the first DES process, It means only one controller.
        # This implementations is so simple, please see the VRGameFog-IfogSim-WL Selection placement to understand better the selection process

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module, traffic, from_des):
        """
        Get the path between a node of the topology and a module deployed in a node. Furthermore it chooses the process deployed in that node.
        """
        node_src = topology_src  # TOPOLOGY SOURCE where the message is generated

        # print "Node (Topo id): %s" %node_src
        # print "Service DST: %s "%message.dst
        DES_dst = alloc_module[app_name][message.dst]
        # print "DES DST: %s" % DES_dst
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


class MinPathRoundRobin(Selection):
    def __init__(self):
        self.rr = {}  # for a each type of service, we have a mod-counter

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module, traffic, from_des):
        """
        Computes the minimun path among the source elemento of the topology and the localizations of the module

        Return the path and the identifier of the module deployed in the last element of that path
        """
        node_src = topology_src
        DES_dst = alloc_module[app_name][message.dst]  # returns an array with all DES process serving

        if message.dst not in list(self.rr.keys()):
            self.rr[message.dst] = 0
        logger.debug(f"Searching path for node {node_src}. Request service: {message.dst} (process_id={DES_dst})")

        bestPath = []
        bestDES = []

        for ix, des in enumerate(DES_dst):
            if message.name == "M.A":
                if self.rr[message.dst] == ix:
                    dst_node = alloc_DES[des]

                    path = list(nx.shortest_path(sim.topology.G, source=node_src, target=dst_node))

                    bestPath = [path]
                    bestDES = [des]

                    self.rr[message.dst] = (self.rr[message.dst] + 1) % len(DES_dst)
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


class DeviceSpeedAwareRouting(Selection):
    def __init__(self):
        self.cache = {}
        self.cache_size = -1  # Number of nodes in the cache. If the number changes, the cache is getting cleared.

    @staticmethod
    def _best_dst_process(src_node, alloc_DES, G, DES_dst, message):
        tuples = [(nx.shortest_path(G, src_node, alloc_DES[dev]), dev) for dev in DES_dst]
        return min(tuples, key=lambda path, _: len(path))

    @staticmethod
    def _DSAR(src_node, alloc_DES, G, DES_dst, message):
        def _speed(path, _):
            network_time = 0
            for node in range(len(path) - 1):
                edge = G.edges[(path[node], path[node + 1])]
                network_time += edge[Topology.LINK_PR] + (message.bytes / edge[Topology.LINK_BW])
            dst_node = G.nodes[path[-1]]
            processing_time = message.instructions / float(dst_node["IPT"])
            return network_time + processing_time
        tuples = [(dev, nx.shortest_path(G, src_node, alloc_DES[dev])) for dev in DES_dst]
        return min(tuples, key=_speed)

    def get_paths(self, G, app_name, message, src_node, alloc_DES, alloc_module):
        DES_dst = alloc_module[app_name][message.dst]  # module sw that can serve the message
        if self.cache_size != len(G):
            self.cache_size = len(G)
            self.cache = {}

        if (src_node, DES_dst) not in list(self.cache.keys()):
            # _DSAR
            self.cache[src_node, DES_dst] = self._best_dst_process(src_node, alloc_DES, sim, DES_dst, message)

        path, des = self.cache[src_node, DES_dst]
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
                message.dst_int = node_src  # TODO Not sure whether Selections should change messages...
                return [conc_path], des
            else:
                return [], []
