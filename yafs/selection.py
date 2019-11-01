import logging
import random
from abc import ABC, abstractmethod
from typing import Tuple

import networkx as nx


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


class OneRandomPathSelection(Selection):
    """Among all the possible options, it returns a random path."""

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module, traffic, from_des):
        paths = []
        dst_idDES = []
        src_node = topology_src
        DES = alloc_module[message.app_name][message.dst]
        for idDES in DES:
            dst_node = alloc_module[idDES]
            pathX = list(nx.all_simple_paths(sim.topology.G, source=src_node, target=dst_node))
            one = random.randint(0, len(pathX) - 1)
            paths.append(pathX[one])
            dst_idDES.append(idDES)
        return paths, dst_idDES


class FirstShortestPathSelection(Selection):  # MinimunPath??
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
