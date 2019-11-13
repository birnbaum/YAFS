import logging
from abc import abstractmethod, ABC
from typing import Iterator, List

import networkx as nx

from pyfogsim.application import Application

logger = logging.getLogger(__name__)


class Placement(ABC):
    """A placement (or allocation) algorithm controls where to locate the service modules and their replicas in the different nodes of the topology,
    according to load criteria or other objectives.

    A placement consists out of two functions:
    - *initial_allocation*: Invoked at the start of the simulation
    - *run*: Invoked according to the assigned temporal distribution

    Args:
        apps: List of applications to place on the network
        activation_dist: a distribution function to active the *run* function in execution time
    """

    def __init__(self, apps: List[Application], activation_dist: Iterator = None):
        self.apps = apps
        self.activation_dist = activation_dist

    def run(self, simulation: "Simulation"):
        """This method will be invoked during the simulation to change the assignment of the modules to the topology."""
        self._initial_allocation(simulation)
        if self.activation_dist:
            while True:
                try:
                    next_activation = next(self.activation_dist)
                except StopIteration:
                    break
                else:
                    yield simulation.env.timeout(next_activation)
                    self._run(simulation)

    def _initial_allocation(self, simulation: "Simulation"):  # TODO Why does this know about the simulation?
        """Given an ecosystem, it starts the allocation of modules in the topology."""
        self._run(simulation)

    @abstractmethod
    def _run(self, simulation: "Simulation"):  # TODO Why does this know about the simulation?
        """This method will be invoked during the simulation to change the assignment of the modules to the topology."""


class CloudPlacement(Placement):
    """Locates the operator of the application in the node with the highest processing power"""

    def _run(self, simulation: "Simulation"):
        logger.debug(f"CloudPlacement placing {len(self.apps)} applications.")
        for app in self.apps:
            cloud_node_id, _ = max(simulation.network.nodes(data=True), key=lambda node: node[1]["IPT"])
            for operator in app.operators:
                logger.debug(f"CloudPlacement placing operator '{operator.name}' at node '{cloud_node_id}'.")
                operator.node = cloud_node_id


class EdgePlacement(Placement):  # TODO First implementation, now very sophisticated
    """Locates the services of the application in the first hop on the shortest path to the destination"""

    def _run(self, simulation: "Simulation"):
        logger.debug(f"EdgePlacement placing {len(self.apps)} applications.")
        for app in self.apps:
            path = nx.shortest_path(simulation.network, source=app.source.node, target=app.sink.node)
            for operator in app.operators:
                logger.debug(f"EdgePlacement placing operator '{operator.name}' at node '{path[1]}'.")
                operator.node = path[1]
