import logging
from abc import abstractmethod, ABC
from typing import Iterator, List

from yafs.application import Application

logger = logging.getLogger(__name__)


class Placement(ABC):  # TODO Logger
    """A placement (or allocation) algorithm controls where to locate the service modules and their replicas in the different nodes of the topology,
    according to load criteria or other objectives.

    A placement consists out of two functions:
    - *initial_allocation*: Invoked at the start of the simulation
    - *run*: Invoked according to the assigned temporal distribution

    Args:
        name: Placement name
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
    def _run(self, simulation: "Simulation"):  # TODO Does this have to be implemented?  # TODO Why does this know about the simulation?
        """This method will be invoked during the simulation to change the assignment of the modules to the topology."""


class CloudPlacement(Placement):
    """Locates the services of the application in the cheapest cloud regardless of where the sources or sinks are located.  # TODO Docstring wrong?

    It only runs once, in the initialization.
    """

    def _run(self, simulation: "Simulation"):
        for app in self.apps:
            cloud_node_id, _ = max(simulation.topology.G.nodes(data=True), key=lambda node: node[1]["IPT"])
            for module in app.service_modules:
                simulation.deploy_module(app, module.name, module.services, node_ids=[cloud_node_id])
