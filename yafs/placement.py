import logging
from abc import abstractmethod, ABC
from typing import Callable, Iterator

from yafs.application import Application

logger = logging.getLogger(__name__)


class Placement(ABC):
    """A placement (or allocation) algorithm controls where to locate the service modules and their replicas in the different nodes of the topology, according to load criteria or other objectives.

    A placement consists out of two functions:
    - *initial_allocation*: Invoked at the start of the simulation
    - *run*: Invoked according to the assigned temporal distribution

    Args:
        name: Placement name
        activation_dist: a distribution function to active the *run* function in execution time  TODO What das function mean? Callable?
    """

    def __init__(self, name: str, activation_dist: Iterator = None):  # TODO Remove logger
        self.name = name  # TODO What do we need this for
        self.activation_dist = activation_dist
        self.scaleServices = {}  # TODO What does this do??

    def scaleService(self, scale):  # TODO Refactor, this is not pythonic
        self.scaleServices = scale

    def get_next_activation(self):
        """
        Returns:
            the next time to be activated
        """
        return next(self.activation_dist)  # TODO Data type?

    @abstractmethod
    def initial_allocation(self, simulation: "Simulation", application: Application):  # TODO Why does this know about the simulation?
        """Given an ecosystem, it starts the allocation of modules in the topology."""

    def run(self, simulation: "Simulation"):  # TODO Does this have to be implemented?  # TODO Why does this know about the simulation?
        """This method will be invoked during the simulation to change the assignment of the modules to the topology."""


class CloudPlacement(Placement):
    """Locates the services of the application in the cheapest cloud regardless of where the sources or sinks are located.  # TODO Docstring wrong?

    It only runs once, in the initialization.
    """

    def initial_allocation(self, simulation: "Simulation", application: str):
        for app in self.apps:
        cloud_node_id, _ = max(simulation.topology.G.nodes(data=True), key=lambda node: node[1]["IPT"])
        app = simulation.deployments[application].application
        for module in app.service_modules:
            simulation.deploy_module(application, module.name, module.services, node_ids=[cloud_node_id])


class JSONPlacement(Placement):  # TODO The placement should not care how it was instantiated
    def __init__(self, json, **kwargs):
        super(JSONPlacement, self).__init__(**kwargs)
        self.data = json

    def initial_allocation(self, simulation, application):
        for item in self.data["initialAllocation"]:
            if application == item["app"]:
                app = simulation.deployments[application].application
                module = next(m for m in app.modules if m.name == item["module_name"])
                idtopo = item["id_resource"]

                simulation.deploy_module(application, module.name, module.services, [idtopo])


class JSONPlacementOnlyCloud(Placement):  # TODO The placement should not care how it was instantiated
    """Initialization of the service only in the cloud. We filter the rest of the assignments from the JSON file"""

    def __init__(self, json, idcloud, **kwargs):
        super(JSONPlacementOnlyCloud, self).__init__(**kwargs)
        self.data = json
        self.idcloud = idcloud

    def initial_allocation(self, simulation, application):
        for item in self.data["initialAllocation"]:
            idtopo = item["id_resource"]
            print(idtopo)
            if idtopo == self.idcloud:
                app = simulation.apps[item["app"]]
                module = next(m for m in app.modules if m.name == item["module_name"])
                simulation.deploy_module(application, module.name, module.services, [idtopo])


class NoPlacementOfModules(Placement):

    def initial_allocation(self, simulation, application):
        pass
