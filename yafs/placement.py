import logging
from abc import abstractmethod, ABC
from typing import Callable


class Placement(ABC):
    """A placement (or allocation) algorithm controls where to locate the service modules and their replicas in the different nodes of the topology, according to load criteria or other objectives.

    A placement consists out of two functions:
    - *initial_allocation*: Invoked at the start of the simulation
    - *run*: Invoked according to the assigned temporal distribution

    Args:
        name: Placement name
        activation_dist (function): a distribution function to active the *run* function in execution time  TODO What das function mean? Callable?

    Kwargs:
        param (dict): the parameters of the *activation_dist*  TODO ???
    """

    def __init__(self, name: str, activation_dist: Callable = None, logger=None):  # TODO Remove logger
        self.logger = logger or logging.getLogger(__name__)
        self.name = name
        self.activation_dist = activation_dist
        self.scaleServices = []

    def scaleService(self, scale):  # TODO Refactor, this is not pythonic
        self.scaleServices = scale

    def get_next_activation(self):
        """
        Returns:
            the next time to be activated
        """
        return next(self.activation_dist)  # TODO Data type?

    @abstractmethod
    def initial_allocation(self, sim: "Simulation", app_name: str):  # TODO Why does this know about the simulation?
        """Given an ecosystem, it starts the allocation of modules in the topology."""

    def run(self, sim: "Simulation"):  # TODO Does this have to be implemented?  # TODO Why does this know about the simulation?
        """This method will be invoked during the simulation to change the assignment of the modules to the topology."""
        self.logger.debug("Activiting - RUN - Placement")


class JSONPlacement(Placement):  # TODO The placement should not care how it was instantiated
    def __init__(self, json, **kwargs):
        super(JSONPlacement, self).__init__(**kwargs)
        self.data = json

    def initial_allocation(self, sim, app_name):
        for item in self.data["initialAllocation"]:
            if app_name == item["app"]:
                # app_name = item["app"]
                module = item["module_name"]
                idtopo = item["id_resource"]
                app = sim.apps[app_name]
                services = app.services
                idDES = sim.deploy_module(app_name, module, services[module], [idtopo])  # TODO unused variable


class JSONPlacementOnCloud(Placement):  # TODO The placement should not care how it was instantiated
    def __init__(self, json, idCloud, **kwargs):
        super(JSONPlacementOnCloud, self).__init__(**kwargs)
        self.data = json
        self.idCloud = idCloud

    def initial_allocation(self, sim, app_name):
        for item in self.data["initialAllocation"]:
            if app_name == item["app"]:
                app_name = item["app"]
                module = item["module_name"]

                app = sim.apps[app_name]
                services = app.services
                idDES = sim.deploy_module(app_name, module, services[module], [self.idCloud])  # TODO unused variable


class ClusterPlacement(Placement):
    """Locates the services of the application in the cheapest cluster regardless of where the sources or sinks are located.  # TODO Docstring wrong?

    Only runs once during initialization.
    """

    def initial_allocation(self, sim: "Simulation", app_name: str):  # TODO Why does this know about the simulation?
        # We find the ID-nodo/resource
        value = {"model": "Cluster"}  # TODO These are very implicit assumptions about module naming...
        id_cluster = sim.topology.find_IDs(value)  # there is only ONE Cluster  # TODO Why?
        value = {"model": "m-"}  # TODO These are very implicit assumptions about module naming...
        id_mobiles = sim.topology.find_IDs(value)

        # Given an application we get its modules implemented
        app = sim.apps[app_name]
        services = app.services

        for module in list(services.keys()):
            if "Coordinator" == module:
                if "Coordinator" in list(self.scaleServices.keys()):
                    # print self.scaleServices["Coordinator"]
                    for rep in range(0, self.scaleServices["Coordinator"]):
                        # Deploy as many modules as elements in the array
                        idDES = sim.deploy_module(app_name, module, services[module], id_cluster)  # TODO unused variable

            elif "Calculator" == module:
                if "Calculator" in list(self.scaleServices.keys()):
                    for rep in range(0, self.scaleServices["Calculator"]):
                        idDES = sim.deploy_module(app_name, module, services[module], id_cluster)  # TODO unused variable

            elif "Client" == module:
                idDES = sim.deploy_module(app_name, module, services[module], id_mobiles)  # TODO unused variable


class EdgePlacement(Placement):
    """Locates the services of the application in the cheapest cluster regardless of where the sources or sinks are located.  # TODO Docstring wrong?

    Only runs once during initialization.
    """

    def initial_allocation(self, sim, app_name):
        # We find the ID-nodo/resource
        value = {"model": "Cluster"}
        id_cluster = sim.topology.find_IDs(value)  # there is only ONE Cluster
        value = {"model": "d-"}
        id_proxies = sim.topology.find_IDs(value)

        value = {"model": "m-"}
        id_mobiles = sim.topology.find_IDs(value)

        # Given an application we get its modules implemented
        app = sim.apps[app_name]
        services = app.services

        for module in list(services.keys()):

            print(module)

            if "Coordinator" == module:
                # Deploy as many modules as elements in the array
                idDES = sim.deploy_module(app_name, module, services[module], id_cluster)  # TODO Unused variable
            elif "Calculator" == module:
                idDES = sim.deploy_module(app_name, module, services[module], id_proxies)  # TODO Unused variable
            elif "Client" == module:
                idDES = sim.deploy_module(app_name, module, services[module], id_mobiles)  # TODO Unused variable


class NoPlacementOfModules(Placement):
    """Locates the services of the application in the cheapest cluster regardless of where the sources or sinks are located.  # TODO Docstring wrong?

    Only runs once during initialization.
    """

    def initial_allocation(self, sim, app_name):
        # The are not modules to be allocated
        None
