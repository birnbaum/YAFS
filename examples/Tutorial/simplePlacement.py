"""
    This type of algorithm have two obligatory functions:

        *initial_allocation*: invoked at the start of the simulation

        *run* invoked according to the assigned temporal distribution.

"""

from yafs.placement import Placement


class CloudPlacement(Placement):
    """This implementation locates the services of the application in the cheapest cloud regardless of where the sources or sinks are located.

    It only runs once, in the initialization.

    """

    def initial_allocation(self, simulation: "Simulation", app_name: str):  # TODO Why does the placement know about the simulation?
        # We find the ID-nodo/resource
        value = {"mytag": "cloud"}  # or whatever tag

        id_cluster = simulation.topology.find_IDs(value)
        app = simulation.applications[app_name]
        services = app.services

        for module in services:
            if module in self.scaleServices:
                for rep in range(0, self.scaleServices[module]):
                    idDES = simulation.deploy_module(app_name, module, services[module], id_cluster)

    # end function
