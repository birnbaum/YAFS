"""
    This type of algorithm have two obligatory functions:

        *initial_allocation*: invoked at the start of the simulation

        *run* invoked according to the assigned temporal distribution.

"""

from yafs.placement import Placement


class CloudPlacement(Placement):
    """
    This implementation locates the services of the application in the cheapest cloud regardless of where the sources or sinks are located.

    It only runs once, in the initialization.

    """

    def initial_allocation(self, simulation, app_name):
        # We find the ID-nodo/resource
        value = {"model": "Cluster"}
        id_cluster = simulation.topology.find_IDs(value)  # there is only ONE Cluster
        value = {"model": "m-"}
        id_mobiles = simulation.topology.find_IDs(value)

        # Given an application we get its modules implemented
        app = simulation.apps[app_name]
        services = app.services

        for module in list(services.keys()):
            if "Coordinator" == module:
                if "Coordinator" in list(self.scaleServices.keys()):
                    # print self.scaleServices["Coordinator"]
                    for rep in range(0, self.scaleServices["Coordinator"]):
                        process_id = simulation.deploy_module(app_name, module, services[module], id_cluster)  # Deploy as many modules as elements in the array

            elif "Calculator" == module:
                if "Calculator" in list(self.scaleServices.keys()):
                    for rep in range(0, self.scaleServices["Calculator"]):
                        process_id = simulation.deploy_module(app_name, module, services[module], id_cluster)

            elif "Client" == module:
                process_id = simulation.deploy_module(app_name, module, services[module], id_mobiles)

    # end function


class FogPlacement(Placement):
    """
    This implementation locates the services of the application in the fog-device regardless of where the sources or sinks are located.

    It only runs once, in the initialization.

    """

    def initial_allocation(self, simulation, app_name):
        # We find the ID-nodo/resource
        value = {"model": "Cluster"}
        id_cluster = simulation.topology.find_IDs(value)  # there is only ONE Cluster

        value = {"model": "d-"}
        id_proxies = simulation.topology.find_IDs(value)

        value = {"model": "m-"}
        id_mobiles = simulation.topology.find_IDs(value)

        # Given an application we get its modules implemented
        app = simulation.applications[app_name]
        services = app.services

        for module in list(services.keys()):
            if "Coordinator" == module:
                if "Coordinator" in list(self.scaleServices.keys()):
                    for rep in range(0, self.scaleServices["Coordinator"]):
                        process_id = simulation.deploy_module(app_name, module, services[module], id_cluster)  # Deploy as many modules as elements in the array
            elif "Calculator" == module:
                if "Calculator" in list(self.scaleServices.keys()):
                    for rep in range(0, self.scaleServices["Calculator"]):
                        process_id = simulation.deploy_module(app_name, module, services[module], id_proxies)
            elif "Client" == module:
                process_id = simulation.deploy_module(app_name, module, services[module], id_mobiles)
