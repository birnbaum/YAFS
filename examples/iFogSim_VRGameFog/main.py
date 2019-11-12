"""Implementation of "VRGameFog.java [#f1]_  of EGG_GAME a latency-sensitive online game" presented in [#f2]_ (first case study).

.. [#f1] https://github.com/Cloudslab/iFogSim/blob/master/src/org/fog/test/perfeval/VRGameFog.java
.. [#f2] Gupta, H., Vahid Dastjerdi, A., Ghosh, S. K., & Buyya, R. (2017). iFogSim: A toolkit for modeling and simulation of resource management techniques in the Internet of Things, Edge and Fog computing environments. Software: Practice and Experience, 47(9), 1275-1296.
"""

import argparse
import time

from examples.iFogSim_VRGameFog.selection_multipleDeploys import BroadPath, CloudPath_RR
from yafs.application import Application, Message, Module
from yafs.core import Simulation
from yafs.distribution import DeterministicDistribution
from yafs.placement import Placement
from yafs.population import StaticPopulation
from yafs.stats import Stats
from yafs.topology import Topology


class CloudPlacementIFogSIM(Placement):

    def initial_allocation(self, simulation, application):
        # We find the ID-nodo/resource
        value = {"model": "Cluster"}
        id_cluster = simulation.topology.find_IDs(value)  # there is only ONE Cluster
        value = {"model": "m-"}
        id_mobiles = simulation.topology.find_IDs(value)

        # Given an application we get its modules implemented
        app = simulation.deployments[application].application
        for module_name in app.service_modules:
            module = next(m for m in app.modules if m.name == module_name)
            if "Coordinator" == module_name:
                if "Coordinator" in list(self.scaleServices.keys()):
                    # print self.scaleServices["Coordinator"]
                    for rep in range(0, self.scaleServices["Coordinator"]):
                        simulation.deploy_module(application, module.name, module.services, id_cluster)  # Deploy as many modules as elements in the array

            elif "Calculator" == module_name:
                if "Calculator" in list(self.scaleServices.keys()):
                    for rep in range(0, self.scaleServices["Calculator"]):
                        simulation.deploy_module(application, module.name, module.services, id_cluster)

            elif "Client" == module_name:
                simulation.deploy_module(application, module.name, module.services, id_mobiles)


class FogPlacementIFogSIM(Placement):
    """
    This implementation locates the services of the application in the fog-device regardless of where the sources or sinks are located.

    It only runs once, in the initialization.

    """

    def initial_allocation(self, simulation, application):
        # We find the ID-nodo/resource
        value = {"model": "Cluster"}
        id_cluster = simulation.topology.find_IDs(value)  # there is only ONE Cluster

        value = {"model": "d-"}
        id_proxies = simulation.topology.find_IDs(value)

        value = {"model": "m-"}
        id_mobiles = simulation.topology.find_IDs(value)

        # Given an application we get its modules implemented
        app = simulation.deployments[application].application
        for module in list(app.services.keys()):
            if "Coordinator" == module:
                if "Coordinator" in list(self.scaleServices.keys()):
                    for rep in range(0, self.scaleServices["Coordinator"]):
                        simulation.deploy_module(application, module, app.services[module], id_cluster)  # Deploy as many modules as elements in the array
            elif "Calculator" == module:
                if "Calculator" in list(self.scaleServices.keys()):
                    for rep in range(0, self.scaleServices["Calculator"]):
                        simulation.deploy_module(application, module, app.services[module], id_proxies)
            elif "Client" == module:
                simulation.deploy_module(application, module, app.services[module], id_mobiles)


def create_application():
    a = Application(name="EGG_GAME", operators=[
        Module("EGG", is_source=True),
        Module("Display", is_sink=True),
        Module("Client", data={"RAM": 10}),
        Module("Calculator", data={"RAM": 10}),
        Module("Coordinator", data={"RAM": 10}),
    ])

    """
    Messages among MODULES (AppEdge in iFogSim)
    """
    m_egg = Message("M.EGG", "EGG", "Client", instructions=2000 * 10 ^ 6, size=500)
    m_sensor = Message("M.Sensor", "Client", "Calculator", instructions=3500 * 10 ^ 6, size=500)
    m_player_game_state = Message("M.Player_Game_State", "Calculator", "Coordinator", instructions=1000 * 10 ^ 6, size=1000)
    m_concentration = Message("M.Concentration", "Calculator", "Client", instructions=14 * 10 ^ 6, size=500)  # This message is sent to all client modules
    m_global_game_state = Message(
        "M.Global_Game_State", "Coordinator", "Client", instructions=28 * 10 ^ 6, size=1000, broadcasting=True
    )  # This message is sent to all client modules
    m_global_state_update = Message("M.Global_State_Update", "Client", "Display", instructions=1000 * 10 ^ 6, size=500)
    m_self_state_update = Message("M.Self_State_Update", "Client", "Display", instructions=1000 * 10 ^ 6, size=500)

    """
    Defining which messages will be dynamically generated # the generation is controlled by Population algorithm
    """
    a.add_source_message(m_egg)

    """
    MODULES/SERVICES: Definition of Generators and Consumers (AppEdges and TupleMappings in iFogSim)
    """
    # MODULE SOURCES: only periodic messages
    dDistribution = DeterministicDistribution(name="Deterministic", time=100)

    a.add_service_source("Calculator", dDistribution, m_player_game_state)  # According with the comments on VRGameFog.java, the period is 100ms
    a.add_service_source("Coordinator", dDistribution, m_global_game_state)
    # # MODULE SERVICES
    a.add_service_module("Client", m_egg, m_sensor, probability=0.9)
    a.add_service_module("Client", m_concentration, m_self_state_update)
    a.add_service_module("Client", m_global_game_state, m_global_state_update)
    a.add_service_module("Calculator", m_sensor, m_concentration)
    a.add_service_module("Coordinator", m_player_game_state)

    """
    The concept of "loop" (in iFogSim) is not necessary in YAFS, we can extract this information from raw-data
    """

    return a


def create_json_topology(numOfDepts, numOfMobilesPerDept):
    """
       TOPOLOGY DEFINITION

       Some attributes of fog entities (nodes) are approximate
       """

    # CLOUD Abstraction
    id = 0
    cloud_dev = {"id": id, "model": "Cluster", "IPT": 44800 * 10 ^ 6, "RAM": 40000, "COST": 3, "WATT": 20.0}
    id += 1
    # PROXY DEVICE
    proxy_dev = {"id": id, "model": "Proxy-server", "IPT": 2800 * 10 ^ 6, "RAM": 4000, "COST": 3, "WATT": 40.0}

    topology_json = {"entity": [cloud_dev, proxy_dev], "link": [{"s": 0, "d": 1, "BW": 10000, "PR": 14}]}
    id += 1

    for idx in range(numOfDepts):
        # GATEWAY DEVICE
        gw = id
        topology_json["entity"].append({"id": id, "model": "d-", "IPT": 2800 * 10 ^ 6, "RAM": 4000, "COST": 3, "WATT": 40.0})
        topology_json["link"].append({"s": 1, "d": id, "BW": 100, "PR": 10})
        id += 1

        for idm in range(numOfMobilesPerDept):
            # MOBILE DEVICE
            topology_json["entity"].append({"id": id, "model": "m-", "IPT": 1000 * 10 ^ 6, "RAM": 1000, "COST": 0, "WATT": 40.0})
            topology_json["link"].append({"s": gw, "d": id, "BW": 100, "PR": 2})
            id += 1
            # SENSOR
            topology_json["entity"].append({"id": id, "model": "s", "COST": 0, "WATT": 0.0})
            topology_json["link"].append({"s": id - 1, "d": id, "BW": 100, "PR": 4})
            id += 1
            # ACTUATOR
            topology_json["entity"].append({"id": id, "model": "a", "COST": 0, "WATT": 0.0})
            topology_json["link"].append({"s": id - 2, "d": id, "BW": 100, "PR": 1})
            id += 1

    return topology_json


# @profile
def main(simulated_time, depth, police):

    # random.seed(RANDOM_SEED)
    # np.random.seed(RANDOM_SEED)

    """
    TOPOLOGY from a json
    """
    numOfDepts = depth
    numOfMobilesPerDept = 4  # Thus, this variable is used in the population algorithm
    # In YAFS simulator, entities representing mobiles devices (sensors or actuactors) are not necessary because they are simple "abstract" links to the  access points
    # in any case, they can be implemented with node entities with no capacity to execute services.
    #

    t = Topology()
    t_json = create_json_topology(numOfDepts, numOfMobilesPerDept)
    # print t_json
    t.load(t_json)

    # nx.write_gefx(t.G, "network_%s.gexf"%depth)

    """
    APPLICATION
    """
    app = create_application()

    """
    PLACEMENT algorithm
    """
    # In this case: it will deploy all app.modules in the cloud
    if police == "cloud":
        # print "cloud"
        placement = CloudPlacementIFogSIM("onCloud")
        placement.scaleService({"Calculator": numOfDepts * numOfMobilesPerDept, "Coordinator": 1})
    else:
        # print "EDGE"
        placement = FogPlacementIFogSIM("onProxies")
        placement.scaleService({"Calculator": numOfMobilesPerDept, "Coordinator": 1})

    # placement = ClusterPlacement("onCluster", activation_dist=next_time_periodic, time_shift=600)
    """
    POPULATION algorithm
    """
    # In ifogsim, during the creation of the application, the Sensors are assigned to the topology, in this case no. As mentioned, YAFS differentiates the adaptive sensors and their topological assignment.
    # In their case, the use a statical assignment.
    pop = StaticPopulation("Statical")
    # For each type of sink modules we set a deployment on some type of devices
    # A control sink consists on:
    #  args:
    #     model (str): identifies the device or devices where the sink is linked
    #     number (int): quantity of sinks linked in each device
    #     module (str): identifies the module from the app who receives the messages
    pop.set_sink_control({"model": "a", "number": 1, "module": "Display"})  # TODO module hardcoded

    # In addition, a source includes a distribution function:
    dDistribution = DeterministicDistribution(name="Deterministic", time=100)
    pop.set_src_control({"model": "s", "number": 1, "message": app.messages["M.EGG"], "distribution": dDistribution})

    """--
    SELECTOR algorithm
    """
    # Their "selector" is actually the shortest way, there is not type of orchestration algorithm.
    # This implementation is already created in selector.class,called: First_ShortestPath
    if police == "cloud":
        selectorPath = CloudPath_RR()
    else:
        selectorPath = BroadPath(numOfMobilesPerDept)

    """
    SIMULATION ENGINE
    """

    stop_time = simulated_time
    s = Simulation(t, default_results_path="Results_%s_%i_%i" % (police, stop_time, depth))
    s.deploy_app(app, placement, pop, selectorPath)

    s.run(stop_time, progress_bar=False)
    # s.draw_allocated_topology() # for debugging


if __name__ == "__main__":
    import logging.config
    import os

    time_loops = [["M.EGG", "M.Sensor", "M.Concentration"]]

    logging.config.fileConfig(os.getcwd() + "/logging.ini")

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--time", help="Simulated time ")
    parser.add_argument("-d", "--depth", help="Depths ")
    parser.add_argument("-p", "--police", help="cloud or edge ")
    args = parser.parse_args()

    if not args.time:
        stop_time = 10000
    else:
        stop_time = int(args.time)

    start_time = time.time()
    if not args.depth:
        dep = 16
    else:
        dep = int(args.depth)

    if not args.police:
        police = "edge"
    else:
        police = str(args.police)

    # police ="edge"

    for i in range(50):

        main(stop_time, dep, police)
        s = Stats(defaultPath="Results_%s_%s_%s" % (police, stop_time, dep))
        print("%f," % (s.valueLoop(stop_time, time_loops=time_loops)))

    print(("\n--- %s seconds ---" % (time.time() - start_time)))
