"""

    Created on Wed Nov 22 15:03:21 2017

    @author: isaac

"""
import random

import networkx as nx

from yafs.core import Simulation
from yafs.application import Application, Message, Module

from yafs.population import *
from yafs.selection import FirstShortestPathSelection
from yafs.topology import Topology, load_yafs_json

from examples.Tutorial.simplePlacement import CloudPlacement
from yafs.stats import Stats
from yafs.distribution import DeterministicDistribution
from yafs.utils import fractional_selectivity
import time
import numpy as np

RANDOM_SEED = 1


def create_application():
    a = Application(name="SimpleApp", modules=[  # (S) --> (ServiceA) --> (A)
        Module("Sensor", is_source=True),
        Module("ServiceA", data={"RAM": 10}),
        Module("Actuator", is_sink=True),
    ])

    # Messages among MODULES (AppEdge in iFogSim)
    message_a = Message("M.A", src="Sensor", dst="ServiceA", instructions=20 * 10 ^ 6, size=1000)
    message_b = Message("M.B", src="ServiceA", dst="Actuator", instructions=30 * 10 ^ 6, size=500)

    # Defining which messages will be dynamically generated # the generation is controlled by Population algorithm
    a.add_source_messages(message_a)

    # MODULES/SERVICES: Definition of Generators and Consumers (AppEdges and TupleMappings in iFogSim)
    a.add_service_module("ServiceA", message_a, message_b, fractional_selectivity, threshold=1.0)

    return a


# @profile
def main(simulated_time):
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    G = load_yafs_json({
        "entity": [
            {"id": 0, "model": "cloud", "mytag": "cloud", "IPT": 5000 * 10 ^ 6, "RAM": 40000, "COST": 3, "WATT": 20.0},
            {"id": 1, "model": "sensor-device", "IPT": 100 * 10 ^ 6, "RAM": 4000, "COST": 3, "WATT": 40.0},
            {"id": 2, "model": "actuator-device", "IPT": 100 * 10 ^ 6, "RAM": 4000, "COST": 3, "WATT": 40.0},
        ],
        "link": [
            {"s": 0, "d": 1, "BW": 1, "PR": 10},
            {"s": 0, "d": 2, "BW": 1, "PR": 1}
        ],
    })
    t = Topology(G)

    app = create_application()

    """
    PLACEMENT algorithm
    """
    placement = CloudPlacement("onCloud")  # it defines the deployed rules: module-device
    placement.scaleService({"ServiceA": 1})

    """
    POPULATION algorithm
    """
    # In ifogsim, during the creation of the application, the Sensors are assigned to the topology, in this case no.
    # As mentioned, YAFS differentiates the adaptive sensors and their topological assignment.
    # In their case, the use a statical assignment.
    # For each type of sink modules we set a deployment on some type of devices
    # A control sink consists on:
    #  args:
    #     model (str): identifies the device or devices where the sink is linked
    #     number (int): quantity of sinks linked in each device
    #     module (str): identifies the module from the app who receives the messages
    population = StaticPopulation("Statical")
    population.set_sink_control({"model": "actuator-device", "number": 1, "module": "Actuator"})  # TODO Sink hardcoded
    population.set_src_control({"model": "sensor-device", "number": 1, "message": app.messages["M.A"],
                                "distribution": DeterministicDistribution(name="Deterministic", time=100)})

    stop_time = simulated_time
    s = Simulation(t, default_results_path="Results")
    s.deploy_app(app, placement=placement, population=population, selection=FirstShortestPathSelection())
    s.run(stop_time, show_progress_monitor=False)

    # s.draw_allocated_topology() # for debugging


if __name__ == "__main__":
    import logging.config
    import os

    # logging.config.fileConfig(os.getcwd() + "/logging.ini")

    start_time = time.time()
    main(simulated_time=1000)

    print(("\n--- %s seconds ---" % (time.time() - start_time)))

    ### Finally, you can analyse the results:
    # print("-"*20)
    # print("Results:")
    # print("-" * 20)
    m = Stats(default_path="Results")  # Same name of the results
    time_loops = [["M.A", "M.B"]]
    m.showResults2(1000, time_loops=time_loops)
    # print("\t- Network saturation -")
    # print("\t\tAverage waiting messages : %i" % m.average_messages_not_transmitted())
    # print("\t\tPeak of waiting messages : %i" % m.peak_messages_not_transmitted())
    # print("\t\tTOTAL messages not transmitted: %i" % m.messages_not_transmitted())
