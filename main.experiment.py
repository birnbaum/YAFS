import logging
import random

from yafs import utils
from yafs.core import Simulation
from yafs.application import Application, Message, Module
from yafs.placement import CloudPlacement
from yafs.population import StaticPopulation

from yafs.selection import ShortestPath
from yafs.topology import Topology, load_yafs_json

from yafs.distribution import DeterministicDistribution
import time
import numpy as np

RANDOM_SEED = 1


def create_application(name: str = "SimpleApp"):
    sensor = Module("sensor", is_source=True)
    service_a = Module("service_a", is_source=True)
    actuator = Module("actuator", is_source=True)
    a = Application(name=name, modules=[sensor, service_a, actuator])

    # Messages among MODULES (AppEdge in iFogSim)
    message_a = Message("M.A", src=sensor, dst=service_a, instructions=20 * 10 ^ 6, size=1000)
    message_b = Message("M.B", src=service_a, dst=actuator, instructions=30 * 10 ^ 6, size=500)

    # Defining which messages will be dynamically generated # the generation is controlled by Population algorithm
    a.add_source_messages(message_a)

    service_a.add_service(message_a, message_b)

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

    app1 = create_application("App1")
    app2 = create_application("App2")

    placement = CloudPlacement("onCloud")  # it defines the deployed rules: module-device
    placement.scaleService({"service_a": 1})

    distribution = DeterministicDistribution(name="Deterministic", time=100)
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
    population.set_sink_control({"model": "actuator-device", "number": 1, "module": "actuator"})  # TODO Sink hardcoded
    population.set_src_control({"model": "sensor-device",
                                "number": 1,
                                "message": app1.messages["M.A"],
                                "distribution": distribution})

    selection = ShortestPath()

    simulation = Simulation(t)
    simulation.deploy_app(app1, placement=placement, population=population, selection=selection)
    simulation.deploy_app(app2, placement=placement, population=population, selection=selection)
    simulation.run(until=simulated_time, results_path="results")
    utils.draw_topology(t, simulation.get_alloc_entities())

    simulation.stats.print_report(1000, topology=t, time_loops=[["M.A", "M.B"]])


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.DEBUG)
    start_time = time.time()
    main(simulated_time=1000)
    print(("\n--- %s seconds ---" % (time.time() - start_time)))
