import logging
import random
from typing import Tuple, List

import networkx as nx

from yafs import utils
from yafs.core import Simulation
from yafs.application import Application, Message, Module, Sink, Source, Operator
from yafs.placement import CloudPlacement

from yafs.selection import ShortestPath
from yafs.topology import Topology

from yafs.distribution import UniformDistribution
import time
import numpy as np

RANDOM_SEED = 1


def main(simulated_time):
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    G = nx.node_link_graph({
        "directed": False,
        "multigraph": False,
        "graph": {},
        "nodes": [
            {"id": "cloud", "IPT": 10**6, "RAM": 10**6, "WATT": 20.0},
            {"id": "sensor1", "IPT": 1000, "RAM": 4000, "WATT": 40.0},
            {"id": "sensor2", "IPT": 1000, "RAM": 4000, "WATT": 40.0},
            {"id": "sensor3", "IPT": 1000, "RAM": 4000, "WATT": 40.0},
            {"id": "sensor4", "IPT": 1000, "RAM": 4000, "WATT": 40.0},
            {"id": "fog1", "IPT": 5000, "RAM": 4000, "WATT": 40.0},
            {"id": "fog2", "IPT": 5000, "RAM": 4000, "WATT": 40.0},
            {"id": "actuator1", "IPT": 1000, "RAM": 4000, "WATT": 40.0},
            {"id": "actuator2", "IPT": 1000, "RAM": 4000, "WATT": 40.0},
        ],
        "links": [
            {"source": "sensor1", "target": "fog1", "BW": 1, "PR": 10},
            {"source": "sensor1", "target": "fog2", "BW": 1, "PR": 10},
            {"source": "sensor2", "target": "fog1", "BW": 1, "PR": 10},
            {"source": "sensor2", "target": "fog2", "BW": 1, "PR": 10},
            {"source": "sensor3", "target": "fog1", "BW": 1, "PR": 10},
            {"source": "sensor3", "target": "fog2", "BW": 1, "PR": 10},
            {"source": "sensor4", "target": "fog1", "BW": 1, "PR": 10},
            {"source": "sensor4", "target": "fog2", "BW": 1, "PR": 10},
            {"source": "fog1", "target": "cloud", "BW": 5, "PR": 10},
            {"source": "fog1", "target": "actuator1", "BW": 5, "PR": 10},
            {"source": "fog1", "target": "actuator2", "BW": 5, "PR": 10},
            {"source": "fog2", "target": "cloud", "BW": 5, "PR": 10},
            {"source": "fog2", "target": "actuator1", "BW": 5, "PR": 10},
            {"source": "fog2", "target": "actuator2", "BW": 5, "PR": 10},
            {"source": "cloud", "target": "actuator1", "BW": 5, "PR": 10},
            {"source": "cloud", "target": "actuator2", "BW": 5, "PR": 10},
        ]
    })
    t = Topology(G)
    utils.draw_topology(t)

    distribution = UniformDistribution(min=1, max=100)

    # Application Graph
    actuator = Sink("actuator", node="actuator1")
    message_b = Message("M.B", dst=actuator, instructions=30 * 10 ^ 6, size=500)
    service_a = Operator("service_a", message_out=message_b)
    message_a = Message("M.A", dst=service_a, instructions=20 * 10 ^ 6, size=1000)
    sensor = Source("sensor", node="sensor1", message_out=message_a, distribution=distribution)

    simulation = Simulation(t, selection=ShortestPath())

    app1 = Application(name="App1", source=sensor, operators=[service_a], sink=actuator)

    simulation.deploy_app(app1)

    simulation.deploy_placement(CloudPlacement(apps=[app1]))

    simulation.run(until=simulated_time, results_path="results", progress_bar=False)
    simulation.stats.print_report(simulated_time)
    utils.draw_topology(t, simulation.node_to_modules)


if __name__ == "__main__":
    logging.basicConfig(format="%(name)s - %(levelname)s - %(message)s", level=logging.DEBUG)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    start_time = time.time()
    main(simulated_time=1000)
    print(("\n--- %s seconds ---" % (time.time() - start_time)))
