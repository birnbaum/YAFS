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


def create_application(name: str = "SimpleApp") -> Tuple[Application, List[Message]]:
    sensor = Source("sensor")
    service_a = Operator("service_a")
    actuator = Sink("actuator")
    message_a = Message("M.A", src=sensor, dst=service_a, instructions=20 * 10 ^ 6, size=1000)
    message_b = Message("M.B", src=service_a, dst=actuator, instructions=30 * 10 ^ 6, size=500)
    service_a.add_service(message_a, message_b)  # TODO Weird back-referencing objects
    application = Application(name=name, source=sensor, operators=[service_a], sink=actuator)

    return application, [message_a]


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

    app1, source_messages1 = create_application("App1")
    # app2, source_messages2 = create_application("App2")

    simulation = Simulation(t)

    selection = ShortestPath()
    simulation.deploy_app(app1, selection=selection)

    distribution = UniformDistribution(min=1, max=100)

    simulation.deploy_source(app1, node_id="sensor1", message=source_messages1[0], distribution=distribution)
    simulation.deploy_sink(app1, node_id="actuator1", module_name="actuator")

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
