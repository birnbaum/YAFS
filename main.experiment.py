import logging
import random

import networkx as nx

from pyfogsim import utils
from pyfogsim.core import Simulation
from pyfogsim.application import Application, Message, Sink, Source, Operator
from pyfogsim.placement import CloudPlacement, EdgePlacement

from pyfogsim.selection import ShortestPath

from pyfogsim.distribution import UniformDistribution, Distribution
import numpy as np

RANDOM_SEED = 1


def _app(name: str, source_node: str, sink_node: str, distribution: Distribution):
    actuator = Sink(f"{name}:sink", node=sink_node)
    message_b = Message(f"{name}:operator->sink", dst=actuator, instructions=50, size=50)
    service_a = Operator(f"{name}:operator", message_out=message_b)
    message_a = Message(f"{name}:source->operator", dst=service_a, instructions=30, size=1000)
    sensor = Source(f"{name}:source", node=source_node, message_out=message_a, distribution=distribution)
    return Application(name=name, source=sensor, operators=[service_a], sink=actuator)


def main(simulated_time, placement):
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    G = nx.node_link_graph({
        "directed": False,
        "multigraph": False,
        "graph": {},
        "nodes": [
            {"id": "cloud", "IPT": 60, "RAM": 10**6, "WATT": 20.0},
            {"id": "sensor1", "IPT": 10, "RAM": 4000, "WATT": 40.0},
            {"id": "sensor2", "IPT": 10, "RAM": 4000, "WATT": 40.0},
            {"id": "sensor3", "IPT": 10, "RAM": 4000, "WATT": 40.0},
            {"id": "sensor4", "IPT": 10, "RAM": 4000, "WATT": 40.0},
            {"id": "fog1", "IPT": 10, "RAM": 4000, "WATT": 40.0},
            {"id": "fog2", "IPT": 10, "RAM": 4000, "WATT": 40.0},
            {"id": "actuator1", "IPT": 10, "RAM": 4000, "WATT": 40.0},
            {"id": "actuator2", "IPT": 10, "RAM": 4000, "WATT": 40.0},
        ],
        "links": [
            {"source": "sensor1", "target": "fog1", "BW": 300, "PR": 1},
            {"source": "sensor2", "target": "fog1", "BW": 300, "PR": 1},
            {"source": "sensor3", "target": "fog2", "BW": 300, "PR": 1},
            {"source": "sensor4", "target": "fog2", "BW": 300, "PR": 1},
            {"source": "fog1", "target": "cloud", "BW": 500, "PR": 10},
            {"source": "fog1", "target": "actuator1", "BW": 500, "PR": 10},
            {"source": "fog1", "target": "actuator2", "BW": 500, "PR": 10},
            {"source": "fog2", "target": "cloud", "BW": 500, "PR": 10},
            {"source": "fog2", "target": "actuator1", "BW": 500, "PR": 10},
            {"source": "fog2", "target": "actuator2", "BW": 500, "PR": 10},
            {"source": "cloud", "target": "actuator1", "BW": 500, "PR": 10},
            {"source": "cloud", "target": "actuator2", "BW": 500, "PR": 10},
        ]
    })

    simulation = Simulation(G, selection=ShortestPath())

    # Application Graph
    apps = []
    distribution = UniformDistribution(min=5, max=50)
    for i in range(1, 5):
        app = _app(f"App{i}", source_node=f"sensor{i}", sink_node=f"actuator{(i-1)%2+1}", distribution=distribution)
        simulation.deploy_app(app)
        apps.append(app)

    simulation.deploy_placement(placement(apps=apps))

    simulation.run(until=simulated_time, progress_bar=False)
    simulation.stats.print_report(simulated_time)
    utils.draw_topology(G, simulation.node_to_modules, name=placement.__name__)


if __name__ == "__main__":
    logging.basicConfig(format="%(name)s - %(levelname)s - %(message)s", level=logging.INFO)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    main(simulated_time=10000, placement=CloudPlacement)
    main(simulated_time=10000, placement=EdgePlacement)
