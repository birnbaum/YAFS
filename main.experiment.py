import logging
import random
from typing import Any

import networkx as nx

from pyfogsim import utils
from pyfogsim.core import Simulation
from pyfogsim.application import Application, Message, Sink, Source, Operator
from pyfogsim.placementalgorithm import CloudPlacement, EdgePlacement, GeneticPlacement
from pyfogsim.resource import Cloud, Fog, Sensor, Link4G, LinkCable

from pyfogsim.selection import ShortestPath

from pyfogsim.distribution import UniformDistribution, Distribution
import numpy as np

RANDOM_SEED = 1


def _app(name: str, source_node: Any, sink_node: Any, distribution: Distribution):
    actuator = Sink(f"{name}:sink", node=sink_node)
    message_b = Message(f"{name}:operator->sink", dst=actuator, instructions=50, size=50)
    service_a = Operator(f"{name}:operator", message_out=message_b)
    message_a = Message(f"{name}:source->operator", dst=service_a, instructions=30, size=1000)
    sensor = Source(f"{name}:source", node=source_node, message_out=message_a, distribution=distribution)
    return Application(name=name, source=sensor, operators=[service_a], sink=actuator)


def main(simulated_time, placement):
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    cloud = Cloud("")
    fog_a = Fog("A")
    fog_b = Fog("B")
    sensor_a = Sensor("A")
    sensor_b = Sensor("B")
    sensor_c = Sensor("C")
    sensor_d = Sensor("D")

    G = nx.node_link_graph({
        "directed": False,
        "multigraph": False,
        "graph": {},
        "nodes": [
            {"id": cloud},
            {"id": sensor_a},
            {"id": sensor_b},
            {"id": sensor_c},
            {"id": sensor_d},
            {"id": fog_a},
            {"id": fog_b},
        ],
        "links": [
            {"source": sensor_a, "target": fog_a, "link": Link4G()},
            {"source": sensor_b, "target": fog_a, "link": Link4G()},
            {"source": sensor_c, "target": fog_b, "link": Link4G()},
            {"source": sensor_d, "target": fog_b, "link": Link4G()},
            {"source": fog_a, "target": cloud, "link": LinkCable()},
            {"source": fog_b, "target": cloud, "link": LinkCable()},
        ]
    })

    simulation = Simulation(G, selection=ShortestPath())

    # Application Graph
    apps = []
    distribution = UniformDistribution(min=1, max=40)
    for sensor in [sensor_a, sensor_b, sensor_c, sensor_d]:
        app = _app(f"App{sensor.name}", source_node=sensor, sink_node=cloud, distribution=distribution)
        simulation.deploy_app(app)
        apps.append(app)

    simulation.deploy_placement(placement(apps=apps))

    simulation.run(until=simulated_time, progress_bar=False)
    simulation.stats.print_report(simulated_time)

    print("\nNode Usage:")
    for node in G:
        if node.usage > 0:
            print(f"{node} usage: {node.usage * 100:.1f}%\tconsumption: {node.energy_consumption:.2f} Watt")

    print("\nLink Usage:")
    for source, target, data in G.edges(data=True):
        if data["link"].usage > 0:
            print(f"{source}->{target} usage: {data['link'].usage * 100:.1f}%\tconsumption: {data['link'].energy_consumption:.2f} Watt")

    utils.draw_topology1(G, simulation.node_to_modules, name=placement.__name__)


if __name__ == "__main__":
    logging.basicConfig(format="%(name)s - %(levelname)s - %(message)s", level=logging.INFO)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    main(simulated_time=1000, placement=GeneticPlacement)
    # main(simulated_time=1000, placement=CloudPlacement)
    # main(simulated_time=1000, placement=EdgePlacement)
