import logging
import os
import random
from typing import Any

import networkx as nx

from geo.geo import generate_network
from pyfogsim.core import Simulation
from pyfogsim.application import Application, Message, Sink, Source, Operator
from pyfogsim.placement import CloudPlacement, EdgePlacement, GeneticPlacement
from pyfogsim.plot import plot
from pyfogsim.resource import Cloud, Fog, Sensor, Link4G, LinkCable

from pyfogsim.selection import ShortestPath

from pyfogsim.distribution import UniformDistribution, Distribution
import numpy as np

logging.basicConfig(format="%(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("matplotlib").setLevel(logging.WARNING)
random.seed(0)
np.random.seed(0)


def _app(name: str, source_node: Any, sink_node: Any, distribution: Distribution):
    actuator = Sink(f"{name}:sink", node=sink_node)
    message_b = Message(f"{name}:operator->sink", dst=actuator, instructions=50, size=50)
    service_a = Operator(f"{name}:operator", message_out=message_b)
    message_a = Message(f"{name}:source->operator", dst=service_a, instructions=30, size=1000)
    sensor = Source(f"{name}:source", node=source_node, message_out=message_a, distribution=distribution)
    return Application(name=name, source=sensor, operators=[service_a], sink=actuator)


def generate_simple_network() -> nx.Graph:
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
            #            {"id": sensor_b},
            #            {"id": sensor_c},
            #            {"id": sensor_d},
            {"id": fog_a},
            #            {"id": fog_b},
        ],
        "links": [
            {"source": sensor_a, "target": fog_a, "link": Link4G()},
            #            {"source": sensor_b, "target": fog_a, "link": Link4G()},
            #            {"source": sensor_c, "target": fog_b, "link": Link4G()},
            #            {"source": sensor_d, "target": fog_b, "link": Link4G()},
            {"source": fog_a, "target": cloud, "link": LinkCable()},
            #            {"source": fog_b, "target": cloud, "link": LinkCable()},
        ]
    })
    return G


def setup_simulation(G):
    simulation = Simulation(G, selection=ShortestPath())
    # Application Graph
    distribution = UniformDistribution(min=1, max=40)
    cloud = next(n for n in G.nodes() if isinstance(n, Cloud))
    for sensor in [n for n in G.nodes() if isinstance(n, Sensor)]:
        app = _app(f"App{sensor.name}", source_node=sensor, sink_node=cloud, distribution=distribution)
        simulation.deploy_app(app)
    return simulation


def main(network, simulated_time, placement, out_dir):
    simulation = setup_simulation(network)
    simulation.deploy_placement(placement(apps=simulation.apps))
    simulation.run(until=simulated_time, progress_bar=False)
    simulation.stats.print_report(simulated_time)

    print("\nNode Usage:")
    for node in simulation.network:
        if node.usage > 0:
            print(f"usage: {node.usage * 100:.1f}%\tconsumption: {node.energy_consumption:.2f} Watt")

    plot(simulation.network, out_path=f"{out_dir}/load.png", node_load=True, edge_load=True)

    # print("\nLink Usage:")
    # for source, target, data in simulation.network.edges(data=True):
    #     if data["link"].usage > 0:
    #         print(f"{source}->{target} usage: {data['link'].usage * 100:.1f}%\tconsumption: {data['link'].energy_consumption:.2f} Watt")

    # utils.draw_topology1(simulation.network, simulation.node_to_modules, name=placement.__name__)


if __name__ == "__main__":
    SIMULATED_TIME = 1000
    N_SENSORS = 300
    PLACEMENTS = [
        CloudPlacement,
        EdgePlacement,
    #    GeneticPlacement,
    ]
    experiment_name = f"experiment_{N_SENSORS}_sensors"
    os.makedirs(experiment_name, exist_ok=True)

    network = generate_network(N_SENSORS)
    plot(network, out_path=f"{experiment_name}/city.png", plot_map=True, plot_labels=True)
    plot(network, out_path=f"{experiment_name}/topology.png", plot_cloud_fog_edges=False)

    for placement in PLACEMENTS:
        out_dir = f"{experiment_name}/{placement.__name__}_{SIMULATED_TIME}"
        os.makedirs(out_dir, exist_ok=True)
        main(network=generate_network(N_SENSORS), simulated_time=SIMULATED_TIME, placement=placement, out_dir=out_dir)
