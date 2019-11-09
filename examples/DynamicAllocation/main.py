"""This example implements a simple evolutive deployment of fog devices to study the latency of two applications.
There is a comparison between:
- One application has a cloud placement
- Another one (equivalent application) has an evolutive deployement on fog devices
"""

import copy
import operator
import random
import time
import logging

import networkx as nx
import numpy as np

from yafs.application import Application, Message
from yafs.core import Simulation
from yafs.distribution import DeterministicDistribution, DeterministicDistributionStartPoint
from yafs.population import Evolutive, Statical
from yafs.selection import BroadPath, CloudPathRR
from yafs.topology import Topology

RANDOM_SEED = 1

logger = logging.getLogger(__name__)


def create_application(name):
    a = Application(name=name)

    a.set_modules([{"Generator": {"Type": Application.TYPE_SOURCE}}, {"Actuator": {"Type": Application.TYPE_SINK}}])

    m_egg = Message("M.Action", "Generator", "Actuator", instructions=100, size=10)
    a.add_source_message(m_egg)
    return a


# @profile
def main(simulated_time):
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    t = Topology()
    t.G = nx.read_graphml("Euclidean.graphml")

    ls = list(t.G.nodes)
    li = {x: int(x) for x in ls}
    nx.relabel_nodes(t.G, li, False)  # Transform str-labels to int-labels

    print("Nodes: %i" % len(t.G.nodes()))
    print("Edges: %i" % len(t.G.edges()))
    # MANDATORY fields of a link
    # Default values =  {"BW": 1, "PR": 1}
    valuesOne = dict(zip(t.G.edges(), np.ones(len(t.G.edges()))))

    nx.set_edge_attributes(t.G, name="BW", values=valuesOne)
    nx.set_edge_attributes(t.G, name="PR", values=valuesOne)

    centrality = nx.betweenness_centrality(t.G)
    nx.set_node_attributes(t.G, name="centrality", values=centrality)

    sorted_clustMeasure = sorted(list(centrality.items()), key=operator.itemgetter(1), reverse=True)

    top20_devices = sorted_clustMeasure[:20]
    main_fog_device = copy.copy(top20_devices[0][0])

    print("-" * 20)
    print("Top 20 centralised nodes:")
    for item in top20_devices:
        print(item)
    print("-" * 20)
    """
    APPLICATION
    """
    app1 = create_application("app1")
    app2 = create_application("app2")

    """
    PLACEMENT algorithm
    """
    # There are not modules to place.
    placement = NoPlacementOfModules("NoPlacement")

    """
    POPULATION algorithm
    """
    number_generators = int(len(t.G) * 0.1)
    print(number_generators)
    dDistribution = DeterministicDistributionStartPoint(3000, 300, name="Deterministic")
    dDistributionSrc = DeterministicDistribution(name="Deterministic", time=10)
    pop1 = Evolutive(top20_devices, number_generators, name="top", activation_dist=dDistribution)
    pop1.set_sink_control({"app": app1.name, "number": 1, "module": app1.sink_modules})
    pop1.set_src_control({"number": 1, "message": app1.get_message["M.Action"], "distribution": dDistributionSrc})

    pop2 = Statical(number_generators, name="Statical")
    pop2.set_sink_control({"id": main_fog_device, "number": number_generators, "module": app2.sink_modules})

    pop2.set_src_control({"number": 1, "message": app2.get_message["M.Action"], "distribution": dDistributionSrc})

    # In addition, a source includes a distribution function:

    """--
    SELECTOR algorithm
    """
    selectorPath1 = BroadPath()
    selectorPath2 = CloudPathRR()

    """
    SIMULATION ENGINE
    """

    s = Simulation(t, default_results_path="Results_%s_singleApp1" % (simulated_time))
    s.deploy_app(app1, placement, pop1, selectorPath1)
    # s.deploy_app(app2, placement, pop2,  selectorPath2)

    s.run(simulated_time, progress_bar=False)
    # s.draw_allocated_topology() # for debugging


if __name__ == "__main__":
    import logging.config
    import os

    logging.config.fileConfig(os.getcwd() + "/logging.ini")

    start_time = time.time()

    main(simulated_time=12000)

    print(("\n--- %s seconds ---" % (time.time() - start_time)))
