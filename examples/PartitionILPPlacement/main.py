"""Availability-aware Service Placement Policy in Fog Computing Based on Graph Partitions

https://ieeexplore.ieee.org/document/8588297
"""

import json
import logging
import os
import time

import networkx as nx

from yafs.application import Application, Message
from yafs.core import Simulation
from yafs.distribution import *
from yafs.placement import JSONPlacement
from yafs.population import JSONPopulation
from yafs.selection import DeviceSpeedAwareRouting2
from yafs.topology import Topology

logger = logging.getLogger(__name__)


def create_applications_from_json(data):
    applications = {}
    for app in data:
        a = Application(name=app["name"])
        modules = [{"None": {"Type": Application.TYPE_SOURCE}}]
        for module in app["module"]:
            modules.append({module["name"]: {"RAM": module["RAM"], "Type": Application.TYPE_MODULE}})
        a.set_modules(modules)

        ms = {}
        for message in app["message"]:
            # print "Creando mensaje: %s" %message["name"]
            ms[message["name"]] = Message(message["name"], message["s"], message["d"], instructions=message["instructions"], size=message["bytes"])
            if message["s"] == "None":
                a.add_source_message(ms[message["name"]])

        # print "Total mensajes creados %i" %len(ms.keys())
        for idx, message in enumerate(app["transmission"]):
            if "message_out" in list(message.keys()):
                a.add_service_module(message["module"], ms[message["message_in"]], ms[message["message_out"]])
            else:
                a.add_service_module(message["module"], ms[message["message_in"]])

        applications[app["name"]] = a

    return applications


def main(simulated_time, experiment_path, ilpPath, it):
    """
    TOPOLOGY from a json
    """
    t = Topology()
    dataNetwork = json.load(open(experiment_path + "networkDefinition.json"))
    t.load(dataNetwork)
    nx.write_gefx(t.G, "network.gexf")

    """
    APPLICATION
    """
    dataApp = json.load(open(experiment_path + "appDefinition.json"))
    apps = create_applications_from_json(dataApp)
    # for app in apps:
    #  print apps[app]

    """
    PLACEMENT algorithm
    """
    placementJson = json.load(open(experiment_path + "allocDefinition%s.json" % ilpPath))
    placement = JSONPlacement(name="Placement", json=placementJson)

    ### Placement histogram

    # listDevices =[]
    # for item in placementJson["initialAllocation"]:
    #     listDevices.append(item["id_resource"])
    # import matplotlib.pyplot as plt
    # print listDevices
    # print np.histogram(listDevices,bins=range(101))
    # plt.hist(listDevices, bins=100)  # arguments are passed to np.histogram
    # plt.title("Placement Histogram")
    # plt.show()
    ## exit()
    """
    POPULATION algorithm
    """
    dataPopulation = json.load(open(experiment_path + "usersDefinition.json"))
    pop = JSONPopulation(name="Statical", json=dataPopulation, iteration=it)

    """
    SELECTOR algorithm
    """
    selectorPath = DeviceSpeedAwareRouting2()

    """
    SIMULATION ENGINE
    """
    s = Simulation(t, default_results_path=os.path.join(experiment_path, f"Results_RND_FAIL_{ilpPath}_{simulated_time}_{it}"))

    # Node Failure Generator
    # centrality = np.load(experimento+"centrality.npy")
    rnd = np.load(os.path.join(experiment_path, "random.npy"))
    distribution = DeterministicDistributionStartPoint(name="NodeFailureDistribution", time=10000, start=1)
    logfile = os.path.join(experiment_path, f"Failure_{ilpPath}_{simulated_time}.csv")
    with open(logfile, "w") as stream:
        stream.write("node,module,time\n")
    s.deploy_node_failure_generator(nodes=rnd, distribution=distribution, logfile=logfile)

    # For each deployment the user - population have to contain only its specific sources
    for aName in list(apps.keys()):
        print("Deploying app: ", aName)
        pop_app = JSONPopulation(name="Statical_%s" % aName, json={}, iteration=it)
        data = []
        for element in pop.data["sources"]:
            if element["app"] == aName:
                data.append(element)
        pop_app.data["sources"] = data

        s.deploy_app(apps[aName], placement, pop_app, selectorPath)

    s.run(simulated_time, progress_bar=False)

    ## Enrouting information
    # print "Values"
    # print selectorPath.cache.values()

    # #CHECKS
    # print s.G.nodes
    # s.print_debug_assignaments()


if __name__ == "__main__":
    experiment_path = "exp_rev"
    for i in range(50):
        random.seed(i)
        np.random.seed(i)

        start_time = time.time()
        print("Running Partition")
        main(simulated_time=1000000, experiment_path=experiment_path, ilpPath="", it=i)
        print(("\n--- %s seconds ---" % (time.time() - start_time)))

        start_time = time.time()
        print("Running: ILP ")
        main(simulated_time=1000000, experiment_path=experiment_path, ilpPath="ILP", it=i)
        print(("\n--- %s seconds ---" % (time.time() - start_time)))
