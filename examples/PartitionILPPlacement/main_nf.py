import json
import time

import networkx as nx

from yafs.application import Application, Message
from yafs.core import Simulation
from yafs.distribution import *
from yafs.placement import JSONPlacement
from yafs.population import JSONPopulation
from yafs.selection import DeviceSpeedAwareRouting2
from yafs.topology import Topology
from yafs.utils import fractional_selectivity


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
                a.add_source_messages(ms[message["name"]])

        # print "Total mensajes creados %i" %len(ms.keys())
        for idx, message in enumerate(app["transmission"]):
            if "message_out" in list(message.keys()):
                a.add_service_module(message["module"], ms[message["message_in"]], ms[message["message_out"]], fractional_selectivity, threshold=1.0)
            else:
                a.add_service_module(message["module"], ms[message["message_in"]])

        applications[app["name"]] = a

    return applications


def main(simulated_time, experimento, ilpPath, it):
    """
    TOPOLOGY from a json
    """
    t = Topology()
    dataNetwork = json.load(open(experimento + "networkDefinition.json"))
    t.load(dataNetwork)
    nx.write_gefx(t.G, "network.gexf")

    """
    APPLICATION
    """
    dataApp = json.load(open(experimento + "appDefinition.json"))
    apps = create_applications_from_json(dataApp)
    # for app in apps:
    #  print apps[app]

    """
    PLACEMENT algorithm
    """
    placementJson = json.load(open(experimento + "allocDefinition%s.json" % ilpPath))
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
    dataPopulation = json.load(open(experimento + "usersDefinition.json"))
    pop = JSONPopulation(name="Statical", json=dataPopulation, iteration=it)

    """
    SELECTOR algorithm
    """
    selectorPath = DeviceSpeedAwareRouting2()

    """
    SIMULATION ENGINE
    """

    stop_time = simulated_time
    s = Simulation(t, default_results_path=experimento + "Results_%s_%i_%i" % (ilpPath, stop_time, it))

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

    s.run(stop_time, test_initial_deploy=False, progress_bar=False)  # TEST to TRUE

    ## Enrouting information
    # print "Values"
    # print selectorPath.cache.values()

    # failurefilelog.close()

    # #CHECKS
    # print s.topology.G.nodes
    # s.print_debug_assignaments()


if __name__ == "__main__":
    # import logging.config
    import os

    pathExperimento = "exp_rev/"
    pathExperimento = "/home/uib/src/YAFS/src/examples/PartitionILPPlacement/exp_rev/"

    print(os.getcwd())
    # logging.config.fileConfig(os.getcwd()+'/logging.ini')
    for i in range(50):
        start_time = time.time()
        random.seed(i)
        np.random.seed(i)
        # 1000000
        print("Running Partition")
        main(simulated_time=1000000, experimento=pathExperimento, ilpPath="", it=i)
        print(("\n--- %s seconds ---" % (time.time() - start_time)))
        start_time = time.time()
        print("Running: ILP ")
        main(simulated_time=1000000, experimento=pathExperimento, ilpPath="ILP", it=i)
        print(("\n--- %s seconds ---" % (time.time() - start_time)))

    print("Simulation Done")
