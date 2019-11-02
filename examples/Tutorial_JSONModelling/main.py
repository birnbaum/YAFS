"""

    This example

    @author: Isaac Lera & Carlos Guerrero

"""
import json

import networkx as nx

from yafs.core import Simulation
from yafs.application import Application, Message
from yafs.topology import Topology
from yafs.placement import JSONPlacement
from yafs.distribution import *
import numpy as np

from yafs.utils import fractional_selectivity

from .selection_multipleDeploys import DeviceSpeedAwareRouting
from .jsonPopulation import JSONPopulation

import time

RANDOM_SEED = 1


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


def main(simulated_time, experimento, ilpPath):
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

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

    """
    POPULATION algorithm
    """
    dataPopulation = json.load(open(experimento + "usersDefinition.json"))
    pop = JSONPopulation(name="Statical", json=dataPopulation)

    """
    SELECTOR algorithm
    """
    selectorPath = DeviceSpeedAwareRouting()

    """
    SIMULATION ENGINE
    """

    stop_time = simulated_time
    s = Simulation(t, default_results_path=experimento + "Results_%s_%i" % (ilpPath, stop_time))

    # For each deployment the user - population have to contain only its specific sources
    for aName in list(apps.keys()):
        print("Deploying app: ", aName)
        pop_app = JSONPopulation(name="Statical_%s" % aName, json={})
        data = []
        for element in pop.data["sources"]:
            if element["app"] == aName:
                data.append(element)
        pop_app.data["sources"] = data

        s.deploy_app(apps[aName], placement, pop_app, selectorPath)

    s.run(stop_time, test_initial_deploy=False, progress_bar=False)  # TEST to TRUE


if __name__ == "__main__":
    import logging.config
    import os

    pathExperimento = "case/"

    logging.config.fileConfig(os.getcwd() + "/logging.ini")

    start_time = time.time()
    print("Running Partition")
    main(simulated_time=100000, experimento=pathExperimento, ilpPath="")
    print("Running: ILP ")
    main(simulated_time=100000, experimento=pathExperimento, ilpPath="ILP")

    print("Simulation Done")
    print(("\n--- %s seconds ---" % (time.time() - start_time)))
