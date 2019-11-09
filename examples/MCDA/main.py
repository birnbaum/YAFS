# TODO This example has >2000 files in it

import collections
import json
import logging.config
import os
import pickle
import random
import time

import networkx as nx
import numpy as np

from examples.MCDA.MCDAPathSelectionNPlacement import MCDARoutingAndDeploying
from examples.MCDA.WAPathSelectionNPlacement import WARoutingAndDeploying
from yafs.application import Application, Message
from yafs.core import Simulation, ExponentialDistribution
from yafs.placement import JSONPlacement, JSONPlacementOnlyCloud
from yafs.population import DynamicPopulation
from yafs.topology import Topology


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


def main(simulated_time, path, pathResults, case, failuresON, it, idcloud):
    """
    TOPOLOGY from a json
    """
    t = Topology()
    dataNetwork = json.load(open(path + "networkDefinition.json"))
    t.load(dataNetwork)
    nx.write_gefx(t.G, path + "network.gexf")
    # t = loadTopology(path + 'test_GLP.gml')

    """
    APPLICATION
    """
    dataApp = json.load(open(path + "appDefinition.json"))
    apps = create_applications_from_json(dataApp)
    # for app in apps:
    #  print apps[app]

    """
    PLACEMENT algorithm
    """
    # In our model only initial cloud placements are enabled
    placementJson = json.load(open(path + "allocDefinition%s.json" % case))
    if case == "MCDA":
        # We modify this class to enable only cloud placement
        # Note: In this json, all service are assigned to the cloud device and other devices/nodes
        placement = JSONPlacementOnlyCloud(name="MCDA-Placement", idcloud=idcloud, json=placementJson)
    else:
        placement = JSONPlacement(name="Placement", json=placementJson)

    """
    SELECTOR and Deploying algorithm
    """
    if case == "MCDA":
        selectorPath = MCDARoutingAndDeploying(path=path, pathResults=pathResults, idcloud=idcloud)
    else:
        selectorPath = WARoutingAndDeploying(path=path, pathResults=pathResults, idcloud=idcloud)

    """
    SIMULATION ENGINE
    """

    stop_time = simulated_time
    s = Simulation(t, default_results_path=pathResults + "Results_%s_%i_%i" % (case, stop_time, it))

    # For each deployment the user - population have to contain only its specific sources

    """
    POPULATION algorithm
    """
    dataPopulation = json.load(open(path + "usersDefinition.json"))

    # Each application has an unique population politic
    # For the original json, we filter and create a sub-list for each app politic
    for aName in list(apps.keys()):
        data = []
        for element in dataPopulation["sources"]:
            if element["app"] == aName:
                data.append(element)

        distribution = ExponentialDistribution(name="Exp", lambd=random.randint(100, 1000), seed=int(aName) * 100 + it)
        pop_app = DynamicPopulation(name="Dynamic_%s" % aName, data=data, iteration=it, activation_dist=distribution)
        s.deploy_app(apps[aName], placement, pop_app, selectorPath)

    logging.info(" Performing simulation: %s %i " % (case, it))
    s.run(stop_time, progress_bar=False)  # TEST to TRUE

    ## Enrouting information
    # print "Values"
    # print selectorPath.cache.values()

    # if failuresON:
    #     failurefilelog.close()
    # #CHECKS
    # print s.topology.G.nodes
    s.print_debug_assignaments()

    # Genera un fichero GEPHI donde se marcan los nodos con usuarios (userposition) y los nodos con servicios desplegados (services)
    print("----")
    l = s.node_to_modules
    userposition = {}
    deploymentservices = {}
    for k in l:
        cu = 0
        cd = 0
        for item in l[k]:
            if "None" in item:
                cu += 1
            else:
                cd += 1

        deploymentservices[k] = cd
        userposition[k] = cu

    nx.set_node_attributes(s.topology.G, values=deploymentservices, name="services")
    nx.set_node_attributes(s.topology.G, values=userposition, name="userposition")
    # nx.write_gexf(s.topology.G, "network_assignments.gexf")

    print(selectorPath.dname)
    f = open(selectorPath.dname + "/file_alloc_entities_%s_%i_%i.pkl" % (case, stop_time, it), "wb")
    pickle.dump(l, f)
    f.close()

    print("----")
    controlServices = selectorPath.controlServices
    # print controlServices
    attEdges = collections.Counter()
    for k in controlServices:
        path = controlServices[k][0]
        for i in range(len(path) - 1):
            edge = (path[i], path[i + 1])
            attEdges[edge] += 1

    dl = {}
    for item in attEdges:
        dl[item] = {"W": attEdges[item]}
    nx.set_edge_attributes(s.topology.G, values=dl)

    nx.write_gexf(s.topology.G, selectorPath.dname + "/network_assignments_%s_%i_%i.gexf" % (case, stop_time, it))

    f = open(selectorPath.dname + "/file_assignments_%s_%i_%i.pkl" % (case, stop_time, it), "wb")
    pickle.dump(controlServices, f)
    f.close()


logging.config.fileConfig(os.getcwd() + "/logging.ini")
if __name__ == "__main__":

    # NOTE: ABSOLUTE PATH TO JSON FILES ACCORDING TO THE EXECUTION-PLACE
    # We simplify the path update in our experimentation to external servers (it's a bit precarious but functional)
    runpath = os.getcwd()
    print(runpath)
    if "/home/uib/" in runpath:
        pathExperimento = "/home/uib/src/YAFS/src/examples/MCDA/exp1/"
    else:
        pathExperimento = "exp1/"
    #####

    print("PATH EXPERIMENTO: ", pathExperimento)
    nSimulations = 1
    timeSimulation = 10000
    datestamp = time.strftime("%Y%m%d")
    dname = pathExperimento + "results_" + datestamp + "/"
    os.makedirs(dname, exist_ok=True)

    # Multiple simulations
    for i in range(nSimulations):
        start_time = time.time()

        random.seed(i)
        np.random.seed(i)

        logging.info("Running MCDA - ELECTRE - %s" % pathExperimento)

        # Note: Some simulation parameters have to be defined inside of the main function
        # - Distribution lambdas
        # - Device id cloud
        # - Random seed for users
        main(simulated_time=timeSimulation, path=pathExperimento, pathResults=dname, case="MCDA", failuresON=False, it=i, idcloud=153)

        random.seed(i)
        np.random.seed(i)

        logging.info("Running WA - %s" % pathExperimento)
        main(simulated_time=timeSimulation, path=pathExperimento, pathResults=dname, case="WA", failuresON=False, it=i, idcloud=153)

        print(("\n--- %s seconds ---" % (time.time() - start_time)))

    print("Simulation Done")

### NOTAS:
# Deberia de cambiar la posicion en cada simulation
