import json
import logging
import logging.config
import os
import time
from collections import Counter, defaultdict

import networkx as nx

from yafs.application import Application, Message, Module
from yafs.core import Simulation
from yafs.distribution import *
from yafs.placement import JSONPlacement
from yafs.population import Population
from yafs.selection import Selection
from yafs.topology import Topology, load_yafs_json

logger = logging.getLogger(__name__)


class CustomStrategy:
    def __init__(self, pathResults):
        self.activations = 0
        self.pathResults = pathResults

    def summarize(self):
        print("Number of evolutions %i" % self.activations)

    def deploy_module(self, sim, service, idtopo):
        app_name = service[0 : service.index("_")]
        app = sim.deployments[app_name].application
        services = app.services
        sim.deploy_operator(app_name, service, services[service], [idtopo])

    def is_already_deployed(self, sim, service_name, idtopo):
        app_name = service_name[0 : service_name.index("_")]

        all_des = []
        for k, v in list(sim.alloc_DES.items()):
            if v == idtopo:
                all_des.append(k)

        # Clearing other related structures
        for des in sim.alloc_module[app_name][service_name]:
            if des in all_des:
                return True

    def get_current_services(self, sim):
        """ returns a dictionary with name_service and a list of node where they are deployed
        example: defaultdict(<type 'list'>, {u'2_19': [15], u'3_22': [5]})
        """
        current_services = sim.node_to_modules
        current_services = dict((k, v) for k, v in current_services.items() if len(v) > 0)
        deployed_services = defaultdict(list)
        for k, v in current_services.items():
            for service_name in v:
                if not "None" in service_name:  # [u'2#2_19']
                    deployed_services[service_name[service_name.index("#") + 1 :]].append(k)
        return deployed_services

    def __call__(self, sim, routing, case, stop_time, it):

        self.activations += 1
        routing.invalid_cache_value = True

        # sim.print_debug_assignaments()
        # routing.invalid_cache_value = True

        # Current utilization of services
        services = defaultdict(list)
        for k, v in routing.controlServices.items():
            # print k[1]
            services[k[1]].append(v[0])
            # print v #[(node_src, service)] = (path, des)
        print("Current utilization of services")
        print(services)
        print("-" * 30)

        # Current deployed services
        print("Current deployed services")
        current_services = self.get_current_services(sim)
        print(current_services)
        print("-" * 30)

        # Deployed services not used
        services_not_used = defaultdict(list)
        for k in current_services:
            if not k in list(services.keys()):
                # This service is not used
                None
            else:
                for service in current_services[k]:
                    found = False
                    for path in services[k]:
                        if path[-1] == service:
                            found = True
                            break
                    # endfor
                    if not found:
                        services_not_used[k].append(service)

        print("-- Servicios no usados")
        print(services_not_used)
        print("-" * 30)

        # # We remove all the services not used but they have been called in a previous step
        # for service_name,nodes in services_not_used.iteritems():
        #     for node in nodes:
        #         app_name = service_name[0:service_name.index("_")]
        #         print " + Removing module: %s from node: %i"%(service_name,node)
        #         sim.undeploy_module(app_name,service_name,node)

        # por cada servicio se toma una decision:
        # clonarse
        for service in services:
            # TODO other type of operation
            if random.random() < 1.0:
                # clonning
                clones = len(services[service])  # numero de veces que se solicita
                for clon in range(clones):
                    path = services[service][clon]
                    # path[-1] is the current location of the service
                    if len(path) > 2:
                        nextLocation = path[-2]
                        # TODO capacity control
                        if not self.is_already_deployed(sim, service, nextLocation):
                            self.deploy_module(sim, service, nextLocation)

        # entities = sim.alloc_entities
        # f = open(self.pathResults + "/file_alloc_entities_%s_%i_%i_%i.pkl" % (case, stop_time, it,self.activations), "wb")
        # pickle.dump(entities, f)
        # f.close()

        # if self.activations==2:
        #     sim.print_debug_assignaments()
        #     print "ESTOOOO "
        #
        #     exit()


class DynamicPopulation(Population):
    """
    We launch one user by invocation
    """

    def __init__(self, data, iteration, **kwargs):
        super(DynamicPopulation, self).__init__(**kwargs)
        self.data = data
        self.it = iteration
        self.userOrderInputByInvocation = []
        logger.info(" Initializating dynamic population: %s" % self.name)

    """
    In userOrderInputByInvocation, we create the user apparition sequence
    """

    def initial_allocation(self, simulation, application):
        size = len(self.data)
        self.userOrderInputByInvocation = random.sample(list(range(size)), size)

    """
    In each invocation, we launch one user
    """

    def run(self, sim):
        if len(self.userOrderInputByInvocation) > 0:
            idx = self.userOrderInputByInvocation.pop(0)
            item = self.data[idx]

            app_name = item["app"]
            idtopo = item["id_resource"]
            lambd = item["lambda"]

            logger.info("Launching user %i (app: %s), in node: %i, at time: %i " % (item["id_resource"], app_name, idtopo, sim.env.now))

            app = sim.deployments[app_name].application
            msg = app.messages[item["message"]]

            # A basic creation of the seed: unique for each user and different in each simulation repetition
            seed = item["id_resource"] * 1000 + item["lambda"] + self.it

            dDistribution = ExponentialDistribution(name="Exp", lambd=lambd, seed=seed)

            sim._deploy_source(app_name, node=idtopo, message=msg, distribution=dDistribution)


class DeviceSpeedAwareRouting(Selection):
    def __init__(self):
        self.cache = {}
        self.invalid_cache_value = True

        self.controlServices = {}
        # key: a service
        # value : a list of idDevices
        super(DeviceSpeedAwareRouting, self).__init__()

    def compute_BEST_DES(self, node_src, alloc_DES, sim, DES_dst, message):
        min_path = []
        best_des = None
        for dev in DES_dst:
            node_dst = alloc_DES[dev]
            try:
                path = list(nx.shortest_path(sim.G, source=node_src, target=node_dst))
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                logger.warning("There is no path between two nodes: %s - %s " % (node_src, node_dst))
                break
            if best_des is None or len(path) < len(min_path):
                min_path = path
                best_des = dev
        return min_path, best_des

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module):
        node_src = topology_src  # entity that sends the message

        # Name of the service
        service = message.dst

        DES_dst = alloc_module[app_name][message.dst]  # module sw that can serve the message

        # print "Enrouting from SRC: %i  -<->- DES %s"%(node_src,DES_dst)

        # The number of nodes control the updating of the cache. If the number of nodes changes, the cache is totally cleaned.
        if self.invalid_cache_value:
            self.invalid_cache_value = False
            self.cache = {}

        if (node_src, tuple(DES_dst)) not in list(self.cache.keys()):
            self.cache[node_src, tuple(DES_dst)] = self.compute_BEST_DES(node_src, alloc_DES, sim, DES_dst, message)

        path, des = self.cache[node_src, tuple(DES_dst)]
        self.controlServices[(node_src, service)] = (path, des)

        return [path], [des]

    def get_path_from_failure(self, sim, message, link, alloc_DES, alloc_module, ctime):
        # print "Example of enrouting"
        # print message.path # [86, 242, 160, 164, 130, 301, 281, 216]
        # print message.next_dst  # 301
        # print link #(130, 301) link is broken! 301 is unreacheble

        idx = message.path.index(link[0])
        # print "IDX: ",idx
        if idx == len(message.path):
            # The node who serves ... not possible case
            return [], []
        else:
            node_src = message.path[idx]  # In this point to the other entity the system fail
            # print "SRC: ",node_src # 164

            node_dst = message.path[len(message.path) - 1]
            # print "DST: ",node_dst #261
            # print "INT: ",message.next_dst #301

            path, des = self.get_path(sim, message.app_name, message, node_src, alloc_DES, alloc_module)
            if len(path[0]) > 0:
                # print path # [[164, 130, 380, 110, 216]]
                # print des # [40]

                concPath = message.path[0 : message.path.index(path[0][0])] + path[0]
                # print concPath # [86, 242, 160, 164, 130, 380, 110, 216]
                newINT = node_src  # path[0][2]
                # print newINT # 380

                message.next_dst = newINT
                return [concPath], des
            else:
                return [], []


def create_applications_from_json(data):
    applications = {}
    for app in data:
        modules = [Module("None", is_source=True)]
        for module in app["module"]:
            modules.append(Module(module["name"], data={"RAM": module["RAM"]}))
        a = Application(name=app["name"], operators=modules)

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


def main(simulated_time, path, results_path, case, run_id):
    G = load_yafs_json(json.load(open(path + "networkDefinition.json")))
    app_json = json.load(open(path + "appDefinition.json"))
    apps = create_applications_from_json(app_json)

    # In our model only initial cloud placements are enabled
    placement_json = json.load(open(path + "allocDefinition.json"))
    placement = JSONPlacement(name="Placement", json=placement_json)

    from yafs.selection import ShortestPath
    selectorPath = ShortestPath()
    # selectorPath = DeviceSpeedAwareRouting()

    s = Simulation(topology=Topology(G))

    dataPopulation = json.load(open(path + "usersDefinition.json"))
    # Each application has an unique population politic
    # For the original json, we filter and create a sub-list for each app politic
    for aName in list(apps.keys()):
        data = []
        for element in dataPopulation["sources"]:
            if element["app"] == aName:
                data.append(element)

        distribution = ExponentialDistribution(name="Exp", lambd=random.randint(100, 200), seed=int(aName) * 100 + run_id)
        population = DynamicPopulation(name="Dynamic_%s" % aName, data=data, iteration=run_id, activation_dist=distribution)
        s.deploy_app(apps[aName], placement, population, selectorPath)

    """
    CUSTOM EVOLUTION
    """
    dStart = DeterministicDistributionStartPoint(simulated_time / 2.0, simulated_time / 2.0 / 10.0, name="Deterministic")
    evol = CustomStrategy(results_path)
    s.deploy_monitor("EvolutionOfServices", evol, dStart, **{"sim": s, "routing": selectorPath, "case": case, "stop_time": simulated_time, "it": run_id})

    """
    RUNNING
    """
    logging.info(" Performing simulation: %s %i " % (case, run_id))
    s.run(simulated_time, results_path=results_path, progress_bar=False)

    """
    Storing results from other strategies
    """

    # Getting some info
    s.print_debug_assignaments()

    evol.summarize()

    print("----")
    entities = s.node_to_modules
    src_entities, modules_entities = Counter(), Counter()
    for k, v in entities.items():
        src_entities[k] = 0
        modules_entities[k] = 0
        for service in v:
            if "None" in service:
                src_entities[k] += 1
            elif "_" in service:
                modules_entities[k] += 1  # [u'3#3_22', u'2#2_19']

    nx.set_node_attributes(s.G, values=src_entities, name="SRC")
    nx.set_node_attributes(s.G, values=modules_entities, name="MOD")

    nx.write_gexf(s.G, os.path.join(results_path, "network.gexf"))

    # controlServices = selectorPath.controlServices
    # f = open(pathResults + "/file_assignments_%s_%i_%i.pkl" % (case, simulated_time, it), "wb")
    # pickle.dump(controlServices, f)
    # f.close()


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.DEBUG)

    n_simulations = 2
    simulated_time = 10000

    # Multiple simulations
    for i in range(1, n_simulations + 1):
        start_time = time.time()
        random.seed(i)
        np.random.seed(i)

        experiment = f"results_CQ_{simulated_time}_{i}_{time.strftime('%Y%m%d')}"
        logging.info("Running Conquest - %s" % experiment)

        main(simulated_time=simulated_time, path="exp/", results_path=experiment, case="CQ", run_id=i)
        print(("\n--- %s seconds ---" % (time.time() - start_time)))
