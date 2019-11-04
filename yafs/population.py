import logging
import random
from abc import ABC, abstractmethod

import networkx as nx

from yafs import distribution
from yafs.distribution import Distribution, ExponentialDistribution

logger = logging.getLogger(__name__)


class Population(ABC):
    """Controls how the message generation of the sensor modules is associated in the nodes of the topology.

    This assignment is based on a generation controller to each message. And a generation control is assigned to a node or to several
    in the topology both during the initiation and / or during the execution of the simulation.

    A polulation consists out of two functions:
    - *initial_allocation*: Invoked at the start of the simulation
    - *run*: Invoked according to the assigned temporal distribution

    Args:
        name: associated name
        activation_dist: a distribution function to active the *run* function in execution time

    Kwargs:
        param (dict): the parameters of the *activation_dist*  # TODO ???
    """

    def __init__(self, name: str, activation_dist: Distribution = None):
        self.name = name
        self.activation_dist = activation_dist
        self.src_control = []  # TODO Private or document  # TODO Make this a class
        self.sink_control = []  # TODO Private or document  # TODO Make this a class

    def set_sink_control(self, values):
        """Localization of sink modules"""
        self.sink_control.append(values)

    def get_next_activation(self):
        """Returns the next time to be activated in the simulation"""
        return next(self.activation_dist)  # TODO Data type?

    def set_src_control(self, values):
        """Stores the drivers of each message generator."""
        self.src_control.append(values)

    @abstractmethod
    def initial_allocation(self, sim: "Simulation", app_name: str):
        """Given an ecosystem and an application, it starts the allocation of pure sources in the topology."""
        self.run(sim)

    @abstractmethod
    def run(self, sim: "Simulation"):
        """Invoked during the simulation to change the assignment of the modules that generate the messages."""


class StaticPopulation(Population):
    """Statically assigns the generation of a source in a node of the topology. It is only invoked in the initialization."""

    def initial_allocation(self, sim, app_name):
        # Assignment of SINK and SOURCE pure modules
        for id_entity in sim.topology.G.nodes:
            entity = sim.topology.G.nodes[id_entity]

            for ctrl in self.sink_control:
                # A node can have several sinks modules
                if entity["model"] == ctrl["model"]:
                    # In this node there is a sink
                    module = ctrl["module"]
                    for number in range(ctrl["number"]):
                        sim.deploy_sink(app_name, node_id=id_entity, module=module)

            for ctrl in self.src_control:
                # A node can have several source modules
                if entity["model"] == ctrl["model"]:
                    msg = ctrl["message"]
                    dst = ctrl["distribution"]
                    for number in range(ctrl["number"]):
                        sim.deploy_source(app_name, node_id=id_entity, message=msg, distribution=dst)

    def run(self, sim: "Simulation"):
        raise NotImplementedError()


class Evolutive(Population):
    def __init__(self, fog, srcs, **kwargs):
        # TODO arreglar en otros casos
        self.fog_devices = fog
        self.number_generators = srcs
        super(Evolutive, self).__init__(**kwargs)

    def initial_allocation(self, sim, app_name):
        # ASSIGNAMENT of SOURCE - GENERATORS - ACTUATORS
        id_nodes = list(sim.topology.G.nodes())
        for ctrl in self.src_control:
            msg = ctrl["message"]
            dst = ctrl["distribution"]
            for item in range(self.number_generators):
                id = random.choice(id_nodes)
                for number in range(ctrl["number"]):
                    sim.deploy_source(app_name, node_id=id, message=msg, distribution=dst)

        # ASSIGNAMENT of the first SINK
        fog_device = self.fog_devices[0][0]
        del self.fog_devices[0]
        for ctrl in self.sink_control:
            module = ctrl["module"]
            for number in range(ctrl["number"]):
                sim.deploy_sink(app_name, node_id=fog_device, module=module)

    def run(self, sim):
        if len(self.fog_devices) > 0:
            fog_device = self.fog_devices[0][0]
            del self.fog_devices[0]
            logger.debug("Activiting - RUN - Evolutive - Deploying a new actuator at position: %i" % fog_device)
            for ctrl in self.sink_control:
                module = ctrl["module"]
                app_name = ctrl["app"]
                for number in range(ctrl["number"]):
                    sim.deploy_sink(app_name, node_id=fog_device, module=module)


# TODO Whats the difference to StaticPopulation?
class Statical(Population):
    def __init__(self, srcs, **kwargs):
        self.number_generators = srcs
        super(Statical, self).__init__(**kwargs)

    def initial_allocation(self, sim, app_name):
        for ctrl in self.src_control:
            msg = ctrl["message"]
            dst = ctrl["distribution"]
            param = ctrl["param"]
            for item in range(self.number_generators):
                id = random.choice(list(sim.topology.G.nodes()))
                for number in range(ctrl["number"]):
                    sim.deploy_source(app_name, node_id=id, message=msg, distribution=dst, param=param)

        # ASSIGNAMENT of the only one SINK
        for ctrl in self.sink_control:
            module = ctrl["module"]
            best_device = ctrl["id"]
            for number in range(ctrl["number"]):
                sim.deploy_sink(app_name, node_id=best_device, module=module)


# TODO Whats the difference to StaticPopulation?
class Statical2(Population):
    """
    This implementation of a population algorithm statically assigns the generation of a source in a node of the topology. It is only invoked in the initialization.

    Extends: :mod: Population
    """

    def initial_allocation(self, sim, app_name):
        # Assignment of SINK and SOURCE pure modules

        for ctrl in self.src_control:
            if "id" in list(ctrl.keys()):
                msg = ctrl["message"]
                dst = ctrl["distribution"]
                for idx in ctrl["id"]:
                    sim.deploy_source(app_name, node_id=idx, message=msg, distribution=dst)

        for ctrl in self.sink_control:
            if "id" in list(ctrl.keys()):
                module = ctrl["module"]
                for idx in ctrl["id"]:
                    sim.deploy_sink(app_name, node_id=idx, module=module)


class PopAndFailures(Population):
    def __init__(self, srcs, **kwargs):
        self.number_generators = srcs
        self.nodes_removed = []
        self.count_down = 20
        self.limit = 200
        super(PopAndFailures, self).__init__(**kwargs)

    def initial_allocation(self, sim, app_name):
        # ASSIGNAMENT of SOURCE - GENERATORS - ACTUATORS
        id_nodes = list(sim.topology.G.nodes())
        for ctrl in self.src_control:
            msg = ctrl["message"]
            dst = ctrl["distribution"]
            for item in range(self.number_generators):
                id = random.choice(id_nodes)
                for number in range(ctrl["number"]):
                    sim.deploy_source(app_name, node_id=id, message=msg, distribution=dst)

        for ctrl in self.sink_control:
            module = ctrl["module"]
            ids_coefficient = ctrl["ids"]
            for id in ids_coefficient:
                for number in range(ctrl["number"]):
                    sim.deploy_sink(app_name, node_id=id[0], module=module)

    def getProcessFromThatNode(self, sim, node_to_remove):
        if node_to_remove in list(sim.alloc_DES.values()):
            someModuleDeployed = False
            keys = []
            # This node can have multiples DES processes on itself
            for k, v in list(sim.alloc_DES.items()):
                if v == node_to_remove:
                    keys.append(k)
            # key = sim.alloc_DES.keys()[sim.alloc_DES.values().index(node_to_remove)]
            for key in keys:
                # Information
                # print "\tNode %i - with a DES process: %i" % (node_to_remove, key)
                # This assignamnet can be a source/sensor module:
                if key in list(sim.alloc_source.keys()):
                    # print "\t\t a sensor: %s" % sim.alloc_source[key]["module"]
                    ## Sources/Sensors modules are not removed
                    return False, [], False
                someModuleAssignament = sim.assigned_structured_modules_from_process()
                if key in list(someModuleAssignament.keys()):
                    # print "\t\t a module: %s" % someModuleAssignament[key]["module"]
                    if self.count_down < 3:
                        return False, [], False
                    else:
                        self.count_down -= 1
                        someModuleDeployed = True

            return True, keys, someModuleDeployed
        else:
            return True, [], False

    def run(self, sim):
        logger.debug("Activiting - Failure -  Removing a topology nodo == a network element, including edges")
        if self.limit > 0:
            nodes = list(sim.topology.G.nodes())
            # print sim.alloc_DES
            is_removable = False
            node_to_remove = -1
            someModuleDeployed = False
            while not is_removable:  ## WARNING: In this case there is a possibility of an infinite loop
                node_to_remove = random.choice(nodes)
                is_removable, keys_DES, someModuleDeployed = self.getProcessFromThatNode(sim, node_to_remove)

            logger.debug("Removing node: %i, Total nodes: %i" % (node_to_remove, len(nodes)))
            print("\tStopping some DES processes: %s" % keys_DES)

            self.nodes_removed.append({"id": node_to_remove, "module": someModuleDeployed, "time": sim.env.now})

            sim.remove_node(node_to_remove)

            self.limit -= 1


class PopulationMove(Population):
    def __init__(self, srcs, node_dst, **kwargs):
        self.number_generators = srcs
        self.node_dst = node_dst
        self.pos = None
        self.activation = 0
        super(PopulationMove, self).__init__(**kwargs)

    def initial_allocation(self, sim, app_name):
        # ASSIGNAMENT of SOURCE - GENERATORS - ACTUATORS
        id_nodes = list(sim.topology.G.nodes())
        for ctrl in self.src_control:
            msg = ctrl["message"]
            dst = ctrl["distribution"]
            for item in range(self.number_generators):
                id = random.choice(id_nodes)
                for number in range(ctrl["number"]):
                    sim.deploy_source(app_name, node_id=id, message=msg, distribution=dst)

        for ctrl in self.sink_control:
            module = ctrl["module"]
            best_device = ctrl["id"]
            for number in range(ctrl["number"]):
                sim.deploy_sink(app_name, node_id=best_device, module=module)

    def run(self, sim):
        import matplotlib.pyplot as plt
        import pandas as pd

        logger.debug("Activiting - Population movement")
        if self.pos == None:
            self.pos = {}
            df = pd.read_csv("pos_network.csv")
            for r in df.iterrows():
                self.pos[r[0]] = (r[1].x, r[1].y)
            del df

        fig = plt.figure(figsize=(10, 8), dpi=100)
        ax = fig.add_subplot(111)
        nx.draw(sim.topology.G, with_labels=True, pos=self.pos, node_size=60, node_color="orange", font_size=5)

        # fig, ax = plt.subplots(nrows=1, ncols=1)  # create figure & 1 axis
        for node in list(sim.alloc_DES.values()):
            # for id_s, service in enumerate(current_services):
            # for node in current_services[service]:
            #     node = sim.alloc_DES[key]
            #     print "WL in node: ",node
            if node != 72:
                circle2 = plt.Circle(self.pos[node], 40, color="green", alpha=0.8)
                ax.add_artist(circle2)

        # top centralized device
        circle2 = plt.Circle(self.pos[72], 60, color="red", alpha=0.8)
        ax.add_artist(circle2)

        # nx.draw(sim.topology.G, self.pos, node_color='gray', alpha=0.4)

        # labels = nx.draw_networkx_labels(sim.topology.G, self.pos)

        plt.text(2, 1000, "Step: %i" % self.activation, {"color": "C0", "fontsize": 16})
        # for i in range(10):
        #     plt.text(i, -.7, i, {'color': 'C2', 'fontsize': 10 + (i * .5)})  # app2
        # for i in range(10):
        #     plt.text(i, -1.2, 9 - i, {'color': 'C1', 'fontsize': 10 + (9 - i) * 0.5})  # app3

        fig.savefig("figure/net_%03d.png" % self.activation)  # save the figure to file
        plt.close(fig)  # close the figure
        # exit()

        # para cada modulo generador desplegado en la topologia
        # -- trazo el camino mas cercano hacia un modulo
        #    -- muevo dicho generador hasta el siguiente path -1 del anterior trazado

        for key in list(sim.alloc_source.keys()):
            node_src = sim.alloc_DES[key]
            path = list(nx.shortest_path(sim.topology.G, source=node_src, target=self.node_dst))
            print(path)
            if len(path) > 2:
                next_src_position = path[1]
                # print path,next_src_position
                sim.alloc_DES[key] = next_src_position
            else:
                None
                # This source cannot move more

        self.activation += 1

        # print "-" * 40
        # print "DES\t| TOPO \t| Src.Mod \t| Modules"
        # print "-" * 40
        # for k in sim.alloc_DES:
        #    print k, "\t|", self.alloc_DES[k], "\t|", self.alloc_source[k][
        #        "module"] if k in self.alloc_source.keys() else "--", "\t\t|", fullAssignation[k][
        #        "Module"] if k in fullAssignation.keys() else "--"
        # print "-" * 40


# TODO Remove this class, the population does not care how it got created
class JSONPopulation(Population):
    def __init__(self, json, iteration, **kwargs):
        super(JSONPopulation, self).__init__(**kwargs)
        self.data = json
        self.it = iteration

    def initial_allocation(self, sim, app_name):
        # for item in self.data["sinks"]:
        #     app_name = item["app"]
        #     module = item["module_name"]
        #     idtopo = item["id_resource"]
        #     sim.deploy_sink(app_name, node=idtopo, module=module)

        for item in self.data["sources"]:
            if item["app"] == app_name:
                app_name = item["app"]
                idtopo = item["id_resource"]
                lambd = item["lambda"]
                app = sim.apps[app_name]
                msg = app.messages[item["message"]]

                dDistribution = ExponentialDistribution(name="Exp", lambd=lambd, seed=self.it)

                sim.deploy_source(app_name, node_id=idtopo, message=msg, distribution=dDistribution)


class JSONPopulation2(Population):
    def __init__(self, json, it, **kwargs):
        super().__init__(**kwargs)
        self.data = json
        self.it = it

    def initial_allocation(self, sim, app_name):
        for idx, behaviour in enumerate(self.data["sources"]):
            # Creating the type of the distribution
            # behaviour["args"] should have the same attributes of the used distribution
            class_ = getattr(distribution, behaviour["distribution"])
            if "seed" not in list(behaviour["args"].keys()):
                seed = idx + self.it
                instance_distribution = class_(name="h%i" % idx, seed=seed, **behaviour["args"])
            else:
                instance_distribution = class_(name="h%i" % idx, **behaviour["args"])

            # Getting information from the APP
            app_name = behaviour["app"]
            app = sim.apps[app_name]
            msg = app.messages[behaviour["message"]]

            # TODO Include a more flexible constructor
            # if behaviour["entity"] == "all":
            #     for entity in sim.mobile_fog_entities:
            #         print entity
            #         sim.deploy_source(app_name, id_node=int(entity), msg=msg, distribution=instance_distribution)
            # else:

            sim.deploy_source(app_name, node_id=behaviour["entity"], message=msg, distribution=instance_distribution)


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

    def initial_allocation(self, sim, app_name):
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

            app = sim.apps[app_name]
            msg = app.get_message[item["message"]]

            # A basic creation of the seed: unique for each user and different in each simulation repetition
            seed = item["id_resource"] * 1000 + item["lambda"] + self.it

            dDistribution = ExponentialDistribution(name="Exp", lambd=lambd, seed=seed)
            sim.deploy_source(app_name, node_id=idtopo, message=msg, distribution=dDistribution)


class SimpleDynamicChanges(Population):
    """Statically assigns the generation of a source in a node of the topology. It is only invoked in the initialization."""

    def __init__(self, run_times, **kwargs):
        self.run_times = run_times
        super(SimpleDynamicChanges, self).__init__(**kwargs)

    def initial_allocation(self, sim, app_name):

        # Assignment of SINK and SOURCE pure modules
        for id_entity in sim.topology.G.nodes:
            entity = sim.topology.G.nodes[id_entity]
            for ctrl in self.sink_control:
                # A node can have several sinks modules
                if entity["model"] == ctrl["model"]:
                    # In this node there is a sink
                    module = ctrl["module"]
                    for number in range(ctrl["number"]):
                        sim.deploy_sink(app_name, node_id=id_entity, module=module)
            # end for sink control

            for ctrl in self.src_control:
                # A node can have several source modules
                if entity["model"] == ctrl["model"]:
                    msg = ctrl["message"]
                    dst = ctrl["distribution"]
                    for number in range(ctrl["number"]):
                        sim.deploy_source(app_name, node_id=id_entity, message=msg, distribution=dst)

            # end for src control
        # end assignments

    def run(self, sim):
        if self.run_times == 0:  # In addition, we can stop the process according to any criteria
            sim.stop_process(sim.des_control_process[self.name])
        else:
            self.run_times -= 1
            # Run whatever you want
            print("Running Population-Evolution: %i" % self.run_times)
