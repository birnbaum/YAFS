import logging
import random
from abc import ABC, abstractmethod

import networkx as nx

from yafs import distribution
from yafs.application import Application
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
        self._src_control = []  # TODO Make this a class?
        self._sink_control = []  # TODO Make this a class?

    def set_sink_control(self, values):
        """Localization of sink modules"""
        self._sink_control.append(values)

    def get_next_activation(self):
        """Returns the next time to be activated in the simulation"""
        return next(self.activation_dist)  # TODO Data type?

    def set_src_control(self, values):
        """Stores the drivers of each message generator."""
        self._src_control.append(values)

    @abstractmethod
    def initial_allocation(self, simulation: "Simulation", application: Application):
        """Given an ecosystem and an application, it starts the allocation of pure sources in the topology."""

    @abstractmethod
    def run(self, sim: "Simulation"):
        """Invoked during the simulation to change the assignment of the modules that generate the messages."""


class StaticPopulation(Population):
    """Statically assigns the generation of a source in a node of the topology. It is only invoked in the initialization."""

    def initial_allocation(self, simulation, application):
        # Assignment of SINK and SOURCE pure modules
        for node_id, node_data in simulation.topology.G.nodes(data=True):

            for ctrl in self._src_control:  # A node can have several source modules
                if node_id == ctrl["id"]:
                    msg = ctrl["message"]
                    dst = ctrl["distribution"]
                    for _ in range(ctrl["number"]):
                        simulation.deploy_source(application, node_id=node_id, message=msg, distribution=dst)

            for ctrl in self._sink_control:  # A node can have several sinks modules
                if node_id == ctrl["id"]:  # In this node there is a sink
                    module_name = ctrl["module"]
                    for _ in range(ctrl["number"]):
                        simulation.deploy_sink(application, node_id=node_id, module_name=module_name)

    def run(self, sim: "Simulation"):
        raise NotImplementedError()


class Evolutive(Population):
    def __init__(self, fog, srcs, **kwargs):
        super().__init__(**kwargs)
        self.fog_devices = fog
        self.number_generators = srcs

    def initial_allocation(self, simulation, application):
        # ASSIGNAMENT of SOURCE - GENERATORS - ACTUATORS
        id_nodes = list(simulation.topology.G.nodes())
        for ctrl in self._src_control:
            msg = ctrl["message"]
            dst = ctrl["distribution"]
            for item in range(self.number_generators):
                id = random.choice(id_nodes)
                for number in range(ctrl["number"]):
                    simulation.deploy_source(application, node_id=id, message=msg, distribution=dst)

        # ASSIGNAMENT of the first SINK
        fog_device = self.fog_devices[0][0]
        del self.fog_devices[0]
        for ctrl in self._sink_control:
            module = ctrl["module"]
            for number in range(ctrl["number"]):
                simulation.deploy_sink(application, node_id=fog_device, module_name=module)

    def run(self, sim):
        if len(self.fog_devices) > 0:
            fog_device = self.fog_devices[0][0]
            del self.fog_devices[0]
            logger.debug("Activiting - RUN - Evolutive - Deploying a new actuator at position: %i" % fog_device)
            for ctrl in self._sink_control:
                module = ctrl["module"]
                app_name = ctrl["app"]
                for number in range(ctrl["number"]):
                    sim.deploy_sink(app_name, node_id=fog_device, module_name=module)


# TODO Whats the difference to StaticPopulation?
class Statical(Population):
    def __init__(self, srcs, **kwargs):
        self.number_generators = srcs
        super(Statical, self).__init__(**kwargs)

    def initial_allocation(self, simulation, application):
        for ctrl in self._src_control:
            msg = ctrl["message"]
            dst = ctrl["distribution"]
            param = ctrl["param"]
            for item in range(self.number_generators):
                id = random.choice(list(simulation.topology.G.nodes()))
                for number in range(ctrl["number"]):
                    simulation.deploy_source(application, node_id=id, message=msg, distribution=dst, param=param)

        # ASSIGNAMENT of the only one SINK
        for ctrl in self._sink_control:
            module = ctrl["module"]
            best_device = ctrl["id"]
            for number in range(ctrl["number"]):
                simulation.deploy_sink(application, node_id=best_device, module_name=module)


class PopAndFailures(Population):
    def __init__(self, srcs, **kwargs):
        self.number_generators = srcs
        self.nodes_removed = []
        self.count_down = 20
        self.limit = 200
        super(PopAndFailures, self).__init__(**kwargs)

    def initial_allocation(self, simulation, application):
        # ASSIGNAMENT of SOURCE - GENERATORS - ACTUATORS
        id_nodes = list(simulation.topology.G.nodes())
        for ctrl in self._src_control:
            msg = ctrl["message"]
            dst = ctrl["distribution"]
            for item in range(self.number_generators):
                id = random.choice(id_nodes)
                for number in range(ctrl["number"]):
                    simulation.deploy_source(application, node_id=id, message=msg, distribution=dst)

        for ctrl in self._sink_control:
            module = ctrl["module"]
            ids_coefficient = ctrl["ids"]
            for id in ids_coefficient:
                for number in range(ctrl["number"]):
                    simulation.deploy_sink(application, node_id=id[0], module_name=module)

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
                is_removable, keys_DES, someModuleDeployed = self._get_process_from_node(sim, node_to_remove)

            logger.debug("Removing node: %i, Total nodes: %i" % (node_to_remove, len(nodes)))
            print("\tStopping some DES processes: %s" % keys_DES)

            self.nodes_removed.append({"id": node_to_remove, "module": someModuleDeployed, "time": sim.env.now})

            sim.remove_node(node_to_remove)

            self.limit -= 1

    def _get_process_from_node(self, sim, node_to_remove):
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


class PopulationMove(Population):
    def __init__(self, srcs, node_dst, **kwargs):
        self.number_generators = srcs
        self.node_dst = node_dst
        self.pos = None
        self.activation = 0
        super(PopulationMove, self).__init__(**kwargs)

    def initial_allocation(self, simulation, application):
        # ASSIGNAMENT of SOURCE - GENERATORS - ACTUATORS
        id_nodes = list(simulation.topology.G.nodes())
        for ctrl in self._src_control:
            msg = ctrl["message"]
            dst = ctrl["distribution"]
            for item in range(self.number_generators):
                id = random.choice(id_nodes)
                for number in range(ctrl["number"]):
                    simulation.deploy_source(application, node_id=id, message=msg, distribution=dst)

        for ctrl in self._sink_control:
            module = ctrl["module"]
            best_device = ctrl["id"]
            for number in range(ctrl["number"]):
                simulation.deploy_sink(application, node_id=best_device, module_name=module)

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


# TODO Remove this class, the population does not care how it got created
class JSONPopulation(Population):
    def __init__(self, json, iteration, **kwargs):
        super().__init__(**kwargs)
        self.data = json
        self.it = iteration

    def initial_allocation(self, simulation, application):
        for item in self.data["sinks"]:
            app_name = item["app"]
            if app_name != application.name:
                continue
            simulation.deploy_sink(app_name, node=item["id_resource"], module_name=item["module_name"])

        for item in self.data["sources"]:
            app_name = item["app"]
            if app_name != application.name:
                continue
            app = simulation.apps[application]
            msg = app.messages[item["message"]]
            dDistribution = ExponentialDistribution(name="Exp", lambd=item["lambda"], seed=self.it)
            simulation.deploy_source(application, node_id=item["id_resource"], message=msg, distribution=dDistribution)
