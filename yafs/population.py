import logging
import random
from abc import ABC

from yafs.distribution import Distribution

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
        self.src_control = []  # TODO Private or document
        self.sink_control = []  # TODO Private or document

    def set_sink_control(self, values):
        """Localization of sink modules"""
        self.sink_control.append(values)

    def get_next_activation(self):
        """Returns the next time to be activated in the simulation"""
        return next(self.activation_dist)  # TODO Data type?

    def set_src_control(self, values):
        """Stores the drivers of each message generator."""
        self.src_control.append(values)

    def initial_allocation(self, sim, app_name):
        """Given an ecosystem and an application, it starts the allocation of pure sources in the topology.

        .. attention:: override required
        """
        self.run()  # TODO Pass sim??

    # override
    def run(self, sim):
        """
        This method will be invoked during the simulation to change the assignment of the modules that generate the messages.

        Args:
            sim (:mod: yafs.core.Sim)
        """
        logger.debug("Activiting - RUN - Population")
        """ User definition of the Population evolution """


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
                        sim.deploy_sink(app_name, node=id_entity, module=module)

            for ctrl in self.src_control:
                # A node can have several source modules
                if entity["model"] == ctrl["model"]:
                    msg = ctrl["message"]
                    dst = ctrl["distribution"]
                    for number in range(ctrl["number"]):
                        idsrc = sim.deploy_source(app_name, id_node=id_entity, msg=msg, distribution=dst)
                        # the idsrc can be used to control the deactivation of the process in a dynamic behaviour


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
                    idsrc = sim.deploy_source(app_name, id_node=id, msg=msg, distribution=dst)

        # ASSIGNAMENT of the first SINK
        fog_device = self.fog_devices[0][0]
        del self.fog_devices[0]
        for ctrl in self.sink_control:
            module = ctrl["module"]
            for number in range(ctrl["number"]):
                sim.deploy_sink(app_name, node=fog_device, module=module)

    def run(self, sim):
        if len(self.fog_devices) > 0:
            fog_device = self.fog_devices[0][0]
            del self.fog_devices[0]
            logger.debug("Activiting - RUN - Evolutive - Deploying a new actuator at position: %i" % fog_device)
            for ctrl in self.sink_control:
                module = ctrl["module"]
                app_name = ctrl["app"]
                for number in range(ctrl["number"]):
                    sim.deploy_sink(app_name, node=fog_device, module=module)


# TODO Whats the difference to StaticPopulation?
class Statical(Population):
    def __init__(self, srcs, **kwargs):
        self.number_generators = srcs
        super(Statical, self).__init__(**kwargs)

    def initial_allocation(self, sim, app_name):
        # ASSIGNAMENT of SOURCE - GENERATORS - ACTUATORS
        id_nodes = list(sim.topology.G.nodes())
        for ctrl in self.src_control:
            msg = ctrl["message"]
            dst = ctrl["distribution"]
            param = ctrl["param"]
            for item in range(self.number_generators):
                id = random.choice(id_nodes)
                for number in range(ctrl["number"]):
                    idsrc = sim.deploy_source(app_name, id_node=id, msg=msg, distribution=dst, param=param)

        # ASSIGNAMENT of the only one SINK
        for ctrl in self.sink_control:
            module = ctrl["module"]
            best_device = ctrl["id"]
            for number in range(ctrl["number"]):
                sim.deploy_sink(app_name, node=best_device, module=module)


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
                    idsrc = sim.deploy_source(app_name, id_node=id, msg=msg, distribution=dst)

        for ctrl in self.sink_control:
            module = ctrl["module"]
            ids_coefficient = ctrl["ids"]
            for id in ids_coefficient:
                for number in range(ctrl["number"]):
                    sim.deploy_sink(app_name, node=id[0], module=module)

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
                someModuleAssignament = sim.get_assigned_structured_modules_from_DES()
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