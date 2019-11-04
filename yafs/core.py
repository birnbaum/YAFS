"""This module unifies the event-discrete simulation environment with the rest of modules: placement, topology, selection, population, utils and metrics."""

import logging
from collections import Callable
from typing import Optional, List, Dict

import simpy
from tqdm import tqdm

from yafs.application import Application, Message, Service
from yafs.distribution import *
from yafs.placement import Placement
from yafs.population import Population
from yafs.selection import Selection
from yafs.stats import Stats, EventLog
from yafs.topology import Topology

EVENT_UP_ENTITY = "node_up"
EVENT_DOWN_ENTITY = "node_down"

logger = logging.getLogger(__name__)


class Simulation:
    """Contains the cloud event-discrete simulation environment and controls the structure variables.

    Args:
        topology: Associated topology of the environment.
        default_results_path  # TODO ???
    """

    NODE_METRIC = "COMP_M"
    SINK_METRIC = "SINK_M"
    LINK_METRIC = "LINK"

    def __init__(self, topology: Topology):
        # TODO Refactor this class. Way too many fields, no clear separation of concerns.

        self.topology = topology

        self.env = simpy.Environment()  # discrete-event simulator (aka DES)
        self.env.process(self._network_process())
        self.network_ctrl_pipe = simpy.Store(self.env)

        self._process_id = 0  # Unique identifier for each process in the DES
        self._message_id = 0  # Unique identifier for each message
        self.network_pump = 0  # Shared resource that controls the exchange of messages in the topology

        self.applications = {}

        self.event_log = EventLog()

        self.placement_policy = {}  # for app.name the placement algorithm
        self.population_policy = {}  # for app.name the population algorithm

        # Start/stop flag for each pure source
        # key: id.source.process
        # value: Boolean
        self.des_process_running = {}

        # key: app.name
        # value: des process
        self.des_control_process = {}

        # Queues for each message
        # App+module+process_id -> pipe
        self.consumer_pipes = {}

        """Relationship of pure source with topology entity

        id.source.process -> value: dict("id","app","module")

          .. code-block:: python

            alloc_source[34] = {"id":id_node,"app":app_name,"module":source module}
        """
        self.alloc_source = {}

        """Represents the deployment of a module in a DES PROCESS each DES has a one topology.node.id (see alloc_des var.)

        It used for (:mod:`Placement`) class interaction.

        A dictionary where the key is an app.name and value is a dictionary with key is a module and value an array of id DES process

        .. code-block:: python

            {"EGG_GAME":{"Controller":[1,3,4],"Client":[4]}}
        """
        self.app_to_module_to_processes = {}

        """Relationship between DES process and topology.node.id

        It is necessary to identify the message.source (topology.node)
        1.N. DES process -> 1. topology.node
        """
        self.process_to_node = {}

        # Store for each app.name the selection policy
        # app.name -> Selector
        self.selector_path = {}

        # This variable control the lag of each busy network links. It avoids the generation of a DES-process for each link
        # edge -> last_use_channel (float) = Simulation time
        self.last_busy_time = {}  # must be updated with up/down node

    @property
    def stats(self):
        return Stats(self.event_log)

    @property
    def node_to_modules(self) -> Dict[int, List]:
        """Returns a dictionary mapping from node ids to their deployed services"""
        result = {key: [] for key in self.topology.G.nodes}
        for src_deployed in self.alloc_source.values():
            result[src_deployed["id"]].append(src_deployed["app"] + "#" + src_deployed["module"].name)
        for app in self.app_to_module_to_processes:
            for module_name in self.app_to_module_to_processes[app]:
                for process_id in self.app_to_module_to_processes[app][module_name]:
                    result[self.process_to_node[process_id]].append(app + "#" + module_name)
        return result

    def process_from_module_in_node(self, node, app_name, module_name):
        deployed = self.app_to_module_to_processes[app_name][module_name]
        for des in deployed:
            if self.process_to_node[des] == node:
                return des
        return []

    def assigned_structured_modules_from_process(self):
        fullAssignation = {}
        for app in self.app_to_module_to_processes:
            for module in self.app_to_module_to_processes[app]:
                deployed = self.app_to_module_to_processes[app][module]
                for des in deployed:
                    fullAssignation[des] = {"DES": self.process_to_node[des], "module": module}
        return fullAssignation
        
    def _next_process_id(self) -> int:
        self._process_id += 1
        return self._process_id

    def _next_message_id(self):
        self._message_id += 1
        return self._message_id

    def _send_message(self, app_name: str, message: Message, process_id: int):
        """Sends a message between modules and updates the metrics once the message reaches the destination module

        Args:
            app_name: TODO
            message: TODO
            process_id: TODO
        """
        # TODO IMPROVE asignation of topo = alloc_DES(IdDES) , It has to move to the get_path process
        try:
            paths, dst_process_id = self.selector_path[app_name].get_path(
                self, app_name, message, self.alloc_DES[process_id], self.alloc_DES, self.alloc_module, self.last_busy_time, from_des=process_id
            )
            if dst_process_id == [None] or dst_process_id == [[]]:
                logger.warning("(#DES:%i)\t--- Unreacheable DST:\t%s: PATH:%s " % (process_id, message.name, paths))
                logger.debug("From __send_message function: ")
                logger.debug("NODES (%i)" % len(self.topology.G.nodes()))
            else:
                logger.debug("(#DES:%i)\t--- SENDING Message:\t%s: PATH:%s  DES:%s" % (process_id, message.name, paths, dst_process_id))
                # May be, the selector of path decides broadcasting multiples paths  # TODO What does this comment mean?
                for idx, path in enumerate(paths):
                    msg = copy.copy(message)
                    msg.path = copy.copy(path)
                    msg.app_name = app_name
                    msg.process_id = dst_process_id[idx]
                    self.network_ctrl_pipe.put(msg)
        except KeyError:
            logger.warning("(#DES:%i)\t--- Unreacheable DST:\t%s " % (process_id, message.name))

    def _network_process(self):
        """Internal DES-process who manages the latency of messages sent in the network.

        Performs the simulation of packages within the path between src and dst entities decided by the selection algorithm.
        In this way, the message has a transmission latency.
        """
        self.last_busy_time = {}  # dict(zip(edges, [0.0] * len(edges)))

        while True:
            message = yield self.network_ctrl_pipe.get()

            # If same SRC and PATH or the message has achieved the penultimate node to reach the dst
            if not message.path or message.path[-1] == message.dst_int or len(message.path) == 1:
                pipe_id = "%s%s%i" % (message.app_name, message.dst.name, message.process_id)  # app_name + module_name (dst) + process_id
                # Timestamp reception message in the module
                message.timestamp_rec = self.env.now
                # The message is sent to the module.pipe
                self.consumer_pipes[pipe_id].put(message)
            else:
                # The message is sent at first time or it sent more times.
                if message.dst_int < 0:
                    src_int = message.path[0]
                    message.dst_int = message.path[1]
                else:
                    src_int = message.dst_int
                    message.dst_int = message.path[message.path.index(message.dst_int) + 1]
                # arista set by (src_int,message.dst_int)
                link = (src_int, message.dst_int)

                # Links in the topology are bidirectional: (a,b) == (b,a)
                last_used = self.last_busy_time.get(link, 0)

                # Computing message latency
                transmit = message.size / (self.topology.G.edges[link][Topology.LINK_BW] * 1000000.0)  # MBITS!
                propagation = self.topology.G.edges[link][Topology.LINK_PR]
                latency_msg_link = transmit + propagation
                logger.debug(f"Link: {link}; Latency: {latency_msg_link}")

                self.event_log.append_transmission(id=message.id,
                                                   type=self.LINK_METRIC,
                                                   src=link[0],
                                                   dst=link[1],
                                                   app=message.app_name,
                                                   latency=latency_msg_link,
                                                   message=message.name,
                                                   ctime=self.env.now,
                                                   size=message.size,
                                                   buffer=self.network_pump)

                # We compute the future latency considering the current utilization of the link
                if last_used < self.env.now:
                    shift_time = 0.0
                    last_used = latency_msg_link + self.env.now  # future arrival time
                else:
                    shift_time = last_used - self.env.now
                    last_used = self.env.now + shift_time + latency_msg_link

                self.last_busy_time[link] = last_used
                self.env.process(self.__wait_message(message, latency_msg_link, shift_time))

                # TODO Temporarily commented out, needs refactoring
                # except:  # TODO Too broad exception clause
                #     # This fact is produced when a node or edge the topology is changed or disappeared
                #     logger.warning("The initial path assigned is unreachabled. Link: (%i,%i). Routing a new one. %i" % (link[0], link[1], self.env.now))
                #
                #     paths, DES_dst = self.selector_path[message.app_name].get_path_from_failure(
                #         self, message, link, self.alloc_DES, self.alloc_module, self.last_busy_time, self.env.now
                #     )
                #
                #     if DES_dst == [] and paths == []:
                #         # Message communication ending: The message have arrived to the destination node but it is unavailable.
                #         logger.debug("\t No path given. Message is lost")
                #     else:
                #         message.path = copy.copy(paths[0])
                #         message.process_id = DES_dst[0]
                #         logger.debug("(\t New path given. Message is enrouting again.")
                #         self.network_ctrl_pipe.put(message)

    def __wait_message(self, message, latency, shift_time):
        """Simulates the transfer behavior of a message on a link"""
        self.network_pump += 1
        yield self.env.timeout(latency + shift_time)
        self.network_pump -= 1
        self.network_ctrl_pipe.put(message)

    def _placement_process(self, placement):
        """
        A DES-process who controls the invocation of Placement.run
        """
        process_id = self._next_process_id()
        self.des_process_running[process_id] = True
        self.des_control_process[placement.name] = process_id

        logger.debug("Added_Process - Placement Algorithm\t#DES:%i" % process_id)
        while True and self.des_process_running[process_id]:
            yield self.env.timeout(placement.get_next_activation())
            placement.run(self)
            logger.debug("(DES:%i) %7.4f Run - Placement Policy" % (process_id, self.env.now))  # Rewrite
        logger.debug("STOP_Process - Placement Algorithm\t#DES:%i" % process_id)

    def _population_process(self, population):
        """
        A DES-process who controls the invocation of Population.run
        """
        process_id = self._next_process_id()
        self.des_process_running[process_id] = True
        self.des_control_process[population.name] = process_id

        logger.debug("Added_Process - Population Algorithm\t#DES:%i" % process_id)
        while True and self.des_process_running[process_id]:
            yield self.env.timeout(population.get_next_activation())
            logger.debug("(DES:%i) %7.4f Run - Population Policy" % (process_id, self.env.now))  # REWRITE
            population.run(self)
        logger.debug("STOP_Process - Population Algorithm\t#DES:%i" % process_id)

    def __add_source_population(self, process_id, name_app, message, distribution):
        """Process who controls the invocation of several Pure Source Modules"""
        logger.debug("Added_Process - Module Pure Source\t#DES:%i" % process_id)
        while True and self.des_process_running[process_id]:
            yield self.env.timeout(next(distribution))
            if self.des_process_running[process_id]:
                logger.debug(f"{name_app}:{process_id}\tGenerating Message: {message.name} \t(T:{self.env.now})")
                new_message = message.evolve(timestamp=self.env.now, id=self._next_message_id())
                self._send_message(name_app, new_message, process_id)
        logger.debug("STOP_Process - Module Pure Source\t#DES:%i" % process_id)

    def __update_node_metrics(self, app, module, message, dst_process, type_):
        """computes the service time in processing a message and record this event"""
        try:
            if module in self.applications[app].sink_modules:  # module is a SINK
                id_node = self.process_to_node[dst_process]
                time_service = 0
            else:
                id_node = self.process_to_node[dst_process]  # module is a processing module
                att_node = self.topology.G.nodes[id_node]
                time_service = message.instructions / float(att_node["IPT"])

            # TODO Investigate
            # WARNING. If there are more than two equal modules deployed in the same entity,
            # it will not be possible to determine which process sent this package at this point.
            # That information will have to be calculated by the trace of the message (message.id)
            src_process = -1
            try:
                DES_possible = self.app_to_module_to_processes[app][message.src]
                for eDES in DES_possible:
                    if self.process_to_node[eDES] == message.path[0]:
                        src_process = eDES
            except:
                for k in list(self.alloc_source.keys()):
                    if self.alloc_source[k]["id"] == message.path[0]:
                        src_process = k

            self.event_log.append_event(id=message.id,
                                        type=type_,
                                        app=app,
                                        module=module,
                                        message=message.name,
                                        DES_src=src_process,
                                        DES_dst=dst_process,
                                        module_src=message.src,
                                        TOPO_src=message.path[0],
                                        TOPO_dst=id_node,
                                        service=time_service,
                                        time_in=self.env.now,
                                        time_out=time_service + self.env.now,
                                        time_emit=float(message.timestamp),
                                        time_reception=float(message.timestamp_rec))

            return time_service
        except KeyError:
            # The node can be removed
            logger.critical("Make sure that this node has been removed or it has all mandatory attributes - Node: DES:%i" % dst_process)
            return 0

    def deploy_node_failure_generator(self, nodes: List[int], distribution: Distribution, logfile: Optional[str] = None) -> None:
        logger.debug(f"Adding Process: Node Failure Generator (DES:{self._next_process_id()}) <nodes={nodes}, distribution={distribution}>")
        self.env.process(self._node_failure_generator(nodes, distribution, logfile))

    def _node_failure_generator(self, nodes: List[int], distribution: Distribution, logfile: Optional[str] = None):
        """Controls the elimination of nodes"""
        for node in nodes:
            yield self.env.timeout(next(distribution))
            processes = [k for k, v in self.process_to_node.items() if v == node]  # A node can host multiples DES processes
            if logfile:
                with open(logfile, "a") as stream:
                    stream.write("%i,%s,%d\n" % (node, len(processes), self.env.now))
            logger.debug("\n\nRemoving node: %i, Total nodes: %i" % (node, len(self.topology.G)))
            self.remove_node(node)
            for process in processes:
                logger.debug("\tStopping DES process: %s\n\n" % process)
                self.stop_process(process)

    """
    MEJORAR - ASOCIAR UN PROCESO QUE LOS CONTROLES®.
    """

    def __add_up_node_process(self, next_event, **param):
        process_id = self._next_process_id()
        logger.debug("Added_Process - UP entity Creation\t#DES:%i" % process_id)
        while True:
            # TODO Define function to ADD a new NODE in topology
            yield self.env.timeout(next_event(**param))
            logger.debug("(DES:%i) %7.4f Node " % (process_id, self.env.now))
        logger.debug("STOP_Process - UP entity Creation\t#DES%i" % process_id)

    """
    MEJORAR - ASOCIAR UN PROCESO QUE LOS CONTROLES.
    """

    def __add_down_node_process(self, next_event, **param):
        process_id = self._next_process_id()
        self.des_process_running[process_id] = True
        logger.debug("Added_Process - Down entity Creation\t#DES:%i" % process_id)
        while self.des_process_running[process_id]:
            yield self.env.timeout(next_event(**param))
            logger.debug("(DES:%i) %7.4f Node " % (process_id, self.env.now))

        logger.debug("STOP_Process - Down entity Creation\t#DES%i" % process_id)

    def __add_consumer_module(self, process_id: int, app_name: str, module: str, services: List[Service]):
        """Process associated to a compute module"""
        logger.debug("Added_Process - Module Consumer: %s\t#DES:%i" % (module, process_id))
        while self.des_process_running[process_id]:
            if self.des_process_running[process_id]:
                msg = yield self.consumer_pipes["%s%s%i" % (app_name, module, process_id)].get()
                # One pipe for each module name

                # print "Registers len: %i" %len(register_consumer_msg)
                doBefore = False
                for service in services:
                    if msg.name == service.message_in.name:
                        # The message can be treated by this module

                        # The module only computes this type of message one time.
                        # It records once
                        if not doBefore:
                            logger.debug("(App:%s#DES:%i#%s)\tModule - Recording the message:\t%s" % (app_name, process_id, module, msg.name))
                            type = self.NODE_METRIC

                            service_time = self.__update_node_metrics(app_name, module, msg, process_id, type)

                            yield self.env.timeout(service_time)
                            doBefore = True

                        """
                        Transferring the message
                        """
                        if not service.message_out:
                            """
                            Sink behaviour (nothing to send)
                            """
                            logger.debug("(App:%s#DES:%i#%s)\tModule - Sink Message:\t%s" % (app_name, process_id, module, msg.name))
                            continue
                        else:
                            if random.random() <= service.probability:
                                last_idDes = msg.last_idDes + [process_id]
                                msg_out = service.message_out.evolve(timestamp=self.env.now, id=msg.id, last_idDes=last_idDes)

                                if not service.module_dst:
                                    # it is not a broadcasting message
                                    logger.debug(
                                        "(App:%s#DES:%i#%s)\tModule - Transmit Message:\t%s" % (app_name, process_id, module, service.message_out.name)
                                    )
                                    self._send_message(app_name, msg_out, process_id)
                                else:
                                    # it is a broadcasting message
                                    logger.debug(
                                        "(App:%s#DES:%i#%s)\tModule - Broadcasting Message:\t%s" % (app_name, process_id, module, service.message_out.name)
                                    )
                                    for idx, module_dst in enumerate(service.module_dst):
                                        if random.random() <= service["p"][idx]:
                                            self._send_message(app_name, msg_out, process_id)

                            else:
                                logger.debug("(App:%s#DES:%i#%s)\tModule - Stopped Message:\t%s" % (app_name, process_id, module, service.message_out.name))

        logger.debug("STOP_Process - Module Consumer: %s\t#DES:%i" % (module, process_id))

    def __add_sink_module(self, ides, app_name, module):
        """
        It generates a DES process associated to a SINK module
        """
        logger.debug("Added_Process - Module Pure Sink: %s\t#DES:%i" % (module, ides))
        while True and self.des_process_running[ides]:
            msg = yield self.consumer_pipes["%s%s%i" % (app_name, module, ides)].get()
            """
            Processing the message
            """
            logger.debug("(App:%s#DES:%i#%s)\tModule Pure - Sink Message:\t%s" % (app_name, ides, module, msg.name))
            type = self.SINK_METRIC
            service_time = self.__update_node_metrics(app_name, module, msg, ides, type)
            yield self.env.timeout(service_time)  # service time is 0

        logger.debug("STOP_Process - Module Pure Sink: %s\t#DES:%i" % (module, ides))

    def __add_consumer_service_pipe(self, app_name, module, process_id):
        logger.debug("Creating PIPE: %s%s%i " % (app_name, module, process_id))
        self.consumer_pipes["%s%s%i" % (app_name, module, process_id)] = simpy.Store(self.env)

    def get_DES(self, name):
        return self.des_control_process[name]

    def deploy_monitor(self, name: str, function: Callable, distribution: Callable, **param):
        """Add a DES process for user purpose

        Args:
            name: name of monitor
            function: function that will be invoked within the simulator with the user's code
            distribution: a temporary distribution function

        Kwargs:
            param (dict): the parameters of the *distribution* function
        """
        self.env.process(self._add_monitor(name, function, distribution, **param))

    def _add_monitor(self, name, function, distribution, **param):
        """Pprocess for user purpose"""
        process_id = self._next_process_id()
        logger.debug("Added_Process - Internal Monitor: %s\t#DES:%i" % (name, process_id))
        while True:
            yield self.env.timeout(next(distribution))
            function(**param)

    def register_event_entity(self, next_event_dist, event_type=EVENT_UP_ENTITY, **args):
        """
        TODO
        """
        if event_type == EVENT_UP_ENTITY:
            self.env.process(self.__add_up_node_process(next_event_dist, **args))
        elif event_type == EVENT_DOWN_ENTITY:
            self.env.process(self.__add_down_node_process(next_event_dist, **args))

    def deploy_source(self, app_name: str, id_node: int, msg, distribution) -> int:
        """Add a DES process for deploy pure source modules (sensors)
        This function its used by (:mod:`Population`) algorithm

        Args:
            app_name: application name
            id_node: entity.id of the topology who will create the messages
            msg: TODO
            distribution (function): a temporary distribution function

        Kwargs:
            param - the parameters of the *distribution* function  # TODO ???

        Returns:
            Process id
        """
        process_id = self._next_process_id()
        self.des_process_running[process_id] = True
        self.env.process(self.__add_source_population(process_id, app_name, msg, distribution))
        self.process_to_node[process_id] = id_node
        self.alloc_source[process_id] = {"id": id_node, "app": app_name, "module": msg.src, "name": msg.name}
        return process_id

    def _deploy_module(self, app_name: str, module: str, id_node: int, services: List[Service]) -> int:
        """Add a DES process for deploy  modules
        This function its used by (:mod:`Population`) algorithm

        Args:
            app_name: application name
            id_node: entity.id of the topology who will create the messages
            module: module name
            services: TODO

        Returns:
            Process id
        """
        process_id = self._next_process_id()
        self.des_process_running[process_id] = True
        self.env.process(self.__add_consumer_module(process_id, app_name, module, services))
        # To generate the QUEUE of a SERVICE module
        self.__add_consumer_service_pipe(app_name, module, process_id)

        self.process_to_node[process_id] = id_node
        if module not in self.app_to_module_to_processes[app_name]:  # TODO defaultdict
            self.app_to_module_to_processes[app_name][module] = []
        self.app_to_module_to_processes[app_name][module].append(process_id)

        return process_id

    def deploy_sink(self, app_name: str, node: int, module: str):
        """Add a DES process to deploy pure SINK modules (actuators).

        This function its used by the placement algorithm internally, there is no DES PROCESS for this type of behaviour

        Args:
            app_name: application name
            node: entity.id of the topology who will create the messages
            module: module
        """
        process_id = self._next_process_id()
        self.des_process_running[process_id] = True
        self.process_to_node[process_id] = node
        self.__add_consumer_service_pipe(app_name, module, process_id)
        # Update the relathionships among module-entity
        if app_name in self.app_to_module_to_processes:
            if module not in self.app_to_module_to_processes[app_name]:
                self.app_to_module_to_processes[app_name][module] = []
        self.app_to_module_to_processes[app_name][module].append(process_id)
        self.env.process(self.__add_sink_module(process_id, app_name, module))

    def stop_process(self, id: int):  # TODO Use SimPy functionality for this
        """All pure source modules (sensors) are controlled by this boolean.
        Using this function (:mod:`Population`) algorithm can stop one source

        Args:
            id.source: the identifier of the DES process.
        """
        self.des_process_running[id] = False

    def start_process(self, id: int):  # TODO Use SimPy functionality for this
        """All pure source modules (sensors) are controlled by this boolean.
        Using this function (:mod:`Population`) algorithm can start one source

        Args:
            id.source: the identifier of the DES process.
        """
        self.des_process_running[id] = True

    def deploy_app(self, app: Application, placement: Placement, population: Population, selection: Selection):
        """This process is responsible for linking the *application* to the different algorithms (placement, population, and service)"""
        self.applications[app.name] = app
        self.app_to_module_to_processes[app.name] = {}

        # Add Placement controls to the App
        if placement.name not in list(self.placement_policy.keys()):  # First Time
            self.placement_policy[placement.name] = {"placement_policy": placement, "apps": []}
            if placement.activation_dist is not None:
                self.env.process(self._placement_process(placement))
        self.placement_policy[placement.name]["apps"].append(app.name)

        # Add Population control to the App
        if population.name not in list(self.population_policy.keys()):  # First Time
            self.population_policy[population.name] = {"population_policy": population, "apps": []}
            if population.activation_dist is not None:
                self.env.process(self._population_process(population))
        self.population_policy[population.name]["apps"].append(app.name)

        # Add Selection control to the App
        self.selector_path[app.name] = selection

    def deploy_module(self, app_name: str, module_name: str, services: List[Service], node_ids: List[int]):
        if len(services) == 0:
            return []
        else:
            return [self._deploy_module(app_name, module_name, node_id, services) for node_id in node_ids]

    def undeploy_module(self, app_name, service_name, idtopo):
        """Removes all modules deployed in a node
        modules with the same name = service_name
        from app_name
        deployed in id_topo
        """
        all_des = []
        for k, v in list(self.process_to_node.items()):
            if v == idtopo:
                all_des.append(k)

        # Clearing related structures
        for des in self.app_to_module_to_processes[app_name][service_name]:
            if des in all_des:
                self.app_to_module_to_processes[app_name][service_name].remove(des)
                self.stop_process(des)
                del self.process_to_node[des]

    def remove_node(self, id_node_topology):
        # Stopping related processes deployed in the module and clearing main structure: alloc_DES
        des_tmp = []
        if id_node_topology in list(self.process_to_node.values()):
            for k, v in list(self.process_to_node.items()):
                if v == id_node_topology:
                    des_tmp.append(k)
                    self.stop_process(k)
                    del self.process_to_node[k]

        # Clearing other related structures
        for k, v in list(self.app_to_module_to_processes.items()):
            for k2, v2 in list(self.app_to_module_to_processes[k].items()):
                for item in des_tmp:
                    if item in v2:
                        v2.remove(item)

        # Finally removing node from topology
        self.topology.G.remove_node(id_node_topology)

    def print_debug_assignaments(self):
        """Prints debug information about the assignment of DES process - Topology ID - Source Module or Modules"""
        fullAssignation = {}

        for app in self.app_to_module_to_processes:
            for module in self.app_to_module_to_processes[app]:
                deployed = self.app_to_module_to_processes[app][module]
                for des in deployed:
                    fullAssignation[des] = {"ID": self.process_to_node[des], "Module": module}  # DES process are unique for each module/element

        print("-" * 40)
        print("DES\t| TOPO \t| Src.Mod \t| Modules")
        print("-" * 40)
        for k in self.process_to_node:
            print(
                k,
                "\t|",
                self.process_to_node[k],
                "\t|",
                self.alloc_source[k]["name"] if k in list(self.alloc_source.keys()) else "--",
                "\t\t|",
                fullAssignation[k]["Module"] if k in list(fullAssignation.keys()) else "--",
            )
        print("-" * 40)

    def run(self, until: int, results_path: Optional[str] = None, test_initial_deploy: bool = False, progress_bar: bool = True):
        """Runs the simulation

        Args:
            until: Defines a stop time
            results_path: TODO
            test_initial_deploy: TODO
            progress_bar: TODO
        """
        # Creating app.sources and deploy the sources in the topology
        for population in self.population_policy.values():
            for app_name in population["apps"]:
                population["population_policy"].initial_allocation(self, app_name)

        # Creating initial deploy of services
        for placement in self.placement_policy.values():
            for app_name in placement["apps"]:
                placement["placement_policy"].initial_allocation(self, app_name)  # internally consideres the apps in charge

        self.print_debug_assignaments()

        if not test_initial_deploy:
            for i in tqdm(range(1, until), total=until, disable=(not progress_bar)):
                self.env.run(until=i)

        if results_path:
            self.event_log.write(results_path)
