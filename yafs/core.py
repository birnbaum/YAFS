"""This module unifies the event-discrete simulation environment with the rest of modules: placement, topology, selection, population, utils and metrics."""

import logging
import random
from collections import Callable
from typing import Optional, List, Dict

import simpy
from simpy import Process
from tqdm import tqdm

from yafs.application import Application, Message, Service
from yafs.distribution import Distribution
from yafs.placement import Placement
from yafs.population import Population
from yafs.selection import Selection
from yafs.stats import Stats, EventLog
from yafs.topology import Topology

logger = logging.getLogger(__name__)


class Simulation:
    """Contains the cloud event-discrete simulation environment and controls the structure variables.

    Args:
        topology: Associated topology of the environment.
    """

    def __init__(self, topology: Topology):
        # TODO Refactor this class. Way too many fields, no clear separation of concerns.

        self.topology = topology

        self.env = simpy.Environment()  # discrete-event simulator (aka DES)
        self.env.process(self._network_process())
        self.network_ctrl_pipe = simpy.Store(self.env)

        self.network_pump = 0  # Shared resource that controls the exchange of messages in the topology

        self.event_log = EventLog()

        self.deployments = {}  # TODO Should become a list?

        self.placement_policy = {}  # for app the placement algorithm
        self.population_policy = {}  # for app the population algorithm

        # Queues for each message
        # <application>:<module_name> -> pipe
        self.consumer_pipes = {}

        """Relationship of pure source with topology entity

        id.source.process -> value: dict("id","app","module")

          .. code-block:: python

            alloc_source[34] = {"id":id_node,"app":app,"module":source module}
        """
        self.alloc_source = {}

        """Represents the deployment of a module in a DES PROCESS each DES has a one topology.node.id (see alloc_des var.)

        It used for (:mod:`Placement`) class interaction.

        A dictionary where the key is an app.name and value is a dictionary with key is a module and value an array of id DES process

        .. code-block:: python

            {Application(...):{"Controller":[1,3,4],"Client":[4]}}
        """
        self.app_to_module_to_processes = {}

        """Relationship between DES process and topology.node.id

        It is necessary to identify the message.source (topology.node)
        1.N. DES process -> 1. topology.node
        """
        self.process_to_node = {}

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
            result[src_deployed["id"]].append(src_deployed["app"].name + "#" + src_deployed["module"].name)
        for app in self.app_to_module_to_processes:
            for module_name in self.app_to_module_to_processes[app]:
                for process in self.app_to_module_to_processes[app][module_name]:
                    result[self.process_to_node[process]].append(app.name + "#" + module_name)
        return result

    # TODO This might have a bug
    def process_from_module_in_node(self, node, application: Application, module_name):
        deployed = self.app_to_module_to_processes[application][module_name]
        for des in deployed:
            if self.process_to_node[des] == node:
                return des
        return []

    def assigned_structured_modules_from_process(self):  # TODO Remove as process dependant
        full_assignation = {}
        for app in self.app_to_module_to_processes:
            for module in self.app_to_module_to_processes[app]:
                deployed = self.app_to_module_to_processes[app][module]
                for des in deployed:
                    full_assignation[des] = {"DES": self.process_to_node[des], "module": module}
        return full_assignation

    def _send_message(self, message: Message, application: Application, src_node: int):
        """Sends a message between modules and updates the metrics once the message reaches the destination module"""
        selection = self.deployments[application].selection
        dst_processes = self.app_to_module_to_processes[application][message.dst.name]
        dst_nodes = [self.process_to_node[dev] for dev in dst_processes]

        paths = selection.get_paths(self.topology.G, message, src_node, dst_nodes)
        for path in paths:
            logger.debug(f"Application {application.name} sending {message} via path {path}")
            new_message = message.evolve(path=path, application=application)
            self.network_ctrl_pipe.put(new_message)

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
                # Timestamp reception message in the module
                message.timestamp_rec = self.env.now
                # The message is sent to the module.pipe
                self.consumer_pipes[f"{message.application.name}:{message.dst.name}"].put(message)
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

                self.event_log.append_transmission(src=link[0],
                                                   dst=link[1],
                                                   app=message.application.name,
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
                #     paths, DES_dst = self.deployments[message.application].selection.get_path_from_failure(
                #         self, message, link, self.alloc_DES, self.alloc_module, self.last_busy_time, self.env.now
                #     )
                #
                #     if DES_dst == [] and paths == []:
                #         # Message communication ending: The message have arrived to the destination node but it is unavailable.
                #         logger.debug("\t No path given. Message is lost")
                #     else:
                #         message.path = copy.copy(paths[0])
                #         logger.debug("(\t New path given. Message is enrouting again.")
                #         self.network_ctrl_pipe.put(message)

    def __wait_message(self, message, latency, shift_time):
        """Simulates the transfer behavior of a message on a link"""
        self.network_pump += 1
        yield self.env.timeout(latency + shift_time)
        self.network_pump -= 1
        self.network_ctrl_pipe.put(message)

    def _compute_service_time(self, application: Application, module, message, node_id, type_):
        """Computes the service time in processing a message and record this event"""
        if module in self.deployments[application].application.sink_modules:  # module is a SINK
            time_service = 0
        else:
            att_node = self.topology.G.nodes[node_id]
            time_service = message.instructions / float(att_node["IPT"])

        self.event_log.append_event(type=type_,
                                    app=application.name,
                                    module=module,
                                    message=message.name,
                                    module_src=message.src,
                                    TOPO_src=message.path[0],
                                    TOPO_dst=node_id,
                                    service=time_service,
                                    time_in=self.env.now,
                                    time_out=time_service + self.env.now,
                                    time_emit=float(message.timestamp),
                                    time_reception=float(message.timestamp_rec))

        return time_service

    def deploy_node_failure_generator(self, nodes: List[int], distribution: Distribution, logfile: Optional[str] = None) -> None:
        self.env.process(self._node_failure_generator(nodes, distribution, logfile))

    def _node_failure_generator(self, nodes: List[int], distribution: Distribution, logfile: Optional[str] = None):
        """Controls the elimination of nodes"""
        logger.debug(f"Adding Process: Node Failure Generator<nodes={nodes}, distribution={distribution}>")
        for node in nodes:
            yield self.env.timeout(next(distribution))
            processes = [k for k, v in self.process_to_node.items() if v == node]  # A node can host multiples DES processes
            if logfile:
                with open(logfile, "a") as stream:
                    stream.write("%i,%s,%d\n" % (node, len(processes), self.env.now))
            logger.debug("\n\nRemoving node: %i, Total nodes: %i" % (node, len(self.topology.G)))
            self.remove_node(node)
            for process in processes:
                self.stop_process(process)

    def _sink_module_process(self, node_id, application: Application, module_name):
        """Process associated to a SINK module"""
        logger.debug(f"Added_Process - Module Pure Sink: {module_name}")
        while True:
            message = yield self.consumer_pipes[f"{application.name}:{module_name}"].get()
            logger.debug("(App:%s#%s)\tModule Pure - Sink Message:\t%s" % (application.name, module_name, message.name))
            service_time = self._compute_service_time(application, module_name, message, node_id, "SINK")
            yield self.env.timeout(service_time)  # service time is 0

    def __add_consumer_service_pipe(self, application: Application, module_name):
        pipe_key = f"{application.name}:{module_name}"
        logger.debug("Creating PIPE: " + pipe_key)
        self.consumer_pipes[pipe_key] = simpy.Store(self.env)

    # TODO What is this used for?
    def deploy_monitor(self, name: str, function: Callable, distribution: Callable, **param):
        """Add a DES process for user purpose

        Args:
            name: name of monitor
            function: function that will be invoked within the simulator with the user's code
            distribution: a temporary distribution function

        Kwargs:
            param (dict): the parameters of the *distribution* function
        """
        self.env.process(self._monitor_process(name, function, distribution, **param))

    def _monitor_process(self, name, function, distribution, **param):
        """Process for user purpose"""
        logger.debug(f"Added_Process - Internal Monitor: {name}")
        while True:
            yield self.env.timeout(next(distribution))
            function(**param)

    def deploy_source(self, application: Application, node_id: int, message: Message, distribution: Distribution) -> int:
        """Add a DES process for deploy pure source modules (sensors)
        This function its used by (:mod:`Population`) algorithm

        Args:
            application: application TODO
            node_id: entity.id of the topology who will create the messages
            message: TODO
            distribution (function): a temporary distribution function

        Kwargs:
            param - the parameters of the *distribution* function  # TODO ???

        Returns:
            Process id
        """
        process = self.env.process(self._source_process(node_id, application, message, distribution))
        self.process_to_node[process] = node_id
        self.alloc_source[process] = {"id": node_id, "app": application, "module": message.src, "name": message.name}
        return process

    def _source_process(self, node_id: int, application: Application, message: Message, distribution: Distribution):
        """Process who controls the invocation of several Pure Source Modules"""
        logger.debug("Added_Process - Module Pure Source")
        while True:
            yield self.env.timeout(next(distribution))
            logger.debug(f"App '{application.name}'\tGenerating Message: {message.name} \t(T:{self.env.now})")
            new_message = message.evolve(timestamp=self.env.now)
            self._send_message(new_message, application, node_id)

    # TODO Rename
    def _deploy_module(self, application: Application, module: str, node_id: int, services: List[Service]) -> int:
        """Add a DES process for deploy  modules
        This function its used by (:mod:`Population`) algorithm

        Args:
            application: application TODO
            node_id: entity.id of the topology who will create the messages
            module: module name
            services: TODO

        Returns:
            Process id
        """
        process = self.env.process(self._consumer_process(node_id, application, module, services))
        self.process_to_node[process] = node_id

        # To generate the QUEUE of a SERVICE module
        self.__add_consumer_service_pipe(application, module)

        if module not in self.app_to_module_to_processes[application]:  # TODO defaultdict
            self.app_to_module_to_processes[application][module] = []
        self.app_to_module_to_processes[application][module].append(process)
        return process

    def _consumer_process(self, node_id: int, application: Application, module_name: str, services: List[Service]):
        """Process associated to a compute module"""
        logger.debug(f"Added_Process - Module Consumer: {module_name}")
        while True:
            pipe_id = f"{application.name}:{module_name}"
            message = yield self.consumer_pipes[pipe_id].get()
            accepting_services = [s for s in services if message.name == s.message_in.name]

            if accepting_services:
                logger.debug(f"{pipe_id}\tRecording message\t{message.name}")
                service_time = self._compute_service_time(application, module_name, message, node_id, "COMP")
                yield self.env.timeout(service_time)

            for service in accepting_services:  # Processing the message
                if not service.message_out:
                    logger.debug(f"{application.name}:{module_name}\tSink message\t{message.name}")
                    continue

                if random.random() <= service.probability:
                    message_out = service.message_out.evolve(timestamp=self.env.now)
                    if not service.module_dst:
                        # it is not a broadcasting message
                        logger.debug(f"{application.name}:{module_name}\tTransmit message\t{service.message_out.name}")
                        self._send_message(message_out, application, node_id)
                    else:
                        # it is a broadcasting message
                        logger.debug(f"{application.name}:{module_name}\tBroadcasting message\t{service.message_out.name}")
                        for idx, module_dst in enumerate(service.module_dst):
                            if random.random() <= service.p[idx]:
                                self._send_message(message_out, application, node_id)
                else:
                    logger.debug(f"{application.name}:{module_name}\tDenied message\t{service.message_out.name}")

    def deploy_sink(self, application: Application, node_id: int, module: str):
        """Add a DES process to deploy pure SINK modules (actuators).

        This function its used by the placement algorithm internally, there is no DES PROCESS for this type of behaviour

        Args:
            application: application TODO
            node_id: entity.id of the topology who will create the messages
            module: module
        """
        process = self.env.process(self._sink_module_process(node_id, application, module))
        self.process_to_node[process] = node_id

        self.__add_consumer_service_pipe(application, module)

        # Update the relathionships among module-entity
        if application in self.app_to_module_to_processes:
            if module not in self.app_to_module_to_processes[application]:
                self.app_to_module_to_processes[application][module] = []
        self.app_to_module_to_processes[application][module].append(process)

    def stop_process(self, process: Process):
        """TODO"""
        process.interrupt()
        del self.process_to_node[process]
        for app in self.app_to_module_to_processes:
            for module_name in self.app_to_module_to_processes[app]:
                if process in self.app_to_module_to_processes[app][module_name]:
                    self.app_to_module_to_processes[app][module_name].remove(process)

    def deploy_app(self, app: Application, placement: Placement, population: Population, selection: Selection):
        """This process is responsible for linking the *application* to the different algorithms (placement, population, and service)"""
        deployment = Deployment(application=app, placement=placement, population=population, selection=selection)
        self.deployments[app] = deployment

        self.app_to_module_to_processes[app] = {}

        # Add Placement controls to the App
        self._deploy_placement(placement)
        self.placement_policy[placement.name]["apps"].append(app)

        # Add Population control to the App
        self._deploy_population(population)
        self.population_policy[population.name]["apps"].append(app)

    def _deploy_placement(self, placement):
        if placement.name not in list(self.placement_policy.keys()):  # First Time
            self.placement_policy[placement.name] = {"placement_policy": placement, "apps": []}
            if placement.activation_dist is not None:
                self.env.process(self._placement_process(placement))

    def _deploy_population(self, population):
        if population.name not in list(self.population_policy.keys()):  # First Time
            self.population_policy[population.name] = {"population_policy": population, "apps": []}
            if population.activation_dist is not None:
                self.env.process(self._population_process(population))

    def _placement_process(self, placement):
        """Controls the invocation of Placement.run"""
        logger.debug("Added_Process - Placement Algorithm")
        while True:
            yield self.env.timeout(placement.get_next_activation())
            logger.debug("Run - Placement Policy")
            placement.run(self)

    def _population_process(self, population):
        """Controls the invocation of Population.run"""
        logger.debug("Added_Process - Population Algorithm")
        while True:
            yield self.env.timeout(population.get_next_activation())
            logger.debug("Run - Population Policy")
            population.run(self)

    def deploy_module(self, application: Application, module_name: str, services: List[Service], node_ids: List[int]):
        assert len(services) == len(node_ids)  # TODO Does this hold?
        if len(services) == 0:
            return []
        else:
            return [self._deploy_module(application, module_name, node_id, services) for node_id in node_ids]

    def undeploy_module(self, application: Application, service_name, idtopo):  # TODO is this used?
        """Removes all modules deployed in a node
        modules with the same name = service_name
        from app_name
        deployed in id_topo
        TODO
        """
        all_des = []
        for k, v in list(self.process_to_node.items()):
            if v == idtopo:
                all_des.append(k)

        # Clearing related structures
        for process in self.app_to_module_to_processes[application][service_name]:
            if process in all_des:
                self.stop_process(process)

    def remove_node(self, node):
        # Stopping related processes deployed in the module and clearing main structure: alloc_DES
        if node in list(self.process_to_node.values()):
            for process, p_node in list(self.process_to_node.items()):
                if p_node == node:
                    self.stop_process(process)

        # Finally removing node from topology
        self.topology.G.remove_node(node)

    def print_debug_assignaments(self):
        """Prints debug information about the assignment of DES process - Topology ID - Source Module or Modules"""
        fullAssignation = {}

        for app in self.app_to_module_to_processes:
            for module in self.app_to_module_to_processes[app]:
                deployed = self.app_to_module_to_processes[app][module]
                for des in deployed:
                    fullAssignation[des] = {"ID": self.process_to_node[des], "Module": module}  # DES process are unique for each module/element

        print("-" * 40)
        print("TOPO \t| Src.Mod \t| Modules")
        print("-" * 40)
        for k in self.process_to_node:
            print(
                self.process_to_node[k],
                "\t\t|",
                self.alloc_source[k]["name"] if k in list(self.alloc_source.keys()) else "--",
                "\t\t|",
                fullAssignation[k]["Module"] if k in list(fullAssignation.keys()) else "--",
            )
        print("-" * 40)

    def run(self, until: int, results_path: Optional[str] = None, progress_bar: bool = True):
        """Runs the simulation

        Args:
            until: Defines a stop time
            results_path: TODO
            progress_bar: TODO
        """
        # Creating app.sources and deploy the sources in the topology
        for population in self.population_policy.values():
            for app in population["apps"]:
                population["population_policy"].initial_allocation(self, app)

        # Creating initial deploy of services
        for placement in self.placement_policy.values():
            for app in placement["apps"]:
                placement["placement_policy"].initial_allocation(self, app)  # internally consideres the apps in charge

        self.print_debug_assignaments()

        for i in tqdm(range(1, until), total=until, disable=(not progress_bar)):
            self.env.run(until=i)

        if results_path:
            self.event_log.write(results_path)


class Deployment:
    def __init__(self, application: Application, placement: Placement, population: Population, selection: Selection):
        self.application = application
        self.placement = placement
        self.population = population
        self.selection = selection