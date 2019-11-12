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
from yafs.selection import Selection
from yafs.stats import Stats, EventLog
from yafs.topology import Topology


class SimulationTimeFilter(logging.Filter):

    def __init__(self, env):
        self.env = env

    def filter(self, record):
        record.simulation_time = self.env.now
        return True


logger = logging.getLogger(__name__)
logger.propagate = False
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(simulation_time).4f - %(name)s - %(levelname)s - %(message)s'))


class Simulation:
    """Contains the cloud event-discrete simulation environment and controls the structure variables.

    Args:
        topology: Associated topology of the environment.
    """

    def __init__(self, topology: Topology):
        # TODO Refactor this class. Way too many fields, no clear separation of concerns.

        self.topology = topology

        self.env = simpy.Environment()  # discrete-event simulator (aka DES)
        logger.addFilter(SimulationTimeFilter(self.env))
        logger.addHandler(ch)

        self.env.process(self._network_process())

        self.event_log = EventLog()

        self.deployments = {}  # TODO Should become a list?

        self.network_ctrl_pipe = simpy.Store(self.env)
        self.consumer_pipes = {}  # Queues for each message <application>:<module_name> -> pipe
        self.network_pump = 0  # Shared resource that controls the exchange of messages in the topology

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
        while True:
            message = yield self.network_ctrl_pipe.get()

            # If same SRC and PATH or the message has achieved the penultimate node to reach the dst
            if not message.path or message.path[-1] == message.next_dst or len(message.path) == 1:
                # Timestamp reception message in the module
                message.timestamp_rec = self.env.now
                # The message is sent to the module.pipe
                self.consumer_pipes[f"{message.application.name}:{message.dst.name}"].put(message)
            else:
                # The message is sent at first time or it sent more times.
                if message.next_dst is None:
                    src_int = message.path[0]
                    message.next_dst = message.path[1]
                else:
                    src_int = message.next_dst
                    message.next_dst = message.path[message.path.index(message.next_dst) + 1]
                # arista set by (src_int,message.next_dst)
                link = (src_int, message.next_dst)

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
        # TODO Why do sinks don't have a service time?
        #if module in self.deployments[application].application.sink_modules:  # module is a SINK
        #    service_time = 0
        #else:
        att_node = self.topology.G.nodes[node_id]
        service_time = message.instructions / float(att_node["IPT"])

        self.event_log.append_event(type=type_,
                                    app=application.name,
                                    module=module,
                                    message=message.name,
                                    module_src=message.src,
                                    TOPO_src=message.path[0],
                                    TOPO_dst=node_id,
                                    service=service_time,
                                    time_in=self.env.now,
                                    time_out=service_time + self.env.now,
                                    time_emit=float(message.timestamp),
                                    time_reception=float(message.timestamp_rec))

        return service_time

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

    def _add_consumer_service_pipe(self, application: Application, module_name):
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

    def deploy_source(self, application: Application, node_id: int, message: Message, distribution: Distribution):
        """Add a DES process for deploy pure source modules (sensors). This function its used by (:mod:`Population`) algorithm"""
        process = self.env.process(self._source_process(node_id, application, message, distribution))
        self.process_to_node[process] = node_id
        self.alloc_source[process] = {"id": node_id, "app": application, "module": message.src, "name": message.name}

    def _source_process(self, node_id: int, application: Application, message: Message, distribution: Distribution):
        """Process who controls the invocation of several Pure Source Modules"""
        logger.debug("Added_Process - Module Pure Source")
        while True:
            yield self.env.timeout(next(distribution))
            logger.debug(f"App '{application.name}'\tGenerating Message: {message.name} \t(T:{self.env.now})")
            new_message = message.evolve(timestamp=self.env.now)
            self._send_message(new_message, application, node_id)

    def deploy_sink(self, application: Application, node_id: int, module_name: str):
        """Add a DES process to deploy pure SINK modules (actuators).

        This function its used by the placement algorithm internally, there is no DES PROCESS for this type of behaviour
        """
        process = self.env.process(self._sink_module_process(node_id, application, module_name))
        self.process_to_node[process] = node_id

        self._add_consumer_service_pipe(application, module_name)

        # Update the relathionships among module-entity
        if application in self.app_to_module_to_processes:
            if module_name not in self.app_to_module_to_processes[application]:
                self.app_to_module_to_processes[application][module_name] = []
        self.app_to_module_to_processes[application][module_name].append(process)

    def _sink_module_process(self, node_id, application: Application, module_name):
        """Process associated to a SINK module"""
        logger.debug(f"Added_Process - Module Pure Sink: {module_name}")
        while True:
            message = yield self.consumer_pipes[f"{application.name}:{module_name}"].get()
            logger.debug("(App:%s#%s)\tModule Pure - Sink Message:\t%s" % (application.name, module_name, message.name))
            service_time = self._compute_service_time(application, module_name, message, node_id, "SINK")
            yield self.env.timeout(service_time)  # service time is 0

    def stop_process(self, process: Process):
        """TODO"""
        process.interrupt()
        del self.process_to_node[process]
        for app in self.app_to_module_to_processes:
            for module_name in self.app_to_module_to_processes[app]:
                if process in self.app_to_module_to_processes[app][module_name]:
                    self.app_to_module_to_processes[app][module_name].remove(process)

    def deploy_app(self, app: Application, selection: Selection):
        """This process is responsible for linking the *application* to the different algorithms (placement, population, and service)"""
        deployment = Deployment(application=app, selection=selection)
        self.deployments[app] = deployment
        self.app_to_module_to_processes[app] = {}

    def deploy_placement(self, placement: Placement) -> Process:
        return self.env.process(placement.run(self))

    def deploy_module(self, application: Application, module_name: str, services: List[Service], node_ids: List[int]):
        """Add a DES process for deploy  modules. This function its used by (:mod:`Population`) algorithm."""
        assert len(services) == len(node_ids)  # TODO Does this hold?
        for node_id in node_ids:
            process = self.env.process(self._consumer_process(node_id, application, module_name, services))
            self.process_to_node[process] = node_id

            # To generate the QUEUE of a SERVICE module
            self._add_consumer_service_pipe(application, module_name)

            if module_name not in self.app_to_module_to_processes[application]:  # TODO defaultdict
                self.app_to_module_to_processes[application][module_name] = []
            self.app_to_module_to_processes[application][module_name].append(process)

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

    def remove_node(self, node):
        # TODO Remove
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
        for i in tqdm(range(1, until), total=until, disable=(not progress_bar)):
            self.env.run(until=i)

        if results_path:
            self.event_log.write(results_path)


class Deployment:
    def __init__(self, application: Application, selection: Selection):
        self.application = application
        self.selection = selection
