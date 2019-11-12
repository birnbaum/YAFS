"""This module unifies the event-discrete simulation environment with the rest of modules: placement, topology, selection, population, utils and metrics."""

import logging
from collections import Callable
from typing import Optional, List, Dict, Any

import simpy
from networkx.utils import pairwise, nx
from simpy import Process
from tqdm import tqdm

from yafs.application import Application, Message, Operator
from yafs.distribution import Distribution
from yafs.placement import Placement
from yafs.selection import Selection
from yafs.stats import Stats, EventLog


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

    def __init__(self, G: nx.Graph, selection: Selection):
        # TODO Refactor this class. Way too many fields, no clear separation of concerns.

        self.G = G
        self.selection = selection
        self.env = simpy.Environment()

        logger.addFilter(SimulationTimeFilter(self.env))
        logger.addHandler(ch)

        self.event_log = EventLog()

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

            {Application(...):{"Controller": 1, "Client": 4}}
        """
        self.app_to_module_to_process = {}

        """Relationship between DES process and topology.node.id

        It is necessary to identify the message.source (topology.node)
        1.N. DES process -> 1. topology.node
        """
        self.process_to_node = {}

        # This variable control the lag of each busy network links. It avoids the generation of a DES-process for each link
        # edge -> last_use_channel (float) = Simulation time
        self.last_busy_time = {}  # must be updated with up/down node

    def transmission_process(self, message: Message, src_node):
        paths = self.selection.get_paths(self.G, message, src_node, [message.dst.node])
        for path in paths:
            latencies = [self.G.edges[x, y]["PR"] for x, y in pairwise(path)]
            total_latency = sum(latencies)
            logger.debug(f"Sending {message} via path {path}. Latency: {total_latency}({latencies})")
            yield self.env.timeout(total_latency)
            self.event_log.append_transmission(src=path[0],
                                               dst=path[-1],
                                               app=message.application.name,
                                               latency=total_latency,
                                               message=message.name,
                                               ctime=self.env.now,
                                               size=message.size,
                                               buffer=self.network_pump)
            self.env.process(message.dst.enter(message, self))

    @property
    def stats(self):
        return Stats(self.event_log)

    @property
    def node_to_modules(self) -> Dict[int, List]:  # Only used in drawing
        """Returns a dictionary mapping from node ids to their deployed services"""
        result = {key: [] for key in self.G.nodes}
        for src_deployed in self.alloc_source.values():
            result[src_deployed["id"]].append(src_deployed["app"].name + "#" + src_deployed["module"].name)
        for app in self.app_to_module_to_process:
            for module_name in self.app_to_module_to_process[app]:
                process = self.app_to_module_to_process[app][module_name]
                result[self.process_to_node[process]].append(app.name + "#" + module_name)
        return result

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
                self.consumer_pipes[f"{message.application.name}:{message.application.sink.name}"].put(message)
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
                transmit = message.size / (self.G.edges[link][Topology.LINK_BW] * 1000000.0)  # MBITS!
                propagation = self.G.edges[link][Topology.LINK_PR]
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

    def __wait_message(self, message, latency, shift_time):
        """Simulates the transfer behavior of a message on a link"""
        self.network_pump += 1
        yield self.env.timeout(latency + shift_time)
        self.network_pump -= 1
        self.network_ctrl_pipe.put(message)

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
            logger.debug("\n\nRemoving node: %i, Total nodes: %i" % (node, len(self.G)))
            self.remove_node(node)
            for process in processes:
                self.stop_process(process)

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

    def stop_process(self, process: Process):
        """TODO"""
        process.interrupt()
        del self.process_to_node[process]
        for app in self.app_to_module_to_process:
            for module_name in self.app_to_module_to_process[app]:
                p = self.app_to_module_to_process[app][module_name]
                if p == process:
                    del self.app_to_module_to_process[app][module_name]

    def deploy_app(self, app: Application):
        """This process is responsible for linking the *application* to the different algorithms (placement, population, and service)"""
        self.app_to_module_to_process[app] = {}
        self._deploy_source(app)
        self._deploy_sink(app)

    def _deploy_source(self, application: Application):
        """Add a DES process for deploy pure source modules (sensors). This function its used by (:mod:`Population`) algorithm"""
        process = self.env.process(application.source.run(self, application))
        self.process_to_node[process] = application.source.node
        self.alloc_source[process] = {"id": application.source.node, "app": application, "module": application.source, "name": application.source.message_out.name}

    def _deploy_sink(self, application: Application):
        """Add a DES process to deploy pure SINK modules (actuators).

        This function its used by the placement algorithm internally, there is no DES PROCESS for this type of behaviour
        """
        # process = self.env.process(application.sink.run(self, application))
        # self.process_to_node[process] = application.sink.node
        # self._add_consumer_service_pipe(application, application.sink.name)
        # self.app_to_module_to_process[application][application.sink.name] = process

    def deploy_operator(self, application: Application, operator: Operator, node: Any):
        """Add a DES process for deploy  modules. This function its used by (:mod:`Population`) algorithm."""
        # process = self.env.process(operator.run(self, application, node))
        # self.process_to_node[process] = node
        # self._add_consumer_service_pipe(application, operator.name)
        # self.app_to_module_to_process[application][operator.name] = process

    def deploy_placement(self, placement: Placement) -> Process:
        return self.env.process(placement.run(self))

    def remove_node(self, node):
        # TODO Remove
        # Stopping related processes deployed in the module and clearing main structure: alloc_DES
        if node in list(self.process_to_node.values()):
            for process, p_node in list(self.process_to_node.items()):
                if p_node == node:
                    self.stop_process(process)

        # Finally removing node from topology
        self.G.remove_node(node)

    def print_debug_assignaments(self):
        """Prints debug information about the assignment of DES process - Topology ID - Source Module or Modules"""
        fullAssignation = {}

        for app in self.app_to_module_to_process:
            for module in self.app_to_module_to_process[app]:
                des = self.app_to_module_to_process[app][module]
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
