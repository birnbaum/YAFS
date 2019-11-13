"""This module unifies the event-discrete simulation environment with the rest of modules: placement, topology, selection, population, utils and metrics."""

import logging
from collections import Callable
from typing import Optional, List, Dict, Any

import simpy
from networkx.utils import pairwise, nx
from simpy import Process, Resource
from tqdm import tqdm

from yafs.application import Application, Message, Operator, Module
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
        self.env = simpy.Environment()
        logger.addFilter(SimulationTimeFilter(self.env))
        logger.addHandler(ch)
        resources = {node: Resource(self.env) for node in G}
        nx.set_node_attributes(G, resources, "resource")
        self.G = G
        self.selection = selection
        self.event_log = EventLog()

        self.apps = []

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
    def node_to_modules(self) -> Dict[Any, List[Module]]:  # Only used in drawing
        """Returns a dictionary mapping from node ids to their deployed services"""
        result = {node: [] for node in self.G}
        for app in self.apps:
            result[app.source.node].append(app.source)
            result[app.sink.node].append(app.sink)
            for operator in app.operators:
                result[operator.node].append(operator)
        return result

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
        self.apps.append(app)
        self.app_to_module_to_process[app] = {}
        self._deploy_source(app)

    def _deploy_source(self, application: Application):
        """Add a DES process for deploy pure source modules (sensors). This function its used by (:mod:`Population`) algorithm"""
        process = self.env.process(application.source.run(self, application))
        self.process_to_node[process] = application.source.node

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
