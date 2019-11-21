"""This module unifies the event-discrete simulation environment with the rest of modules: placement, topology, selection, population, utils and metrics."""

import logging
import time
from typing import Optional, List, Dict, Any

import simpy
from networkx.utils import pairwise, nx
from simpy import Process, Resource
from tqdm import tqdm

from pyfogsim.application import Application, Message, Module
from pyfogsim.placement import Placement
from pyfogsim.selection import Selection
from pyfogsim.stats import Stats, EventLog


class SimulationTimeFilter(logging.Filter):

    def __init__(self, env):
        super().__init__()
        self.env = env

    def filter(self, record):
        record.simulation_time = self.env.now
        return True


logger = logging.getLogger(__name__)
logger.propagate = False
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(simulation_time).4f - %(name)s - %(levelname)s - %(message)s'))


class Simulation:
    """Contains the cloud event-discrete simulation environment and controls the structure variables."""

    def __init__(self, network: nx.Graph, selection: Selection):
        self.env = simpy.Environment()
        logger.addFilter(SimulationTimeFilter(self.env))
        logger.addHandler(ch)
        self.network = self._prepare_network(network)
        self.selection = selection
        self.event_log = EventLog()
        self.apps = []

    @property
    def stats(self):
        return Stats(self.event_log)

    @property
    def node_to_modules(self) -> Dict[Any, List[Module]]:  # Only used in drawing
        """Returns a dictionary mapping from node ids to their deployed services"""
        result = {node: [] for node in self.network}
        for app in self.apps:
            result[app.source.node].append(app.source)
            result[app.sink.node].append(app.sink)
            for operator in app.operators:
                result[operator.node].append(operator)
        return result

    def run(self, until: int, results_path: Optional[str] = None, progress_bar: bool = True):
        """Runs the simulation"""
        start_time = time.time()
        for i in tqdm(range(1, until), total=until, disable=(not progress_bar)):
            self.env.run(until=i)
        if results_path:
            self.event_log.write(results_path)
        logger.info(f"Simulated {until} time units in {time.time() - start_time} seconds.")

    def deploy_app(self, app: Application):
        """This process is responsible for linking the *application* to the different algorithms (placement, population, and service)"""
        self.apps.append(app)
        self.env.process(app.source.run(self, app))

    def deploy_placement(self, placement: Placement) -> Process:
        return self.env.process(placement.run(self))

    def transmission_process(self, message: Message, src_node):
        queue_times = []
        latencies = []
        path = self.selection.get_path(self.network, message, src_node, message.dst.node)
        logger.debug(f"Sending {message} via path {path}.")
        for x, y in pairwise(path):
            link = self.network.edges[x, y]["link"]
            latency = link.latency + message.size / link.bandwidth
            with link.request() as req:
                queue_start = self.env.now
                yield req
                queue_times.append(self.env.now - queue_start)
                yield self.env.timeout(latency)
                latencies.append(latency)
        message.network_queue = sum(queue_times)
        message.network_latency = sum(latencies)
        logger.debug(f"Sent    {message}. Total Latency: {message.network_latency + message.network_queue} ({message.network_queue} due to congestion).")
        self.env.process(message.dst.enter(message, self))

    def _prepare_network(self, network: nx.Graph) -> nx.Graph:
        for node in network:
            node.set_env(self.env)
        for _, _, data in network.edges(data=True):
            data["link"].set_env(self.env)
        return network
