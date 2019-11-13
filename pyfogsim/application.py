from abc import ABC
from copy import copy
from typing import List, Optional, Dict, Any

import logging
from pyfogsim.distribution import Distribution

logger = logging.getLogger(__name__)


class Message:
    """Representation of a request between two modules.

    Args:
        name: Message name
        dst: Name of the module who receives this message
        instructions: Number of instructions to be executed (Instead of MIPS, we use IPT since the time is relative to the simulation units.)
        size: Size in bytes
    """

    def __init__(self, name: str, dst: "Module", instructions: int = 0, size: int = 0):
        self.name = name
        self.dst = dst
        self.instructions = instructions
        self.size = size

        self.created = None  # Simulation timestamp when the message was created and queued for sending

        self.network_queue = None
        self.network_latency = None
        self.operator_queue = None
        self.operator_processing = None

        self.application = None  # TODO Remove this, Message should have no knowledge about application

    def __str__(self):
        return f"Message(\"{self.name}\")"

    def evolve(self, **kwargs) -> "Message":
        message = copy(self)
        for key, value in kwargs.items():
            setattr(message, key, value)
        return message


class Module(ABC):
    def __init__(self, name: str, data: Optional[Dict] = None):
        self.name = name
        self.data = data if data else {}  # TODO find better name
        self.node = None


class Source(Module):
    def __init__(self, name: str, node: Any, message_out: "Message", distribution: Distribution, data: Optional[Dict] = None):
        super().__init__(name, data)
        self.node = node
        self.message_out = message_out
        self.distribution = distribution

    def run(self, simulation: "Simulation", app: "Application"):
        logger.debug("Added_Process - Source")
        while True:
            yield simulation.env.timeout(next(self.distribution))
            message = self.message_out.evolve(created=simulation.env.now, application=app)
            simulation.env.process(simulation.transmission_process(message, self.node))


class Operator(Module):
    """Definition of Generators and Consumers (AppEdges and TupleMappings in iFogSim)

    Args:
        message_out: Output message. If Empty the module is a sink
    """

    def __init__(self, name: str, message_out: "Message", data: Optional[Dict] = None):
        super().__init__(name, data)
        self.message_out = message_out

    def enter(self, message: "Message", simulation: "Simulation"):
        node_data = simulation.network.nodes[self.node]
        logger.debug(f"{message} arrived in operator {self.name}.")
        service_time = message.instructions / node_data["IPT"]

        with node_data["resource"].request() as req:
            queue_start = simulation.env.now
            yield req
            process_start = simulation.env.now
            yield simulation.env.timeout(service_time)
            # node_data["usage"] += simulation.env.now - process_start
        message.operator_queue = process_start - queue_start
        message.operator_processing = simulation.env.now - process_start

        simulation.event_log.append(app=message.application, module=self, message=message)

        message_out = self.message_out.evolve(created=simulation.env.now, application=message.application)
        simulation.env.process(simulation.transmission_process(message_out, self.node))


class Sink(Module):

    def __init__(self, name: str, node: Any, data: Optional[Dict] = None):
        super().__init__(name, data)
        self.node = node

    def enter(self, message: "Message", simulation):
        logger.debug(f"{message} arrived in sink {self.name}")
        simulation.event_log.append(app=message.application, module=self, message=message)
        return
        yield


class Application:
    """Defined by a Directed Acyclic Graph (DAG) between modules that generates, processes and receives messages.

    Args:
        name: Application name, unique within the same topology.
    """

    def __init__(self, name: str, source: Source, operators: List[Operator], sink: Sink):
        self.name = name
        self.source = source
        self.operators = operators
        self.sink = sink

    # G = nx.node_link_graph({
    #     "directed": False,
    #     "multigraph": False,
    #     "graph": {},
    #     "nodes": [
    #         {"id": "sensor", "node": "sensor1", "distribution": distribution},
    #         {"id": "service_a"},
    #         {"id": "actuator", "node": "actuator1"},
    #     ],
    #     "links": [
    #         {"source": "sensor", "target": "service_a", "instructions": 5, "size": 500},
    #         {"source": "service_a", "target": "actuator", "instructions": 5, "size": 1000},
    #     ]
    # })
    # app = nx.DiGraph(name="App1")
    # app.add_node("source", node="sensor1", distribution=distribution)
    # app.add_node("service_a")
    # app.add_node("actuator", node="sensor1")
    # app.add_edge("sensor", "service_a", instructions=3, size=1000)
    # app.add_edge("service_a", "actuator", instructions=3, size=1000)
    # def from_graph(self, G):
    #     sinks = [node for node in G if G.out_degree(node) == 0]
    #     assert len(sinks) == 1
    #     sink_node = sinks[0]
    #     sink = Sink(name=sink_node, **G.nodes[sink_node])
    #
    #     Message("M.B", dst=actuator, instructions=5, size=500)
    #     G.in_edges


