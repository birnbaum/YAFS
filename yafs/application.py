from abc import ABC
from copy import copy
from typing import List, Optional, Dict, Any

import logging

from yafs.distribution import Distribution


logger = logging.getLogger(__name__)


class Module(ABC):
    def __init__(self, name: str, data: Optional[Dict] = None):
        self.name = name
        self.data = data if data else {}  # TODO find better name


class Source(Module):
    def __init__(self, name: str, node: Any, message: "Message", distribution: Distribution, data: Optional[Dict] = None):
        super().__init__(name, data)
        self.node = node
        self.message = message
        self.distribution = distribution

    def run(self, simulation: "Simulation", application: "Application"):
        logger.debug("Added_Process - Module Pure Source")
        while True:
            yield simulation.env.timeout(next(self.distribution))
            logger.debug(f"{application.name}:{self.name}\tGenerating Message: {self.message.name} \t(T:{simulation.env.now})")
            new_message = self.message.evolve(timestamp=simulation.env.now)
            simulation._send_message(new_message, application, self.node)


class Operator(Module):
    """Definition of Generators and Consumers (AppEdges and TupleMappings in iFogSim)

    Args:
        message_in: input message
        message_out: Output message. If Empty the module is a sink
        probability: Probability to process the message
        p: a list of probabilities to send this message. Broadcasting  # TODO Understand and refactor
    """

    def __init__(self, name: str, message_in: "Message", message_out: "Message", data: Optional[Dict] = None, probability: float = 1.0):
        super().__init__(name, data)
        self.probability = probability
        self.message_in = message_in
        self.message_out = message_out


class Sink(Module):
    def __init__(self, name: str, node: Any, data: Optional[Dict] = None):
        super().__init__(name, data)
        self.node = node

    def run(self, simulation: "Simulation", application: "Application"):
        logger.debug(f"Added_Process - Module Pure Sink: {self.name}")
        while True:
            message = yield simulation.consumer_pipes[f"{application.name}:{self.name}"].get()
            logger.debug(f"{application.name}:{self.name}\tSink Message: {message.name} \t(T:{simulation.env.now})")
            service_time = simulation._compute_service_time(application, self.name, message, self.node, "SINK")
            yield simulation.env.timeout(service_time)  # service time is 0


class Message:
    """Representation of a request between two modules.

    Args:
        name: Message name, unique for each application
        src: Name of the module who sent this message
        dst: Name of the module who receives this message
        instructions: Number of instructions to be executed (Instead of MIPS, we use IPt since the time is relative to the simulation units.)
        size: Size in bytes

    Internal args used in the **yafs.core** are:
        timestamp (float): simulation time. Instant of time that was created.
        path (list): a list of entities of the topology that has to travel to reach its target module from its source module.
        next_dst (int): an identifier of the intermediate entity in which it is in the process of transmission.
        app_name (str): the name of the application
    """

    def __init__(self, name: str, instructions: int = 0, size: int = 0):
        self.name = name
        self.instructions = instructions
        self.size = size

        self.timestamp = 0  # TODO Where is this used?
        self.path = []  # TODO Not sure this should be encoded in the message, only the routing can know this?
        self.next_dst = None  # TODO Understand this
        self.application = None  # TODO Remove this, Message should have no knowledge about application
        self.timestamp_rec = 0  # TODO ??

    def __str__(self):
        return f"Message<name=\"{self.name}\">"

    def evolve(self, **kwargs) -> "Message":
        message = copy(self)
        for key, value in kwargs.items():
            setattr(message, key, value)
        return message


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
