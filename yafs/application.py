import random
from abc import ABC
from copy import copy
from typing import List, Optional, Dict, Any

import logging
from yafs.distribution import Distribution

logger = logging.getLogger(__name__)


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

    def __init__(self, name: str, dst: "Module", instructions: int = 0, size: int = 0):
        self.name = name
        self.dst = dst
        self.instructions = instructions
        self.size = size

        self.timestamp = 0  # TODO Where is this used?
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
            logger.debug(f"{app.name}:{self.name}\tGenerating {self.message_out} \t(T:{simulation.env.now})")
            message = self.message_out.evolve(timestamp=simulation.env.now, application=app)
            # simulation._send_message(message, app, self.node)
            simulation.env.process(simulation.transmission_process(message, self.node))


class Operator(Module):
    """Definition of Generators and Consumers (AppEdges and TupleMappings in iFogSim)

    Args:
        message_in: input message
        message_out: Output message. If Empty the module is a sink
        probability: Probability to process the message
        p: a list of probabilities to send this message. Broadcasting  # TODO Understand and refactor
    """

    def __init__(self, name: str, message_out: "Message", data: Optional[Dict] = None, probability: float = 1.0):
        super().__init__(name, data)
        self.probability = probability
        self.message_out = message_out
        self.node = None  # Not yet deployed

    def enter(self, message: "Message", simulation):
        logger.debug(f"{message} arrived in operator {self.name}.")
        service_time = message.instructions / float(simulation.topology.G.nodes[self.node]["IPT"])

        simulation.event_log.append_event(type="COMP",
                                          app=message.application.name,
                                          module=self,
                                          message=message.name,
                                          module_src=message.application.source,
                                          TOPO_src=message.application.source.node,
                                          TOPO_dst=self.message_out.dst.node,
                                          service=service_time,
                                          time_in=simulation.env.now,
                                          time_out=service_time + simulation.env.now,
                                          time_emit=float(message.timestamp))

        yield simulation.env.timeout(service_time)
        if random.random() <= self.probability:
            message_out = self.message_out.evolve(timestamp=simulation.env.now, application=message.application)
            logger.debug(f"{self.name}\tTransmit\t{self.message_out.name}")
            simulation.env.process(simulation.transmission_process(message_out, self.node))
        else:
            logger.debug(f"{self.name}\tDenied\t{self.message_out.name}")


class Sink(Module):
    # TODO Missing message in??
    def __init__(self, name: str, node: Any, data: Optional[Dict] = None):
        super().__init__(name, data)
        self.node = node

    def enter(self, message: "Message", simulation):
        logger.debug(f"{message} arrived in sink {self.name}")

        simulation.event_log.append_event(type="SINK",
                                          app=message.application.name,
                                          module=self,
                                          message=message.name,
                                          module_src=message.application.source,
                                          TOPO_src=message.application.source.node,
                                          TOPO_dst=None,
                                          service=0,
                                          time_in=simulation.env.now,
                                          time_out=0 + simulation.env.now,
                                          time_emit=float(message.timestamp))
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
