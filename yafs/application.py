from copy import copy
from typing import List, Optional, Dict


class Service:
    """Definition of Generators and Consumers (AppEdges and TupleMappings in iFogSim)

    Args:
        message_in: input message
        message_out: Output message. If Empty the module is a sink
        probability: Probability to process the message
        p: a list of probabilities to send this message. Broadcasting  # TODO Understand and refactor
        module_dst: a list of modules who can receive this message. Broadcasting.
    """
    def __init__(self, message_in: "Message", message_out: "Message", probability: float = 1.0, p: Optional[List] = None,
                 module_dst: Optional[List] = None):
        self.message_in = message_in
        self.message_out = message_out
        self.probability = probability
        self.p = p if p else []
        self.module_dst = module_dst if module_dst else []


class Module:
    def __init__(self, name: str, is_source: bool = False, is_sink: bool = False, data: Optional[Dict] = None):
        self.name = name
        self.services = []  # can deal with different messages, "tuppleMapping (iFogSim)"
        self.is_source = is_source
        self.is_sink = is_sink
        self.data = data if data else {}  # TODO find better name

    def add_service(self, message_in: "Message", message_out: "Message", probability: float = 1.0, p: Optional[List] = None,
                    module_dst: Optional[List] = None):
        self.services.append(Service(message_in, message_out, probability, p, module_dst))

    def __str__(self):
        is_source = ", is_source=True" if self.is_source else ""
        is_sink = ", is_sink=True" if self.is_sink else ""
        return f"Module<name=\"{self.name}\"{is_source}{is_sink}>"


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
        dst_int (int): an identifier of the intermediate entity in which it is in the process of transmission.
        app_name (str): the name of the application
    """

    def __init__(self, name: str, src: Module, dst: Module, instructions: int = 0, size: int = 0, broadcasting: bool = False):
        self.name = name
        self.src = src
        self.dst = dst
        self.instructions = instructions  # TODO ??
        self.size = size
        self.broadcasting = broadcasting  # TODO document

        self.timestamp = 0  # TODO Where is this used?
        self.path = []  # TODO Not sure this should be encoded in the message, only the routing can know this?
        self.dst_int = -1  # TODO Understand this
        self.app_name = None  # TODO Remove this, Message should have no knowledge about application
        self.timestamp_rec = 0  # TODO ??

        self.id = -1  # TODO ??

    def __str__(self):
        return f"Message<name=\"{self.name}\", id=\"{self.id}\", src=\"{self.src.name}\", dst=\"{self.dst.name}\">"

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

    def __init__(self, name: str, modules: List[Module]):
        self.name = name
        self.modules = modules
        self.messages = {}  # TODO Only used in Population, should probably be removed

    def __str__(self):  # TODO Refactor this
        result = f"---- APP: \"{self.name}\"\n"
        result += "\n- ".join([str(m) for m in self.modules])
        result += "----"
        result += "\n- ".join([str(m) for m in self.messages])
        return result

    @property
    def src_modules(self):
        return [module for module in self.modules if module.is_source]

    @property
    def service_modules(self):
        return [module for module in self.modules if module.services]

    @property
    def sink_modules(self):
        return [module for module in self.modules if module.is_sink]

    def add_source_message(self, message):
        """Adds messages that come from pure sources (sensors).  This distinction allows them to be controlled by the (:mod:`Population`) algorithm."""
        # Defining which messages will be dynamically generated # the generation is controlled by Population algorithm
        # TODO Check
        self.messages[message.name] = message
