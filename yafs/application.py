from collections import defaultdict
from typing import Callable, List, Optional, Dict


class Module:
    def __init__(self, name: str, is_source: bool = False, is_sink: bool = False, data: Optional[Dict] = None):
        self.name = name
        self.is_source = is_source
        self.is_sink = is_sink
        self.data = data if data else {}  # TODO find better name

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

        self.process_id = None  # TODO ??
        self.last_idDes = []  # TODO ??
        self.id = -1  # TODO ??

    def __str__(self):
        return f"Message<name=\"{self.name}\", id=\"{self.id}\", src=\"{self.src.name}\", dst=\"{self.dst.name}\">"


class Application:
    """Defined by a Directed Acyclic Graph (DAG) between modules that generates, processes and receives messages.

    Args:
        name: Application name, unique within the same topology.
    """

    def __init__(self, name: str, modules: List[Module]):
        self.name = name
        self.modules = modules
        self.services = defaultdict(list)
        self.messages = {}  # TODO Document or private

    def __str__(self):  # TODO Refactor this
        result = f"___ APP. Name: {self.name}"
        result += "\n__ Transmissions "
        for m in list(self.messages.values()):
            result += f"\n\tModule: None : M_In: {m.src}  -> M_Out: {m.dst} "

        for modulename in list(self.services.keys()):
            m = self.services[modulename]
            result += f"\n\t{modulename}"
            for ser in m:
                if "message_in" in list(ser.keys()):
                    try:
                        result += f"\t\t M_In: {ser['message_in'].name}  -> M_Out: {ser['message_out'].name} "
                    except:  # TODO Catch to broad
                        result += f"\t\t M_In: {ser['message_in'].name}  -> M_Out: [NOTHING] "
        return result

    @property
    def src_modules(self):
        return [module for module in self.modules if module.is_source]

    @property
    def sink_modules(self):
        return [module for module in self.modules if module.is_sink]

    def add_source_messages(self, msg):
        """Adds messages that come from pure sources (sensors).  This distinction allows them to be controlled by the (:mod:`Population`) algorithm."""
        # Defining which messages will be dynamically generated # the generation is controlled by Population algorithm
        # TODO Check
        self.messages[msg.name] = msg

    def add_service_source(self, module_name: str, distribution: Callable = None, message: Message = None, module_dst: List = None, p: List = None):
        """Link to each non-pure module a management for creating messages

        Args:
            module_name: Module name
            distribution: A distribution function
            message: The message
            module_dst: List of modules who can receive this message. Broadcasting.
            p: List of probabilities to send this message. Broadcasting

        Kwargs:
            param_distribution (dict): the parameters for *distribution* function  # TODO ???
        """
        # TODO Check
        if not module_dst:
            module_dst = []
        if not p:
            p = []

        if distribution is not None:
            if module_name not in self.services:
                self.services[module_name] = []
            self.services[module_name].append(
                {"type": Application.TYPE_SOURCE, "dist": distribution, "message_out": message, "module_dest": module_dst, "p": p}
            )

    def add_service_module(self, module_name: str, message_in, message_out="", distribution="", module_dst: List = None, p: List = None, **param):
        # TODO Is message_out of type Message or str?
        # TODO Fix mutable default arguments
        # MODULES/SERVICES: Definition of Generators and Consumers (AppEdges and TupleMappings in iFogSim)
        """Link to each non-pure module a management of transfering of messages

        Args:
            module_name: module name
            message_in (Message): input message
            message_out (Message): output message. If Empty the module is a sink
            distribution (function): a function with a distribution function
            module_dst (list): a list of modules who can receive this message. Broadcasting.
            p (list): a list of probabilities to send this message. Broadcasting

        Kwargs:
            param (dict): the parameters for *distribution* function

        """
        self.services[module_name].append(
            {
                "type": Application.TYPE_MODULE,
                "dist": distribution,
                "param": param,
                "message_in": message_in,
                "message_out": message_out,
                "module_dest": module_dst,
                "p": p,
            }
        )

    TYPE_SOURCE = "SOURCE"  # Sensor
    TYPE_MODULE = "MODULE"
    TYPE_SINK = "SINK"  # Actuator

