# -*- coding: utf-8 -*-
from typing import Callable, List


class Message:
    """Representation of a request between two modules.

    Args:
        name: Message name, unique for each application
        src: Name of the module who send this message
        dst: Name of the module who received this message
        instructions: Number of instructions to be executed (Instead of MIPS, we use IPt since the time is relative to the simulation units.)
        size: Size in bytes

    Internal args used in the **yafs.core** are:
        timestamp (float): simulation time. Instant of time that was created.
        path (list): a list of entities of the topology that has to travel to reach its target module from its source module.
        dst_int (int): an identifier of the intermediate entity in which it is in the process of transmission.
        app_name (str): the name of the application
    """

    def __init__(self, name: str, src: str, dst: str, instructions: int = 0, size: int = 0, broadcasting: bool = False):
        self.name = name
        self.src = src
        self.dst = dst
        self.instructions = instructions
        self.size = size
        self.broadcasting = broadcasting  # TODO document

        self.timestamp = 0  # TODO Where is this used?
        self.path = []  # TODO Not sure this should be encoded in the message, only the routing can know this?
        self.dst_int = -1  # TODO Understand this
        self.app_name = None  # TODO Remove this, Message should have no knowledge about application
        self.timestamp_rec = 0  # TODO ??

        self.idDES = None  # TODO ??
        self.last_idDes = []  # TODO ??
        self.id = -1  # TODO ??

    def __str__(self):
        return f"Message {self.name} ({self.id}). From (src): {self.src}  to (dst): {self.dst}."


class Application:
    """Defined by a Directed Acyclic Graph (DAG) between modules that generates, processes and receives messages.

    Args:
        name: Application name, unique within the same topology.
    """
    TYPE_SOURCE = "SOURCE"  # Sensor
    TYPE_MODULE = "MODULE"
    TYPE_SINK = "SINK"  # Actuator

    def __init__(self, name: str):
        self.name = name
        self.services = {}  # TODO Document or private
        self.messages = {}  # TODO Document or private
        self.modules = []  # TODO Document or private
        self.modules_src = []  # TODO Document or private
        self.modules_sink = []  # TODO Document or private
        self.data = {}  # TODO Document or private

    def __str__(self):  # TODO Refactor this
        result = f"___ APP. Name: {self.name}"
        result += "\n__ Transmissions "
        for m in list(self.messages.values()):
            result += "\n\tModule: None : M_In: {m.src}  -> M_Out: {m.dst} "

        for modulename in list(self.services.keys()):
            m = self.services[modulename]
            result += f"\n\t{modulename}"
            for ser in m:
                if "message_in" in list(ser.keys()):
                    try:
                        result += f"\t\t M_In: {ser['message_in'].name}  -> M_Out: {ser['message_out'].name} "
                    except:
                        result += f"\t\t M_In: {ser['message_in'].name}  -> M_Out: [NOTHING] "
        return result

    def set_modules(self, data):
        """
        Pure source or sink modules must be typified

        Args:
            data (dict) : a set of characteristic of modules
        """
        for module in data:
            name = list(module.keys())[0]
            type = list(module.values())[0]["Type"]
            if type == self.TYPE_SOURCE:
                self.modules_src.append(name)
            elif type == self.TYPE_SINK:
                self.modules_sink = name

            self.modules.append(name)

        self.data = data

        # self.modules_sink = modules
        # TODO Remove??
    # def set_module(self, modules, type_module):
    #     """
    #     Pure source or sink modules must be typified
    #
    #     Args:
    #         modules (list): a list of modules names
    #         type_module (str): TYPE_SOURCE or TYPE_SINK
    #     """
    #     if type_module == self.TYPE_SOURCE:
    #         self.modules_src = modules
    #     elif type_module == self.TYPE_SINK:
    #         self.modules_sink = modules
    #     elif type_module == self.TYPE_MODULE:
    #         self.modules_pure = modules

    def get_pure_modules(self):
        """Returns a list of pure source and sink modules"""
        return [s for s in self.modules if s not in self.modules_src and s not in self.modules_sink]

    def get_sink_modules(self):
        """Returns a list of sink modules"""
        return self.modules_sink

    def add_source_messages(self, msg):
        """Adds messages that come from pure sources (sensors).  This distinction allows them to be controlled by the (:mod:`Population`) algorithm."""
        self.messages[msg.name] = msg

    def get_message(self, name):
        """Returns a message instance from the identifier name"""
        return self.messages[name]

    def add_service_source(self, module_name: str, distribution: Callable = None, message: Message = None, module_dst: List = None, p: List = None):
        """
        Link to each non-pure module a management for creating messages

        Args:
            module_name: Module name
            distribution: A distribution function
            message: The message
            module_dst: List of modules who can receive this message. Broadcasting.
            p: List of probabilities to send this message. Broadcasting

        Kwargs:
            param_distribution (dict): the parameters for *distribution* function  # TODO ???
        """
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

    def add_service_module(self, module_name: str, message_in, message_out="", distribution="", module_dest=[], p=[], **param):
        # TODO Is message_out of type Message or str?
        # TODO Fix mutable default arguments

        """
        Link to each non-pure module a management of transfering of messages

        Args:
            module_name: module name
            message_in (Message): input message
            message_out (Message): output message. If Empty the module is a sink
            distribution (function): a function with a distribution function
            module_dest (list): a list of modules who can receive this message. Broadcasting.
            p (list): a list of probabilities to send this message. Broadcasting

        Kwargs:
            param (dict): the parameters for *distribution* function

        """
        if not module_name in self.services:
            self.services[module_name] = []

        self.services[module_name].append(
            {
                "type": Application.TYPE_MODULE,
                "dist": distribution,
                "param": param,
                "message_in": message_in,
                "message_out": message_out,
                "module_dest": module_dest,
                "p": p,
            }
        )
