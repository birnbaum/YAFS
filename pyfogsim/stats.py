import csv
import logging
import os
from typing import List, Dict

import numpy as np
import pandas as pd

from pyfogsim.application import Application, Module, Message

logger = logging.getLogger(__name__)


class EventLog:

    MESSAGE_LOG_FILE = "message_log.csv"

    def __init__(self):
        self.message_log = []

    def load(self, path: str = "results") -> None:
        self.message_log = _load_csv(path, self.MESSAGE_LOG_FILE)

    def write(self, path: str = "results") -> None:
        _write_csv(path, self.MESSAGE_LOG_FILE, self.message_log)

    def append(self, app: Application, module: Module, message: Message) -> None:
        self.message_log.append({
            "app_name": app.name,
            "module_type": module.__class__.__name__,
            "module_name": module.name,
            "node": module.node,
            "message": message.name,
            "instructions": message.instructions,
            "size": message.size,
            "created": message.created,
            "network_queue": message.network_queue,
            "network_latency": message.network_latency,
            "operator_queue": message.operator_queue,
            "operator_processing": message.operator_processing,
        })


# TODO Missing documentation
class Stats:

    def __init__(self, event_log: EventLog):
        self.messages = pd.DataFrame(event_log.message_log)

    def count_messages(self):
        if self.messages.empty:
            return 0
        return len(self.messages)

    def bytes_transmitted(self):
        if self.messages.empty:
            return 0
        return self.messages["size"].sum()

    def utilization(self, id_entity, total_time, from_time=0.0):  # TODO
        if "time_service" not in self.messages.columns:  # cached
            self.messages["time_service"] = self.messages.time_out - self.messages.time_in
        values = self.messages.groupby("DES.dst").time_service.agg("sum")
        return values[id_entity] / total_time

    def times(self, time, value="mean"):
        return self.messages.groupby("message").agg({time: value})

    def message_stats(self):
        resp_msg = self.messages.groupby("message").agg({"time_total_response": ["count", "mean"]})
        resp_msg.columns = resp_msg.columns.droplevel(0)
        return resp_msg

    def print_report(self, total_time):
        print("\n------------ RESULTS ------------")
        print(f"Simulation Time:      {total_time}")
        print(f"Messages transmitted: {self.count_messages()}")
        print(f"Bytes transmitted:    {self.bytes_transmitted()}")
        print()

        if self.messages.empty:
            return
        means = self.messages[["network_queue", "network_latency", "operator_queue", "operator_processing"]].mean()
        print(f"Average message time:  {sum(means):.3f}")
        print(f"- network queue:       {means['network_queue']:.3f}")
        print(f"- network latency:     {means['network_latency']:.3f}")
        print(f"- operator queue:      {means['operator_queue']:.3f}")
        print(f"- operator processing: {means['operator_processing']:.3f}")

    def average_messages_not_transmitted(self):
        return np.mean(self.transmission.buffer)

    def peak_messages_not_transmitted(self):
        return np.max(self.transmission.buffer)

    def messages_not_transmitted(self):
        return self.transmission.buffer[-1:]

    def get_df_modules(self):
        g = self.messages.groupby(["module", "DES.dst"]).agg({"service": ["mean", "sum", "count"]})
        return g.reset_index()

    def get_df_service_utilization(self, service, time):
        """Returns the utilization(%) of a specific module"""
        g = self.messages.groupby(["module", "DES.dst"]).agg({"service": ["mean", "sum", "count"]})
        g.reset_index(inplace=True)
        h = pd.DataFrame()
        h["module"] = g[g.module == service].module
        h["utilization"] = g[g.module == service]["service"]["sum"] * 100 / time
        return h


def _load_csv(directory: str, filename: str) -> List[Dict]:
    with open(os.path.join(directory, filename)) as f:
        return [dict(row) for row in csv.DictReader(f)]


def _write_csv(directory: str, filename: str, content: List[Dict]) -> None:
    if len(content) == 0:
        logger.warning("No stats to write: Empty content.")
        return
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, filename), "w") as f:
        writer = csv.DictWriter(f, fieldnames=content[0].keys())
        writer.writeheader()
        writer.writerows(content)
