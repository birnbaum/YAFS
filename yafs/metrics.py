import csv
import os
from typing import Dict, List


class EventLog:

    MESSAGE_LOG_FILE = "message_log.csv"
    TRANSMISSION_LOG_FILE = "transmission_log.csv"

    TIME_LATENCY = "time_latency"
    TIME_WAIT = "time_wait"
    TIME_RESPONSE = "time_response"
    TIME_SERVICE = "time_service"
    TIME_TOTAL_RESPONSE = "time_total_response"

    WATT_SERVICE = "byService"
    WATT_UPTIME = "byUptime"

    def __init__(self):
        # TODO The two log files seem to always contain the same number of elements with the same ids. If this is True, merge them into one list
        # TODO Events should be dataclasses?
        self.message_log = []
        self.transmission_log = []

    def load(self, path: str = "results") -> None:
        self.message_log = _load_csv(path, self.MESSAGE_LOG_FILE)
        self.transmission_log = _load_csv(path, self.TRANSMISSION_LOG_FILE)

    def write(self, path: str = "results") -> None:
        _write_csv(path, self.MESSAGE_LOG_FILE, self.message_log)
        _write_csv(path, self.TRANSMISSION_LOG_FILE, self.transmission_log)

    def append_event(self, **kwargs) -> None:
        columns = set(kwargs.keys())
        expected_columns = {"id", "type", "app", "module", "message", "DES_src", "DES_dst", "TOPO_src", "TOPO_dst", "module_src", "service",
                            "time_in", "time_out", "time_emit", "time_reception"}
        if columns != expected_columns:
            raise ValueError(f"Cannot append metrics event:\nExpected columns: {expected_columns}\nGot: {columns}")
        self.message_log.append(kwargs)

    def append_transmission(self, **kwargs) -> None:
        columns = set(kwargs.keys())
        expected_columns = {"id", "type", "src", "dst", "app", "latency", "message", "ctime", "size", "buffer"}
        if columns != expected_columns:
            raise ValueError(f"Cannot append metrics transmission:\nExpected columns: {expected_columns}\nGot: {columns}")
        self.transmission_log.append(kwargs)


def _load_csv(directory: str, filename: str) -> List[Dict]:
    with open(os.path.join(directory, filename)) as f:
        return [dict(row) for row in csv.DictReader(f)]


def _write_csv(directory: str, filename: str, content: List[Dict]) -> None:
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, filename), "w") as f:
        writer = csv.DictWriter(f, fieldnames=content[0].keys())
        writer.writeheader()
        writer.writerows(content)
