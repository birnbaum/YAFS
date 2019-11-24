from contextlib import contextmanager
from copy import copy

from simpy import Environment, Resource


class MonitoredResource(Resource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue_over_time = []
        self.start = None
        self.usage_log = []

    @property
    def usage(self):
        log = copy(self.usage_log)
        if self.start is not None:
            log.append((self.start, self._env.now))
        return sum(e - s for s, e in log) / self._env.now

    def request(self, *args, **kwargs):
        self.queue_over_time.append((self._env.now, len(self.queue)))
        if self.start is None:
            self.start = self._env.now
        return super().request(*args, **kwargs)

    def release(self, *args, **kwargs):
        self.queue_over_time.append((self._env.now, len(self.queue)))
        if self.start is not None and len(self.queue) == 0:
            self.usage_log.append((self.start, self._env.now))
            self.start = None
        return super().release(*args, **kwargs)


class Link:
    def __init__(self, bandwidth: int, latency: int, watt_idle: int, watt_load: int):
        self.bandwidth = bandwidth
        self.latency = latency
        self.watt_idle = watt_idle
        self.watt_load = watt_load

        self.env = None
        self._resource = None

        self._time_under_use = 0
        # self.x = []

    def __str__(self):
        return f"{self.__class__.__name__}"

    @property
    def usage(self) -> float:
        return self._resource.usage

    @property
    def energy_consumption(self):
        return self.watt_idle + self.watt_load * self.usage

    def set_env(self, env: Environment):
        self.env = env
        self._resource = MonitoredResource(env)

    @contextmanager
    def request(self):
        with self._resource.request() as req:
            start = self.env.now
            yield req
            self._time_under_use += self.env.now - start
            # self.x.append((start, self.env.now))


class Link4G(Link):
    def __init__(self):
        super().__init__(bandwidth=300, latency=20, watt_idle=3, watt_load=12)


class LinkCable(Link):
    def __init__(self):
        super().__init__(bandwidth=1000, latency=5, watt_idle=0, watt_load=5)


class Node:
    def __init__(self, name: str, ipt: int, ram: int, watt_idle: int, watt_load: int):
        self.name = name
        self.ipt = ipt  # TODO
        self.ram = ram  # MB
        self.watt_idle = watt_idle
        self.watt_load = watt_load

        self.env = None
        self._resource = None

        self._time_under_use = 0

    def __str__(self):
        return f"{self.__class__.__name__}({self.name})"

    @property
    def usage(self) -> float:
        return self._resource.usage

    @property
    def energy_consumption(self):
        return self.watt_idle + self.watt_load * self.usage

    def set_env(self, env: Environment):
        self.env = env
        self._resource = MonitoredResource(env)

    @contextmanager
    def request(self):
        with self._resource.request() as req:
            start = self.env.now
            yield req
            self._time_under_use += self.env.now - start


class Sensor(Node):
    def __init__(self, name: str):
        super().__init__(name, ipt=10, ram=2000, watt_idle=3, watt_load=12)


class Fog(Node):
    def __init__(self, name: str):
        super().__init__(name, ipt=20, ram=4000, watt_idle=5, watt_load=20)


class Cloud(Node):
    def __init__(self, name: str):
        super().__init__(name, ipt=200, ram=20000, watt_idle=10, watt_load=150)