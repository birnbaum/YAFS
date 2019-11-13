"""
This module is a generic class to introduce whatever kind of distribution in the simulator

"""
# TODO Improve documentation

import random
from abc import ABC, abstractmethod

import numpy as np


class Distribution(ABC):

    def __iter__(self):
        return self

    @abstractmethod
    def __next__(self):
        pass


class DeterministicDistribution(Distribution):
    def __init__(self, time):
        self.time = time

    def __next__(self):
        return self.time


class UniformDistribution(Distribution):
    def __init__(self, min, max):
        self.min = min
        self.max = max

    def __next__(self):
        return random.uniform(self.min, self.max)


class DeterministicDistributionStartPoint(Distribution):
    def __init__(self, start, time, **kwargs):
        self.start = start
        self.time = time
        self.started = False
        super(DeterministicDistributionStartPoint, self).__init__(**kwargs)

    def __next__(self):
        if not self.started:
            self.started = True
            return self.start
        else:
            return self.time


class ExponentialDistribution(Distribution):
    def __init__(self, lambd, seed=1, **kwargs):
        super(ExponentialDistribution, self).__init__(**kwargs)
        self.l = lambd
        self.rnd = np.random.RandomState(seed)

    def __next__(self):
        value = int(self.rnd.exponential(self.l, size=1)[0])
        if value == 0:
            return 1
        return value


class ExponentialDistributionStartPoint(Distribution):
    def __init__(self, start, lambd, **kwargs):
        self.lambd = lambd
        self.start = start
        self.started = False
        super(ExponentialDistributionStartPoint, self).__init__(**kwargs)

    def __next__(self):
        if not self.started:
            self.started = True
            return self.start
        else:
            return int(np.random.exponential(self.lambd, size=1)[0])
