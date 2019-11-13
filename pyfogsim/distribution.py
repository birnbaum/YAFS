import random
from abc import ABC, abstractmethod


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
