"""TODO This module is not yet functional

These methods have been factored out of the `core.Simulation` class to reduce complexity.
The refactoring is not 100% finished and this code has neither been integrated to `customMobility` and `Animation` nor any experiments.
"""

import itertools

import networkx as nx
import numpy as np

from trackanimation.tracking import DFTrack
from yafs.topology import Topology


def _load_map(user_tracks: DFTrack) -> "smopy.Map":
    import smopy
    trk_bounds = user_tracks.get_bounds()
    return smopy.Map((trk_bounds.min_latitude, trk_bounds.min_longitude, trk_bounds.max_latitude, trk_bounds.max_longitude), z=12)


def generate_animation(user_tracks: DFTrack, output_file: str, topology: Topology):
    from trackanimation.animation import AnimationTrack
    map_ = _load_map(user_tracks)
    map_.img.save(output_file + "_map_background.png")
    animation = AnimationTrack(self, dpi=100, bg_map=True, aspect="equal")
    animation.make_video(output_file=output_file, framerate=10, linewidth=1.0, G=topology.G)


def set_coverage_class(user_tracks: DFTrack, topology: Topology, class_name, **kwargs):
    endpoints, _ = _endpoints(topology)
    map_ = _load_map(user_tracks)
    return class_name(map_, endpoints, **kwargs)


def _endpoints(topology):
    level = nx.get_node_attributes(topology.G, "level")
    lat = nx.get_node_attributes(topology.G, "lat")
    lng = nx.get_node_attributes(topology.G, "lng")
    endpoints = np.array([[lat[n], lng[n]] for n in level if level[n] == 0])  # TODO Better a Tuple?
    counter = itertools.count(0)
    name_endpoints = {next(counter): n for n in level if level[n] == 0}
    return endpoints, name_endpoints


# def generate_snapshot(self, pathFile,event):
#     if len(self.endpoints) == 0: self.__update_connection_points()
#     if self.map == None: self.__load_map()
#
#     #map_endpoints = [self.map.to_pixels(i[0], i[1]) for i in self.endpoints]
#     #map_endpoints = np.array(map_endpoints)
#
#     animation = AnimationTrack(self, dpi=100, bg_map=True, aspect='equal')
#     animation.make_video(output_file=pathFile, framerate=10, linewidth=1.0)