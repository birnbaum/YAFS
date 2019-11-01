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


# -------------------------- JUST COPIED - NOT YET REFACTORED ----------------------------- #
#
# ### MOBILE ADAPTATION SECTION
# def update_service_coverage(self):
#     if self.street_network is not None:
#         points = utils.create_points(self.topology.G)
#         point_streets = utils.create_points(self.street_network)
#
#         tree = scipy.spatial.KDTree(points.values())
#         points_within_tolerance = tree.query_ball_point(point_streets.values(), self.tolerance)
#
#         # key = node network
#         # value = id - module SW
#
#         self.service_coverage = {}
#         for idx, pt in enumerate(points_within_tolerance):
#             ## MODULE SW
#             key2 = point_streets.keys()[idx]
#             nG2 = self.street_network.nodes[key2]
#             # print "%s is close to " % nG2["model"]
#             ## Street coverage
#             for p in pt:
#                 key = points.keys()[p]
#                 # service_coverage[(G.nodes[key]['x'],G.nodes[key]['y'])]=nG2["model"]
#                 self.service_coverage[key] = nG2["id"]

# def setMobilityUserBehaviour(self,dataPopulation):
#     self.user_behaviour = dataPopulation #TODO CHECK SYNTAX

def __add_mobile_agent(self, ides, gme):
    # The mobile starts

    yield self.env.timeout(gme.start)
    self.logger.info("(#DES:%i)\t--- Mobile Entity STARTS :\t%s " % (ides, gme._id))
    while (len(gme.path) - 1 > gme.current_position) and not self.stop and self.des_process_running[ides]:
        e = (gme.path[gme.current_position], gme.path[gme.current_position + 1])
        data = self.street_network.get_edge_data(*e)
        try:
            next_time = int(utils.toMeters(data[0]["geometry"]) / gme.speed)
        except KeyError:
            next_time = 1  # default time by roundabout or other Spatial THINGS

        # take an action?
        gme.next_time = next_time

        self.logger.info("(#DES:%i)\t--- DO ACTION :\t%s " % (ides, gme._id))
        gme.do.action(gme)

        # TODO Can the MA wait more time in that node?

        yield self.env.timeout(next_time)
        gme.current_position += 1

    # Last movement
    if self.des_process_running[ides] and not self.stop:
        gme.do.action(gme)

    self.logger.info("(#DES:%i)\t--- Mobile Entity ENDS :\t%s " % (ides, gme._id))
    # print "Mobile agent: %s ends " % gme.plate

def add_mobile_agent(self, gme):
    ides = self._get_id_process()
    self.des_process_running[ides] = True
    self.env.process(self.__add_mobile_agent(ides, gme))

    ### ATENCION COONTROLAR VAR: INTERNAS
    # self.alloc_DES[ides] = id_node

    return ides