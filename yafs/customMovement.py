# TODO Missing documentation

import copy
import logging
import time

import networkx as nx
import numpy as np

from trackanimation.animation import AnimationTrack
from yafs.core import Simulation


logger = logging.getLogger(__name__)


class MovementUpdate:
    def __init__(self, pathResults, doExecutionVideo):
        self.current_step = 0
        self.path_results = pathResults

        self.previous_graph_connection = {}
        self.name_endpoints = {}
        self.doExecutionVideo = doExecutionVideo

    def summarize(self):
        print("Number of evolutions %i" % self.current_step)

    def deploy_module(self, sim, service, idtopo):
        app_name = service[0 : service.index("_")]
        app = sim.apps[app_name]
        services = app.services
        sim.deploy_operator(app_name, service, services[service], [idtopo])

    def get_last_points(self, df, step):

        tt = df[df.VideoFrame == step]
        coordinates = {}

        for row in tt.iterrows():
            code = str(row[1]["CodeRoute"])
            lat = row[1]["Latitude"]
            lng = row[1]["Longitude"]
            coordinates[code] = (lat, lng)

        return coordinates

    def update_topology_connections(self, sim, code, id_node, routing):
        # ##type: (Sim, String, int/String, Selection) -> None   ### CREATING ONLY ONE EDGE
        # or
        # ##type: (Sim, String, [int/String], Selection) -> None ### CREATING MORE EDGE
        # or
        # ##type: (Sim, String, None , Selection) -> None ### REMOVING EDGE
        """
        It changes the topology connections
        Args:
            sim:
            code:
            id_node:

        Returns:

        """
        # 1 - remove previous edge (in case)

        # print " Removing edges of node: %s" %code
        edge = ()
        # TODO Improve a dynamic assignament of BW / PR (edge link properties)
        att = {"BW": 10, "PR": 10}
        try:
            existNode = False
            edges_to_remove = [e for e in sim.G.edges() if int(code) in e]
            for edge in edges_to_remove:
                att = sim.G[edge[0]][edge[1]]
                sim.G.remove_edge(*edge)
                existNode = True

            if len(edges_to_remove) == 0 and id_node != None:
                if code in sim.mobile_fog_entities:
                    sim.G.add_node(int(code), level=-1, **sim.mobile_fog_entities[code]["node_attributes"])
                else:
                    sim.G.add_node(int(code), level=-1)
        except nx.NetworkXError:
            # The edge was not established, first time:
            # print "Does not exist: %s"%code
            None
            # sim.G.add_node(code,level=-1)

        # 2 - add new edge
        # print "a new link between nodes: %s -> %s"%(code,id_node)
        if id_node != None:
            if type(id_node) == list:
                for id_n in id_node:
                    sim.G.add_edge(int(code), int(id_n), **att)
            else:
                sim.G.add_edge(int(code), int(id_node), **att)

        # 3 we can interact with other own classes:
        routing.invalid_cache_value = True  # we can invalid the cache of routing packages

    def __call__(self, sim, routing, case, stop_time, it):
        # type: (Simulation, Selection, String, int, int) -> None
        """
        It updates network topology in function of user location and mobile agents in the scenario

        Args:
            sim: Simulator
            routing: selection strategy
            case: name of the simulation to show results
            stop_time: finalization time of the simulation
            it: int to identify the iteration

        Returns:
            None
        """
        logger.info("Movement number (#%i) at time: %i" % (self.current_step, sim.env.now))
        start_time = time.time()

        ##
        # TEST: for debug control !
        # It takes a snapshot of a specific movement
        # if self.current_step == 0:
        self.animation = AnimationTrack(sim, dpi=100, bg_map=True, aspect="equal")

        ######
        # UPDATING LOCATION of MOBILE ENTITIES
        #
        track_code_last_position = self.get_last_points(sim.user_tracks.df, self.current_step)  # TODO "user_tracks" has been removed from Simulation

        # getting the position of mobile endpoint entities
        mobile_endpoints = []
        code_mobile_endpoints = []
        for ix, code_mobile in enumerate(sim.mobile_fog_entities):
            if code_mobile in list(track_code_last_position.keys()):
                (lng, lat) = track_code_last_position[code_mobile]
                mobile_endpoints.append(np.array([lng, lat]))
                code_mobile_endpoints.append(code_mobile)
            else:
                logger.critical(" Mobile entity: %s without a position !" % code_mobile)

        mobile_endpoints = np.array(mobile_endpoints)

        ##########
        # UPDATING CONNECTIONS of MOBILE ENTITIES with NETWORK ENDPOINTS
        #
        self.name_endpoints = copy.copy(sim.name_endpoints)  # TODO "name_endpoints" has been removed from Simulation

        # We join network endpoints and mobile endpoints
        last = len(self.name_endpoints)  # TODO "name_endpoints" has been removed from Simulation
        for ix, code_mobile in enumerate(sim.mobile_fog_entities.keys()):
            self.name_endpoints[last + ix] = code_mobile  # TODO "name_endpoints" has been removed from Simulation
        if len(mobile_endpoints) == 0:
            all_endpoints = sim.endpoints  # TODO "endpoints" has been removed from Simulation
        else:
            all_endpoints = np.concatenate((sim.endpoints, mobile_endpoints), axis=0)  # TODO "endpoints" has been removed from Simulation
        sim.coverage.update_coverage_of_endpoints(sim.map, all_endpoints)

        # We update connections between mobile_entities with the network
        all_current_connection = {}
        # getting the last position of all mobile entities
        for code in track_code_last_position:
            (lng, lat) = track_code_last_position[code]
            new_point = [lng, lat]

            if code not in list(sim.mobile_fog_entities.keys()):
                point_index = sim.coverage.connection(new_point)
                if point_index != None:
                    all_current_connection[code] = self.name_endpoints[point_index]  # TODO "name_endpoints" has been removed from Simulation

        ##########
        # UPDATING CONNECTIONS of MOBILE ENDPOINTS  with NETWORK ENDPOINTS
        #
        fixed_location_endpoints = dict(list(zip(list(sim.name_endpoints.values()), sim.endpoints)))  # TODO "endpoints" and "name_endpoints" have been removed from Simulation
        mobile_location_endpoints = dict(list(zip(code_mobile_endpoints, mobile_endpoints)))

        print("STEP ", self.current_step)
        # print "FIX"
        # print fixed_location_endpoints
        # print "MOB"
        # print mobile_location_endpoints
        connection_mobiles = sim.coverage.connection_between_mobile_entities(fixed_location_endpoints, mobile_location_endpoints, sim.mobile_fog_entities)

        # we merge both type of connections to generate the graph
        for k in connection_mobiles:
            all_current_connection[k] = connection_mobiles[k]

        # we detect the elements without a current disconection
        previous_code = set(all_current_connection)
        without_connection = [x for x in self.previous_graph_connection if x not in previous_code]

        # DEBUGING CODE
        # if self.current_step >= 31 and self.current_step<41:
        #     print "CURRENT CONNECTION "
        #     print all_current_connection
        #     print "PREVIOUS "
        #     print self.previous_graph_connection
        #     print "WITHOUT CONNECTION"
        #     print without_connection

        # updating previous network connections with the current ones
        # TODO Point of improvement: Decide if a user remains connected to the current point or connects to other options.
        changes = False
        for k in all_current_connection:
            if not k in list(self.previous_graph_connection.keys()):
                self.previous_graph_connection[k] = all_current_connection[k]
                self.update_topology_connections(sim, k, self.previous_graph_connection[k], routing)
                changes = True
            else:
                if all_current_connection[k] == self.previous_graph_connection[k]:
                    # Position without changes
                    None
                else:
                    self.previous_graph_connection[k] = all_current_connection[k]
                    self.update_topology_connections(sim, k, self.previous_graph_connection[k], routing)
                    changes = True

            # print "%s ;  %s" % (k,all_current_connection[k])

        # TODO actualizar los cambios de conexiones self.connection = current_connection ????? ISAAC QUE QUERIAS DECIR? JOER

        # updating elements without a current connection: removing edges
        for k in without_connection:
            self.update_topology_connections(sim, k, None, routing)
            del self.previous_graph_connection[k]

        # WARNING:
        # This is an expensive task (optional)
        if self.doExecutionVideo:
            # sim.topology.draw_png(self.path_results + "network_%i" % self.current_step)
            self.animation.make_snap(self.current_step, self.path_results + "snap_%05d" % self.current_step, G=sim.network, draw_connection_line=False)

        logger.info("\texecution time of movement (#%i): %s" % (self.current_step, (time.time() - start_time)))
        # we prepare the next execution of this function
        self.current_step += 1
