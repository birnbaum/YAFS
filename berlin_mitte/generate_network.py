import os
import random
from itertools import islice, count
from typing import Optional, Dict, List

import geojson
import networkx as nx
from shapely.geometry import shape, Point

from pyfogsim.resource import Fog, Sensor, Link4G, LinkCable, Cloud


result_dir = os.path.join(os.path.dirname(__file__), "resources")
DC_GEOJSON = os.path.join(result_dir, "dc.geo.json")
FOG_GEOJSON = os.path.join(result_dir, "fog.geo.json")
MITTE_GEOJSON = os.path.join(result_dir, "mitte.geo.json")


def generate_network(n_sensors: int, n_fog: Optional[int] = None) -> nx.Graph:
    with open(MITTE_GEOJSON) as stream:
        mitte = shape(geojson.load(stream)["geometry"])

    dc_nodes = _dc_nodes()
    nodes_fog = _fog_nodes(n_fog)

    sensor_nodes, sensor_edge_lists = zip(*islice(_sensor_nodes(mitte, nodes_fog), n_sensors))
    nodes = dc_nodes + nodes_fog + list(sensor_nodes)

    edges = [edge for edge_list in sensor_edge_lists for edge in edge_list]  # flatten
    for fog in nodes_fog:
        for cloud in dc_nodes:
            edges.append({"source": fog["id"], "target": cloud["id"], "link": LinkCable()})

    return nx.node_link_graph({
        "directed": False,
        "multigraph": False,
        "graph": {},
        "nodes": nodes,
        "links": edges,
    })


def _dc_nodes() -> List[Dict]:
    nodes = []
    with open(DC_GEOJSON) as stream:
        offices = geojson.load(stream)
    for feature in offices["features"]:
        point = shape(feature['geometry'])
        node = Cloud(name=feature["properties"]["name"])
        nodes.append({"id": node, "pos": (point.x, point.y)})
    return nodes


def _fog_nodes(n: Optional[int] = None) -> List[Dict]:
    nodes = []
    with open(FOG_GEOJSON) as stream:
        stations = geojson.load(stream)
    for feature in stations["features"]:
        point = shape(feature['geometry'])
        node = Fog(name=feature["properties"]["address"])
        nodes.append({"id": node, "pos": (point.x, point.y)})
        if n is not None and len(nodes) == n:
            break
    return nodes


def _sensor_nodes(mitte, fog_nodes, sigma=0.03) -> List[Dict]:
    for i in count():
        position = (random.gauss(13.39, sigma), random.gauss(52.522297, sigma))
        if not mitte.contains(Point(position)):
            continue
        radius = Point(position).buffer(0.015)
        edges = []
        node = Sensor(name=str(i))
        for fog in fog_nodes:
            point = Point(*fog["pos"])
            if radius.contains(point):
                edges.append({"source": node, "target": fog["id"], "link": Link4G()})
        if len(edges) > 0:
            yield {"id": node, "pos": position}, edges
