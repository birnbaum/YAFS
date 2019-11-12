"""
Some common functions
"""
import copy
import random
import numpy as np
from collections import OrderedDict
import networkx as nx
from functools import partial
import math

# TODO Refactor, many methods like `create_pos` are only used in examples and never in the core lib


def create_pos(G, scale):
    x = nx.get_node_attributes(G, "x")
    y = nx.get_node_attributes(G, "y")
    pos = {}
    for k in list(x.keys()):
        lat = x[k] * scale
        lng = y[k] * scale
        pos[k] = np.array([lat, lng])
    return pos


def create_points(G):
    x = nx.get_node_attributes(G, "x")
    y = nx.get_node_attributes(G, "y")
    pos = OrderedDict()
    for k in list(x.keys()):
        lat = x[k]
        lng = y[k]
        pos[k] = [lat, lng]
    return pos


def toMeters(geometry):
    import pyproj
    from shapely.ops import transform

    project = partial(pyproj.transform, pyproj.Proj(init="EPSG:4326"), pyproj.Proj(init="EPSG:32633"))
    return transform(project, geometry).length


def get_random_node(G):
    return list(G.nodes())[random.randint(0, len(G.nodes()) - 1)]


def get_shortest_random_path(G):

    counter = 0
    tries = len(G.nodes) * 0.2
    while tries > counter:
        try:
            src = get_random_node(G)
            dst = get_random_node(G)
            path = nx.shortest_path(G, src, dst)
            if len(path) > 0:
                return path
        except nx.NetworkXNoPath:
            counter += 1


def draw_topology(topology, alloc_entity=None):
    """
    Draw the modeled topology

    .. Note: This classes can be extended to export the topology (graph) to other visualization tools
    """
    import matplotlib.pyplot as plt

    G = copy.copy(topology.G)
    if alloc_entity is not None:
        for node, modules in alloc_entity.items():
            for module in modules:
                G.add_node(module, module=True)
                G.add_edge(node, module, module=True)

    pos = nx.spring_layout(G)

    module_nodes = nx.subgraph_view(G, filter_node=lambda n: "module" in G.nodes[n]).nodes()
    module_edges = nx.subgraph_view(G, filter_edge=lambda s, d: "module" in G.edges[s, d]).edges()

    fig, ax = plt.subplots()
    nx.draw_networkx_nodes(G, nodelist=G.nodes() - module_nodes, node_shape="s", pos=pos, ax=ax)
    nx.draw_networkx_nodes(G, nodelist=module_nodes, node_shape="o", pos=pos, linewidths=0.2, node_color="pink", alpha=0.4, ax=ax)
    nx.draw_networkx_edges(G, edgelist=G.edges() - module_edges, pos=pos, width=1.2, ax=ax)
    nx.draw_networkx_edges(G, edgelist=module_edges, pos=pos, style="dashed", width=0.8, ax=ax)
    nx.draw_networkx_labels(G, pos, font_size=6, ax=ax)
    plt.axis("off")

    plt.ion()
    plt.show()

    fig.savefig("app_deployed.png", format="png")
    plt.close(fig)


def haversine_distance(origin, destination):
    """ Haversine formula to calculate the distance between two lat/long points on a sphere """
    radius = 6371.0  # FAA approved globe radius in km

    dlat = math.radians(destination[0] - origin[0])
    dlon = math.radians(destination[1] - origin[1])

    a = math.sin(dlat / 2.0) * math.sin(dlat / 2.0) + math.cos(math.radians(origin[0])) * math.cos(math.radians(destination[0])) * math.sin(
        dlon / 2.0
    ) * math.sin(dlon / 2.0)

    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return d  # distance in km
