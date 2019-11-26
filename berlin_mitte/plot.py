import os

import numpy as np
import geojson
import matplotlib.pyplot as plt
import networkx as nx
from descartes import PolygonPatch
from matplotlib.colors import ListedColormap
from pygments.styles.paraiso_dark import BLUE

from pyfogsim.resource import Cloud, Sensor, Fog, LinkCable, Link4G

MITTE_PNG = "resources/mitte.png"
MITTE_GEOJSON = "resources/mitte.geo.json"


def plot(
    G,
    plot_map=False,
    plot_labels=False,
    plot_cloud_fog_edges=True,
    edge_load=False,
    node_load=False,
    out_path=None,
):
    fig = plt.figure()
    fig.set_size_inches(7.75, 7)

    ax_bg = fig.add_subplot(111, label="background")
    ax_bg.axis('off')
    ax = fig.add_subplot(111, label="2", frame_on=False)
    ax.set_xlim(13.2985, 13.432)
    ax.set_ylim(52.497, 52.57)

    if plot_map:
        img = plt.imread(MITTE_PNG)
        ax_bg.imshow(img)
    else:
        with open(MITTE_GEOJSON) as stream:
            mitte_feature = geojson.load(stream)
        poly = mitte_feature["geometry"]
        ax.add_patch(PolygonPatch(poly, fc=BLUE, ec=BLUE, alpha=0.1))

    pos = {node: data["pos"] for node, data in G.nodes(data=True)}

    # Colormap
    cmap = plt.get_cmap("Reds")
    my_cmap = cmap(np.arange(cmap.N))  # Get the colormap colors
    my_cmap[:, -1] = np.linspace(0, 1, cmap.N)  # Set alpha
    my_cmap = ListedColormap(my_cmap)  # Create new colormap

    base_props = dict(G=G, pos=pos, ax=ax)

    fog_nodes = _filter_nodes(G, Fog)
    cloud_nodes = _filter_nodes(G, Cloud)
    if node_load:
        nx.draw_networkx_nodes(
            **base_props,
            nodelist=fog_nodes,
            node_shape="o",
            node_color=[node.usage for node in fog_nodes],
            node_size=50,
            edgecolors="black",
            linewidths=1,
            cmap=cmap,
            vmin=0,
            vmax=1,
        )
        nx.draw_networkx_nodes(
            **base_props,
            nodelist=cloud_nodes,
            node_shape="s",
            node_color=[node.usage for node in cloud_nodes],
            node_size=100,
            edgecolors="black",
            linewidths=1,
            cmap=cmap,
            vmin=0,
            vmax=1,
        )
    else:
        nx.draw_networkx_nodes(
            **base_props,
            nodelist=fog_nodes,
            node_shape="o",
            node_color="white",
            node_size=20,
            edgecolors="black",
            linewidths=1,
        )
        nx.draw_networkx_nodes(
            **base_props,
            nodelist=cloud_nodes,
            node_shape="s",
            node_color="white",
            node_size=30,
            edgecolors="red",
            linewidths=1,
        )

    nx.draw_networkx_nodes(
        **base_props,
        nodelist=_filter_nodes(G, Sensor),
        node_shape="o",
        node_color="black",
        node_size=2,
    )

    if plot_cloud_fog_edges:
        edgelist = _filter_edges(G, Link4G)
        if edge_load:
            nx.draw_networkx_edges(
                **base_props,
                edgelist=edgelist,
                width=1,
                edge_color=[G.edges[a, b]["link"].usage for a, b in edgelist],
                edge_cmap=my_cmap,
                edge_vmin=0,
                edge_vmax=1,
            )
        else:
            nx.draw_networkx_edges(
                **base_props,
                edgelist=_filter_edges(G, LinkCable),
                width=0.1,
            )

    edgelist = _filter_edges(G, Link4G)
    if edge_load:
        nx.draw_networkx_edges(
            **base_props,
            edgelist=edgelist,
            width=1,
            edge_color=[G.edges[a, b]["link"].usage for a, b in edgelist],
            edge_cmap=my_cmap,
            edge_vmin=0,
            edge_vmax=1,
        )
    else:
        nx.draw_networkx_edges(
            **base_props,
            edgelist=edgelist,
            width=0.3,
        )

    if plot_labels:
        nx.draw_networkx_labels(
            G=G,
            pos={k: (x, y+0.0025) for k, (x, y) in pos.items()},
            labels={n: n.name for n in G.nodes() if isinstance(n, Cloud)},
            font_weight="light",
            font_size=10,
        )

    ax.axis('off')
    plt.show()

    if out_path is not None:
        assert os.path.splitext(out_path)[1] == ".png"
        fig.savefig(out_path, format="png")


def _filter_nodes(G, cls):
    return [n for n in G.nodes() if isinstance(n, cls)]


def _filter_edges(G, cls):
    return [(a, b) for a, b, n in G.edges(data=True) if isinstance(n["link"], cls)]
