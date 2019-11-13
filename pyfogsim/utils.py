import copy
import networkx as nx
import math


def draw_topology(G, alloc_entity=None, name: str = "app.png"):
    """
    Draw the modeled topology

    .. Note: This classes can be extended to export the topology (graph) to other visualization tools
    """
    import matplotlib.pyplot as plt

    G = copy.copy(G)
    if alloc_entity is not None:
        for node, modules in alloc_entity.items():
            for module in modules:
                G.add_node(module.name, module=True)
                G.add_edge(node, module.name, module=True)

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

    fig.savefig(name + ".png", format="png")
    plt.close(fig)


def haversine_distance(origin, destination):
    """Haversine formula to calculate the distance between two lat/long points on a sphere """
    radius = 6371.0  # FAA approved globe radius in km

    dlat = math.radians(destination[0] - origin[0])
    dlon = math.radians(destination[1] - origin[1])

    a = math.sin(dlat / 2.0) * math.sin(dlat / 2.0) + math.cos(math.radians(origin[0])) * math.cos(math.radians(destination[0])) * math.sin(
        dlon / 2.0
    ) * math.sin(dlon / 2.0)

    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return d  # distance in km
