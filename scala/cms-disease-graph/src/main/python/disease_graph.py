import networkx as nx
import matplotlib.pyplot as plt
import sys

def draw_graph(G, labels=None, graph_layout='shell',
               node_size=1600, node_color='blue', node_alpha=0.3,
               node_text_size=12,
               edge_color='blue', edge_alpha=0.3, edge_tickness=1,
               edge_text_pos=0.3,
               text_font='sans-serif'):

    # these are different layouts for the network you may try
    # shell seems to work best
    if graph_layout == 'spring':
        graph_pos=nx.spring_layout(G)
    elif graph_layout == 'spectral':
        graph_pos=nx.spectral_layout(G)
    elif graph_layout == 'random':
        graph_pos=nx.random_layout(G)
    else:
        graph_pos=nx.shell_layout(G)
    # draw graph
    nx.draw_networkx_nodes(G,graph_pos,node_size=node_size, 
                           alpha=node_alpha, node_color=node_color)
    nx.draw_networkx_edges(G,graph_pos,width=edge_tickness,
                           alpha=edge_alpha,edge_color=edge_color)
    nx.draw_networkx_labels(G, graph_pos,font_size=node_text_size,
                            font_family=text_font)
    nx.draw_networkx_edge_labels(G, graph_pos, edge_labels=labels, 
                                 label_pos=edge_text_pos)
    # show graph
    frame = plt.gca()
    frame.axes.get_xaxis().set_visible(False)
    frame.axes.get_yaxis().set_visible(False)

    plt.show()


datafile = "../../../src/test/resources/disease_disease_pairs.csv"

# first pass: find the max weight to normalize against
fin = open(datafile, 'rb')
max_weight = -sys.maxint
for line in fin:
    weight = float(line.strip().split(",")[2])
    if weight > max_weight:
        max_weight = weight
fin.close()

# second pass: construct the graph
G = nx.Graph()
edge_labels = dict()
fin = open(datafile, 'rb')
for line in fin:
    cols = line.strip().split(",")
    edge_weight = float(cols[2]) * 100 / max_weight
    G.add_edge(cols[0], cols[1], weight = edge_weight)
    edge_labels[(cols[0], cols[1])] = "%d" % (int(edge_weight))
fin.close()
draw_graph(G, labels=edge_labels, graph_layout="shell")

