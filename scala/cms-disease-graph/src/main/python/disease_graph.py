# Draws a disease interaction chart (based on common procedures for treatment)
# Adapted from:
# https://www.udacity.com/wiki/creating-network-graphs-with-python

import networkx as nx
import matplotlib.pyplot as plt
import os

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
    plt.gcf().set_size_inches(10, 10)
    frame.axes.get_xaxis().set_visible(False)
    frame.axes.get_yaxis().set_visible(False)

    plt.show()

def add_node_to_graph(G, node, node_labels):
    if node not in node_labels:
        G.add_node(node)
    node_labels.add(node)

datafile = "../../../data/disease_disease_pairs_final.csv"
lines = []
fin = open(os.path.join(datafile), 'rb')
for line in fin:
    disease_1, disease_2, weight = line.strip().split("\t")
    lines.append((disease_1, disease_2, float(weight)))
fin.close()

max_weight = max([x[2] for x in lines])
norm_lines = map(lambda x: (x[0], x[1], x[2] / max_weight), lines)

G = nx.Graph()
edge_labels = dict()
node_labels = set()
for line in norm_lines:
    add_node_to_graph(G, line[0], node_labels)
    add_node_to_graph(G, line[1], node_labels)
    if line[2] > 0.3:
        G.add_edge(line[0], line[1], weight=line[2])
        edge_labels[(line[0], line[1])] = "%.2f" % (line[2])
draw_graph(G, labels=edge_labels, graph_layout="shell")

