import networkx as nx
import networkx.algorithms.centrality.betweenness as btw 
import itertools
import sys

edge_list=[]
with open(str(sys.argv[1]),"r") as f:
    for each in f.readlines():
        each=each.split("\t")
        edge=(int(each[0]),int(each[1]))
        edge_list.append(edge)
        
nodes=list(set(itertools.chain.from_iterable(edge_list)))

g=nx.Graph()
g.add_nodes_from(nodes)
g.add_edges_from(edge_list)

def edge_toremove():
    btwness=btw.edge_betweenness_centrality(g, normalized=False)
    sorted_btw = sorted(btwness.items(), key=lambda kv:(-kv[1],kv[0]))
    maximal_edge=sorted_btw[0][0]
    return maximal_edge

num_connected=1
while (num_connected!=int(sys.argv[2])):
    max_edge=edge_toremove()
    g.remove_edge(max_edge[0],max_edge[1])
    num_connected=nx.number_connected_components(g)

for each in list(nx.connected_components(g)):
    print(",".join(str(e) for e in list(each)))