# -*- coding: utf-8 -*-
"""fiedler

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1RN28yZVygUQU1gWDbhMZBV0pIqV2ohNY
"""

import networkx as nx
import numpy as np
import itertools
import sys

k=int(sys.argv[2])

edge_list=[]
with open(str(sys.argv[1]),"r") as f:
    for each in f.readlines():
        each=each.split("\t")
        edge=(int(each[0]),int(each[1]))
        edge_list.append(edge)
        
nodes=list(set(itertools.chain.from_iterable(edge_list)))

#creating a graph using the library
g=nx.Graph()
g.add_nodes_from(nodes)
g.add_edges_from(edge_list)

def choose_node(cluster_final):
  node_dict={}
  for each in cluster_final:
    count=len(each)
    if count not in node_dict.keys():
      node_dict[count]=[]
      node_dict[count].append(each)
    else:
      node_dict[count].append(each)
      
  cluster_maxnodes=max(node_dict.items())
  if len(cluster_maxnodes[1])==1:
    chosen_list=cluster_maxnodes[1][0]
  else:
    edge_tocompare=[]
    for every_sublist in cluster_maxnodes[1]:
      num_edges=g.subgraph(every_sublist).number_of_edges()
      edge_tocompare.append((tuple(every_sublist),num_edges))
      edge_sorted=sorted(edge_tocompare,key = lambda tup:(-tup[1], tup[0]))
      chosen_list=list(edge_sorted[0][0])
      
  return chosen_list

def calculate_fiedler(gn):
  
  FG=g.subgraph(gn)
  A = nx.adjacency_matrix(FG)
  D = np.diag(np.ravel(np.sum(A,axis=1)))
  L=D-A
  l, U = np.linalg.eigh(L)
  f = U[:,1] 
  
  node_value_pairs=list(zip(gn,list(f)))
  
  cluster_positive=[]
  cluster_negative=[]
  for each in node_value_pairs:
    if each[1]>0:
      cluster_positive.append(each[0])
    else:
      cluster_negative.append(each[0])
      
  return cluster_positive,cluster_negative

final_result=[nodes]

while(len(final_result)!=k):
  fiedler_node=choose_node(final_result)
  pos_clust,neg_clust=calculate_fiedler(fiedler_node)
  final_result.append(sorted(pos_clust))
  final_result.append(sorted(neg_clust))
  final_result.remove(fiedler_node)

for each in sorted(final_result):
     print( ", ".join( str(e) for e in list(each)))













