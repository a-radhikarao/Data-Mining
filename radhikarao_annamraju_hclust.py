import sys
import heapq
import math
import itertools


# In[2]:


inputfile = sys.argv[1]
k = sys.argv[2]

def load_data(inputfile):
    gold_standard = {}
    dimension = 0
    dataset = []
    clusters = {}
    id = 0
    with open(inputfile) as f:
        for line in f:
            line = line.strip('\n')
            row = str(line)
            row = row.split(",")
            iris_class = row[-1]
            iris_properties = row[:-1]
            data = {}
            data.setdefault("id", id)
            data.setdefault("data", iris_properties)
            data.setdefault("class", iris_class)
            dataset.append(data)
            clusters_key = str([id])
            clusters.setdefault(clusters_key, {})
            clusters[clusters_key].setdefault("centroid", row[:-1])
            clusters[clusters_key].setdefault("members", [id])
            gold_standard.setdefault(iris_class,[])
            gold_standard[iris_class].append(id)
            id += 1
        dimension = len(dataset[0]["data"])
    return dataset, clusters, dimension, gold_standard


# In[3]:


def euclidean_distance(point_one, point_two):
    result = 0.0
    for i in range(len(point_one)):
        d1 = float(point_one[i])
        d2 = float(point_two[i])
        distn = d1 - d2
        result += pow(distn,2)
    result = math.sqrt(result)
    return result    


# In[4]:

def pairwise_distance(dataset):
    result = []
    for i in range(len(dataset) - 1):
        for j in range(i + 1, len(dataset)):
            eucl_dist = euclidean_distance(dataset[i]["data"], dataset[j]["data"])
            result.append( (eucl_dist,[eucl_dist, [[i],[j]]]) )
    return result

# In[5]:


def priority_queue(dist_list):
    heapq.heapify(dist_list)
    heap = dist_list
    return heap 

# In[6]:

def inter_cluster_centroid(current_clusters, data_point_idx, dimension):
    size = len(data_point_idx)
    dim = dimension
    centroid = [0.0]*dim
    for idx in data_point_idx:
        dim_dt = current_clusters[str(idx)]["centroid"]
        for i in range(dim):
            centroid[i] += float(dim_dt[i])
    for i in range(dim):
        centroid[i] /= size
    return centroid
#return dim, data_point_idx, size for intra to use           

# In[12]:

def intra_cluster_centroid(dataset, data_point_idx, dimension):
    size = len(data_point_idx)
    dim = dimension
    centroid = [0.0]*dim
    for idx in data_point_idx:
        dim_dt = dataset[idx]["data"]
        for i in range(dim):
            centroid[i] += float(dim_dt[i])
    for i in range(dim):
        centroid[i] /= size
    return centroid

# In[7]:

def check_heap_node(heap_node, cluster_archive):
    pair_dist = heap_node[0]
    pair_data = heap_node[1]
    for cluster in cluster_archive:
        if cluster in pair_data:
            return False
    return True

# In[8]:

def add_heap_node(heap, new_cluster, current_clusters):
    for each in current_clusters.values():
        new_heap_node = []
        dist = euclidean_distance(each["centroid"], new_cluster["centroid"])
        new_heap_node.append(dist)
        new_heap_node.append([new_cluster["members"], each["members"]])
        heapq.heappush(heap, (dist, new_heap_node))

# In[9]:

def precision_and_recall(current_clusters, gold_standard):
    current_pairs = []
    for k,v in current_clusters.items():
        l = list(itertools.combinations(v["members"], 2))
        current_pairs.extend(l)
    pp = len(current_pairs)
    gold_pairs =[]
    for k,v in gold_standard.items():
        l = list(itertools.combinations(v,2))
        gold_pairs.extend(l)
    pn = len(gold_pairs)
    tp = 0.0
    for pair in current_pairs:
        if pair in gold_pairs:
            tp+=1
    if pp == 0:
        precision = 0.0
    else:
        precision = (tp/pp)
    if pn ==0:
        precision = 0.0
    else:
        recall = (tp/pn)
    return precision, recall

# In[25]:

def hierachical_clustering(inputfile, k):
    dataset, clusters, dimension, gold_standard = load_data(inputfile)
    current_clusters = clusters
    cluster_archive = []
    result = pairwise_distance(dataset)
    heap = priority_queue(result)
    k = int(k)
    while len(current_clusters) > k:
        dist, min_item = heapq.heappop(heap)
        pair_data = min_item[1]
        if not check_heap_node(min_item, cluster_archive):
            continue
        new_cluster = {}
        new_cluster_members = sum(pair_data, [])
        new_cluster_centroid = intra_cluster_centroid(dataset, new_cluster_members, dimension)
        new_cluster_members = sorted(new_cluster_members)
        new_cluster.setdefault("centroid", new_cluster_centroid)
        new_cluster.setdefault("members", new_cluster_members)
        for i in pair_data:
            cluster_archive.append(i)
            del current_clusters[str(i)] ##DO: If archive in cluster, delete
        add_heap_node(heap, new_cluster, current_clusters)
        current_clusters[str(new_cluster_members)] = new_cluster
    precision, recall = precision_and_recall(current_clusters, gold_standard)
    return sorted(current_clusters), precision, recall

# In[26]:

clusters, precision, recall = hierachical_clustering(inputfile, k)

# In[27]:

for i in range(len(clusters)):
    print("Cluster " + str(i + 1) + ":" + clusters[i])
print("Precision = " + str(precision))
print("Recall = " + str(recall))

