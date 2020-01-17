import itertools
import sys
filename=sys.argv[1]
itemlen=int(sys.argv[2])
threshold=int(sys.argv[3])
with open(filename) as f:
    graph=f.readlines()

gl=[]
for each in graph:
    edge=tuple(filter(None,each.strip("\n").split(',')))
    gl.append(edge)

baskets={}
for each in gl:
    if each[1] not in baskets.keys():
        baskets[each[1]]=[int(each[0])]
    elif each[1] in baskets.keys():
        baskets[each[1]].append(int(each[0])) #baskets have all the transactions

c1_all=[]
for each in baskets.values():
    if each not in c1_all:
        c1_all.append(each)
l=list((set().union(*c1_all))) #list of all the candidate single items

ff1=[] #stores all frequent sets
for i in range(1,itemlen+1):    
    cand={}
    frequent={}
    for combo in itertools.combinations(l,i):
        name=[]
        for tname,transaction in baskets.items():
            if(set(combo).issubset(set(transaction))):
                name.append(tname)
        name.append(len(name))
        cand[combo]=name   
        for cname,ct in cand.items():
            if(ct[-1]>=threshold):     
                 frequent[cname]=ct[:-1]
                    
        x=[]
        for each in frequent.keys():
            x.append(list(each))
    l=list(set().union(*x))    
    ff1.append(frequent)

if(len(ff1[itemlen-1])!=0):
    for tup,values in ff1[itemlen-1].items():
        for cout in itertools.combinations(values,threshold):#t
            print(set(tup),set(cout))
else:
    print("no such subgraph")