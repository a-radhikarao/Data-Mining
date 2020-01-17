import sys
import itertools
import math
from pyspark import SparkContext

sc=SparkContext(appName="inf553")
fname=str(sys.argv[1])
file=sc.textFile(fname,use_unicode=False)
data=file.map(lambda x:x.split(",")).map(lambda x:(x[1],int(x[0])))
blist=data.groupByKey().map(lambda x:(x[0],list(x[1]))).collect()
pmap1=sc.parallelize(blist,2)
total_buckets=pmap1.count()
#[('a', [1, 2, 3]), ('c', [1, 2, 3]), ('b', [1, 2])]
itemlen=int(sys.argv[2])
overall_thold=int(sys.argv[3])

def mapper1(blist,itemlen,overall_thold,total_buckets):
    baskets=dict(blist)
    
    threshold=math.ceil((len(baskets)/total_buckets)*overall_thold)
    
    c1_all=[]
    for each in baskets.values():
        if each not in c1_all:
            c1_all.append(each)
    c1=list((set().union(*c1_all))) 

    countmap={}
    for candidate in c1:
        count=0
        for transaction in baskets.values():
            if candidate in transaction:
                count=count+1
        countmap[candidate]=count

    l=[] #stores one frequent items
    for item,count in countmap.items():
        if count>=threshold:
            l.append(item)
    ff1=[] #stores all frequent sets
    
    for i in range(1,itemlen+1):


        cand={}
        frequent={}
        for combo in itertools.combinations(l,i):

            name=[]
            for tname,transaction in baskets.items():

                if(set(combo).issubset(set(transaction))):
                    name.append(tname)
            cand[combo]=len(name) 


            for cname,ct in cand.items():

                if(ct>=threshold):     

                     frequent[cname]=1

            x=[]
            for each in frequent.keys():
                x.append(list(each))
            l=list(set().union(*x)) 

        ff1.append(frequent)
        print(frequent)

    if(len(ff1[itemlen-1])!=0):
        for each in ff1[itemlen-1].keys():
            res=(each,1)
            yield(res)

mapoutput1=pmap1.mapPartitions(lambda x: mapper1(x,itemlen,overall_thold,total_buckets))
reduceoutput1=mapoutput1.reduceByKey(lambda x,y: x).collect()
#[((1, 3), 1), ((1, 2), 1), ((2, 3), 1)]
def mapper2(chunk,ro):
	
	cnamemap={}
	baskets=dict(chunk)
	candidate=dict(ro)
	
	for cand in candidate.keys():
		
		count=0
		for transaction in baskets.values():
			
			if(set(cand).issubset(set(transaction))):
				count=count+1
				
		
		cnamemap[cand]=count
		
	for x,y in cnamemap.items():
		yield(x,y)
			
mapoutput2=pmap1.mapPartitions(lambda x:mapper2(x,reduceoutput1))
reduceoutput2=mapoutput2.reduceByKey(lambda x,y:x+y).filter(lambda x: x[1]>=overall_thold)

# print("reduceoutput2 is")
# print(reduceoutput2)

#[((1, 2), 3)]

def out_formatter(x):
	baskets=dict(blist)
	bnames=[]
	finaldict={}
	if x[1]>=overall_thold:
		for tname,transaction in baskets.items():
			if(set(x[0]).issubset(set(transaction))):
				bnames.append(tname)
			finaldict[x[0]]=bnames
	for x,y in finaldict.items():

		return(x,y)		
finaloutput=reduceoutput2.map(lambda x: out_formatter(x)).filter(lambda x: x!=None).collect()

if (len(dict(finaloutput))!=0):

	for key,value in dict(finaloutput).items():
		if len(value)>=overall_thold:
			for each in itertools.combinations(set(value),overall_thold):
				
				print(key,each)
else:
	print("no such subgraph")




