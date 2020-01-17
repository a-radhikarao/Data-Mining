import sys
import math
from pyspark import SparkContext
import re
import operator

sc=SparkContext(appName="inf553")
fname=str(sys.argv[1])
file=sc.textFile(fname,use_unicode=False)
ofname=str(sys.argv[2])

movies=file.map(lambda x:x.split(",")).map(lambda x:(str(x[0]),x[1:])).collect()

def minhash(x):
	ipmv=dict(x)
	for user,ml in ipmv.items():
		usig=[]
		for i in range(0,20):
			l=[]
			for movie in ml:
				hfunc=((5* int(movie))+13*i)%100 
				l.append(hfunc)
			usig.append(min(l))
		yield(user,usig)	

pmv=sc.parallelize(movies,2)
minrdd=pmv.mapPartitions(lambda x:minhash(x)).collect()

def create_bands(x):
	uband=dict(x)
	for uname,data in uband.items():
		rlist=[]
		count=0
		for b in range(0,4):
			val=(b,(uname,tuple(sorted(data[count:count+5]))))
			rlist.append(val)
			count=count+5
		yield(rlist)

pcb=sc.parallelize(minrdd,2)
bdata=pcb.mapPartitions(lambda x:create_bands(x)).flatMap(lambda x:x).groupByKey().mapValues(lambda x:list(x)).collect()

rp=sc.parallelize(bdata,2)


def band_candidates(a):
	x=list(a)
	res=[]
	for i in range(0,len(x)):
		rra=dict(x[i][1])
		inv_map = {}
		for k,v in rra.items():
			inv_map[v]=inv_map.get(v, [])
			inv_map[v].append(k)
		
		for tup,usr in inv_map.items():
			if len(inv_map[tup])>1:
				
				for i in range(0,len(inv_map[tup])):

					for j in range(i+1,len(inv_map[tup])):
						res.append((inv_map[tup][i],inv_map[tup][j]))
						
	yield(res)


candidate_pairs=rp.mapPartitions(lambda x:band_candidates(x)).flatMap(lambda x:x).distinct().collect()

similardict={}
for each in candidate_pairs:
    if each[0] not in similardict.keys():
        similardict[each[0]]=[]
    if each[1] not in similardict.keys():
        similardict[each[1]] = []
    similardict[each[0]].append(each[1])
    similardict[each[1]].append(each[0])


user_dict={}
mvdict=dict(movies)

for key,value in similardict.items():
	s1=set(mvdict[key])
	jlist=[]
	for each in similardict[key]:
		s2=set(mvdict[each])
		jaccard=float(len(s1&s2))/float(len(s1|s2))
		if jaccard!=0:
			jlist.append((int(each[1:]),jaccard))
			a=sorted(jlist,key=lambda x:(x[0]))
			a=sorted(a,key=lambda x:(x[1]),reverse=True)[:5]
			fin=[]
			for every in a:
				val="U"+str(every[0])
				fin.append(val)
	user_dict[key]=fin


def natural_sort(l): #source:stackoverflow.com
    convert=lambda text: int(text) if text.isdigit() else text.lower() 
    alphanum_key=lambda key:[ convert(c) for c in re.split('([0-9]+)', key) ] 
    return sorted(l,key=alphanum_key)

finalans=dict()
for key,value in user_dict.items():
	freq=dict()
	for each in value:
		for item in mvdict[each]:
			if int(item) in freq:
				freq[int(item)]=freq[int(item)]+1
			else:
				freq[int(item)]=1

		a=sorted(freq.items(),key=operator.itemgetter(0))
		b=sorted(a,key=lambda x:(x[1]),reverse=True)[:3]
	y=[]
	for every in b:
		y.append(str(every[0]))
	finalans[key]=y

f=open(ofname,"w+")
for each in natural_sort(mvdict.keys()):
	if each in finalans.keys():
		f.write(str(each)+","+",".join(finalans[each])+"\n")


