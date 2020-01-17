from pyspark import SparkContext
import sys
sc = SparkContext(appName ="hits")

def createhub(t):
	result=[]
	result.append((t[0],1,1))
	result.append((t[1],1,1))
	return result


def choose(y):
	tuple1=y[0]
	tuple2=y[1]
	if (tuple1[1]==tuple2[0]):
		product=tuple1[2]*tuple2[2]
		return((tuple1[0],product)) 



def auth_update(hubmatrix):
	result=l_transpose.cartesian(hubmatrix).map(lambda x: choose(x)).filter(lambda x: x is not None).reduceByKey(lambda x,y:x+y).collect()
	return result


def hub_update(authmatrix):
	result=l.cartesian(authmatrix).map(lambda x: choose(x)).filter(lambda x: x is not None).reduceByKey(lambda x,y:x+y).collect()
	return result

def normalize_matrix(result):
	normalize=[]
	max_value=max(result,key=lambda x: x[1])[1]
	for each in result:
		normalized_each=(each[0],1,(float(each[1])/float(max_value)))
		normalize.append(normalized_each)
	return normalize



 ### MAIN 
inputfile=sc.textFile(sys.argv[1],use_unicode=False) 

#creating initial matrix
l=inputfile.map(lambda x: x.split("\t")).map(lambda x: (int(x[0]),int(x[1]),1))


#create l transpose
l_transpose=l.map(lambda x:(x[1],x[0],x[2]))


#creating initial hubs
hub=l.map(lambda x: createhub(x)).flatMap(lambda x: x).distinct()

node_list=hub.map(lambda x: x[0]).collect()




total_nodes=int(sys.argv[2])

final_result=dict()

for i in range(0,int(sys.argv[3])):
	final_result[i+1]=dict()
	
	authority=auth_update(hub)
	norm_authority=normalize_matrix(authority)
	
	final_result[i+1]["authority"]=norm_authority

	authority=sc.parallelize(norm_authority)

	hub=hub_update(authority)
	norm_hub=normalize_matrix(hub)
	
	final_result[i+1]["hub"]=norm_hub

	hub=sc.parallelize(norm_hub)


#printing the result

for key,value in final_result.items():
    print("Iteration:"+" "+str(key))
    print("Authorities:\n")
    for auth in value["authority"]:
    	print(str(auth[0])+" "+"%.2f" % round(auth[2],2))

    print("Hubs:\n")
    for hub in value["hub"]:
    	print(str(hub[0])+" "+"%.2f" % round(hub[2],2))
   