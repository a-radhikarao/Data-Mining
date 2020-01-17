from pyspark import SparkContext
import sys

sc=SparkContext(appName="inf553")
arg1=str(sys.argv[1])
arg2=str(sys.argv[2])
arg3=str(sys.argv[3])

data_country=sc.textFile(arg2,use_unicode=False)
data_city=sc.textFile(arg1,use_unicode=False)

country=data_country.map(lambda s:s.split("\t")).map(lambda x:(str(x[0]),str(x[1])))

city=data_city.map(lambda s:s.split("\t")).map(lambda x:(str(x[2]),int(x[4])))


result=city.join(country).filter(lambda x:x[1][0]>=1000000).map(lambda x:( str(x[1][1]),1)).reduceByKey(lambda x,y:x+y).filter(lambda x: x[1]>=3).collect()



with open(arg3, "w+") as text_file:
	for re in result:
		text_file.write(str(re[0])+"\t"+str(re[1])+"\n")


