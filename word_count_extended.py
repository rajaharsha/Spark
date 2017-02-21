import re
from pyspark.sql.functions import regexp_replace, trim, col, lower
from pyspark import SparkContext,SparkConf

sc=SparkContext(appName="word_frequency")
RDD=sc.textFile("pg5000.txt").map(lambda x: x.replace(',','').replace('.','').replace('-','').replace('!','').replace(':','').replace("...","").replace("'","").replace("!","").replace("(","").replace(")","").replace("?","").replace("\"","").replace(";","").lower())
MAP=RDD.flatMap(lambda x: x.split()).map(lambda x:(x.lower(),1)).reduceByKey(lambda x,y:x+y)

def compare(x,y):
	if x[1]<y[1]:
		return y
	else:
		return x

max_word_count=MAP.reduce(compare)
print max_word_count
