from pyspark import SparkContext
import sys
from operator import add

#Check Input Arguments 
if len(sys.argv)!=4:
	print >> sys.stderr, "Usage: create_join_RDD.py <file 1 Path> <file 2 Path> <Output Path>"
	exit(-1)

sc=SparkContext(appName="create_join_RDD")
input_file_1=sc.textFile(sys.argv[1])

file_1_Spark=input_file_1.filter(lambda line: "Spark" in line)
file_1_Spark.cache()

input_file_2=sc.textFile(sys.argv[2])
file_2_Spark=input_file_2.filter(lambda line: "Spark" in line)
file_2_Spark.cache()

counts = file_2_Spark.flatMap(lambda line: line.strip().split(' ')).map(lambda word: (word, 1)).reduceByKey(add)
counts_0 = file_1_Spark.flatMap(lambda line: line.strip().split(' ')).map(lambda word: (word, 1)).reduceByKey(add)

file_2_Spark.cache()
final_c=counts.join(counts_0)
final_c.saveAsTextFile(sys.argv[3])
