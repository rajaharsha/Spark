from pyspark import SparkContext
import sys
if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: logmining_extended.py < input file path >"
        exit(-1)
		
sc = SparkContext(appName="logmining_extended")

input_file = sc.textFile(sys.argv[1])
words = input_file.filter(lambda input_line: "error" in input_line or "Mozilla" in input_line or "compatible" in input_line or "iPhone"  in input_line)

write_to_file=words.collect()
obj=open("logmining_output",'w')

for item in write_to_file:
	obj.write("%s\n" % item)
words.cache()

line_count = words.count()
print "Number of lines with any of the 4 given words:", line_count
