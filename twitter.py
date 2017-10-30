from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
# conf = SparkConf() \
#              .setMaster("local[*]") \
#              .setAppName("Top 100 Tweets")
sc = SparkContext("local[*]","Top 100 Tweets(#tags)")

text_file = sc.textFile("./tweets-1.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .filter(lambda x: x[0].startswith("#")) \
             .reduceByKey(lambda a, b: a + b) \
             .sortBy(lambda x: x[1], False)


for word in counts.take(100):
	print(word)