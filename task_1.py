from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Task_1")
sc = SparkContext(conf=conf)

rdd = sc.textFile('data/geotweets.tsv')
print(rdd.sample(False, 0.1, 5).collect())
