from pyspark import SparkConf, SparkContext


def get_conf(task_name):
    return SparkConf().setAppName(task_name)

def get_context(conf):
    return SparkContext(conf=conf)

def get_tweets(context):
    return context.textFile('data/geotweets.tsv').map(lambda x: x.split('\t')).sample(False, 0.1, 5)
