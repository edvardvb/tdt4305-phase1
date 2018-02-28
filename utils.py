from pyspark import SparkConf, SparkContext


def get_conf(task_name):
    return SparkConf().setAppName(task_name)

def get_context(conf):
    return SparkContext(conf=conf)

def get_tweets(context, sample=False):
    tweets = context.textFile('data/geotweets.tsv').map(lambda x: x.split('\t'))
    return tweets.sample(False, 0.1, 5) if sample else tweets
    
def get_stop_words():
    return open('data/stop_words.txt', 'r').readlines()
