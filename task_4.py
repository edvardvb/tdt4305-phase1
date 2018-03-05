from datetime import datetime as dt

from utils import get_tweets, get_conf, get_context
from constants import header

conf = get_conf('Task_1')
sc = get_context(conf)
tweets = get_tweets(sc, sample=False)

tweets.map(lambda x: (x[header.index('country_name')], int(x[header.index('utc_time')]) + int(x[header.index('timezone_offset')])))\
    .map(lambda x: ((x[0], dt.fromtimestamp(x[1]/1000).hour), 1))\
    .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: (rdd1+rdd2)))\
    .map(lambda x: (x[0][0], (x[0][1], x[1])))\
    .reduceByKey(lambda x, y: y if y[1] > x[1] else x)\
    .map(lambda x: f'{x[0]}\t{x[1][0]}\t{x[1][1]}')\
    .coalesce(1)\
    .saveAsTextFile("data/result_4.tsv")