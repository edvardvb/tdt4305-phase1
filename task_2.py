from utils import get_tweets, get_conf, get_context
from constants import header

conf = get_conf('Task_2')
sc = get_context(conf)
tweets = get_tweets(sc, sample=True)


''' I think this is the correct solution, but not 100% sure.
Probably a better way to format the tuples for the text file?
And it is most likely a better way to do the "first by count, then by name"-sorting
 '''
tweets.map(lambda x: x[header.index('country_name')])\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x + y)\
    .sortByKey()\
    .sortBy(lambda t: t[1], False)\
    .map(lambda x: "%s\t%s" %(x[0], x[1])).coalesce(1).saveAsTextFile("data/result_2.tsv")
