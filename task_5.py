from utils import setup
from constants import header

conf, sc, tweets, result_path = setup(5, sample=True)

tweets.filter(lambda x: x[header.index('country_code')] == 'US')\
    .map(lambda x: (x[header.index('place_name')], 1))\
    .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: (rdd1+rdd2)))\
    .sortByKey()\
    .sortBy(lambda x: x[1], False)\
    .map(lambda x: f'{x[0]}\t{x[1]}').coalesce(1).saveAsTextFile(result_path)