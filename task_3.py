from utils import setup
from constants import header

conf, sc, tweets, result_path = setup(3, sample=True)

def centroid(size, sum_of_lat, sum_of_long):
    return (sum_of_lat/size, sum_of_long/size)


hei = tweets.map(lambda x: (x[header.index('country_name')], 1, float(x[header.index('latitude')]), float(x[header.index('longitude')])))\
    .keyBy(lambda x: x[0])\
    .aggregateByKey(
        (0, 0.0, 0.0),
        (lambda x, y: (x[0]+y[1], x[1]+y[2], x[2]+y[3])),
        (lambda rdd1, rdd2: (rdd1[0]+rdd2[0], rdd1[1]+rdd2[1], rdd1[2]+rdd2[2]))
    )\
    .filter(lambda x: x[1][0] > 10)\
    .map(lambda x: (x[0],centroid(*x[1])))\
    .map(lambda x: f'{x[0]}\t{x[1][0]}\t{x[1][1]}').coalesce(1).saveAsTextFile(result_path)