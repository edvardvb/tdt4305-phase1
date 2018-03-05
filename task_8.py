from pyspark import sql, SparkContext, SparkConf
from pyspark.sql import SparkSession

from utils import setup
from constants import header

conf, sc, tweets, result_path = setup(81, sample=False)
spark = SparkSession.builder.master("").enableHiveSupport().getOrCreate()
tweetdf = spark.createDataFrame(tweets)
result_file = open(result_path, 'w')

#Skal dobbeltsjekke med Vegard om det er ok å gjøre det sånn:

''' #8.a)
number_of_tweets = tweetdf.count()
result_file.write(str(number_of_tweets)+'\n')

#8.b)
number_of_users = tweetdf.select('_8').distinct().count()
result_file.write(str(number_of_users)+'\n')

#8.c)
number_of_countries = tweetdf.select('_2').distinct().count()
result_file.write(str(number_of_countries)+'\n')

#8.d)
number_of_places = tweetdf.select('_5').distinct().count()
result_file.write(str(number_of_places)+'\n')

#8.e)
number_of_languages = tweetdf.select('_6').distinct().count()
result_file.write(str(number_of_languages)+'\n')
 '''
#8.f)
min_latitude = tweetdf.agg({'_12': 'min'}).collect()[0]
max_latitude = tweetdf.agg({'_12': 'max'}).collect()[0]
result_file.write(str(float(min_latitude['min(_12)']))+'\n'+str(float(max_latitude['max(_12)']))+'\n')

''' #8.e)
min_longitude = tweetdf.agg({'_13': 'min'}).collect()[0]
max_longitude = tweetdf.agg({'_13': 'max'}).collect()[0]
result_file.write(str(min_longitude['min(_13)'])+'\n'+str(max_longitude['max(_13)'])+'\n')
 '''
result_file.close()