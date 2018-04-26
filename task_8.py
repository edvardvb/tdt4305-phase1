from pyspark import sql, SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType

from utils import setup

conf, sc, tweets, result_path = setup(81, sample=True)
spark = SparkSession.builder.master("").enableHiveSupport().getOrCreate()
tweetdf = spark.createDataFrame(tweets)
new_tweetdf = tweetdf \
    .withColumn('utc_time',tweetdf._1) \
    .withColumn('country_name',tweetdf._2) \
    .withColumn('country_code',tweetdf._3) \
    .withColumn('place_type', tweetdf._4) \
    .withColumn('place_name', tweetdf._5) \
    .withColumn('language', tweetdf._6) \
    .withColumn('user_screen_name', tweetdf._7) \
    .withColumn('username', tweetdf._8) \
    .withColumn('timezone_offset', tweetdf._9) \
    .withColumn('number_of_friends', tweetdf._10) \
    .withColumn('tweet_text', tweetdf._11) \
    .withColumn('latitude', tweetdf._12.cast(FloatType())) \
    .withColumn('longitude', tweetdf._13.cast(FloatType())) \

#Column names became _1 .. _13 when reading in as dataframe, rename them.
#Lat and long must be casted to float

result_file = open(result_path, 'w')

#8.a)
number_of_tweets = tweetdf.count()
result_file.write(str(number_of_tweets)+'\n')

#8.b) Selects username and count how many distinct usernames there are
number_of_users = new_tweetdf.select('username').distinct().count()
result_file.write(str(number_of_users)+'\n')

#8.c) 
number_of_countries = new_tweetdf.select('country_name').distinct().count()
result_file.write(str(number_of_countries)+'\n')

#8.d)
number_of_places = new_tweetdf.select('place_name').distinct().count()
result_file.write(str(number_of_places)+'\n')

#8.e)
number_of_languages = new_tweetdf.select('language').distinct().count()
result_file.write(str(number_of_languages)+'\n')

#8.f) Aggregate latitude with aggregation functions min and max, collect[0] to get Row(min(latitude)) and Row(max(latitude))
#which can be accessed when writing to file
min_latitude = new_tweetdf.agg({'latitude': 'min'}).collect()[0]
max_latitude = new_tweetdf.agg({'latitude': 'max'}).collect()[0]
result_file.write(str(float(min_latitude['min(latitude)']))+'\n'+str(float(max_latitude['max(latitude)']))+'\n')

#8.e) Aggregate longitude with aggregation functions min and max, collect[0] to get Row(min(longitude)) and Row(max(longitude))
#which can be accessed when writing to file
min_longitude = new_tweetdf.agg({'longitude': 'min'}).collect()[0]
max_longitude = new_tweetdf.agg({'longitude': 'max'}).collect()[0]
result_file.write(str(min_longitude['min(longitude)'])+'\n'+str(max_longitude['max(longitude)'])+'\n')

result_file.close()