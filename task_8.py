from pyspark import sql, SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType

from utils import setup

conf, sc, tweets, result_path = setup(8, sample=False)
spark = SparkSession.builder.master("").enableHiveSupport().getOrCreate()
tweetdf = spark.createDataFrame(tweets)
new_tweetdf = tweetdf \
    .withColumn('utc_time',tweetdf._1) \
    .withColumn('country_name',tweetdf._2) \
    .withColumn('country_code',tweetdf._3) \
    .withColumn('place_type', tweetdf._4) \
    .withColumn('place_name', tweetdf._5) \
    .withColumn('language', tweetdf._6) \
    .withColumn('username', tweetdf._7) \
    .withColumn('user_screen_name', tweetdf._8) \
    .withColumn('timezone_offset', tweetdf._9) \
    .withColumn('number_of_friends', tweetdf._10) \
    .withColumn('tweet_text', tweetdf._11) \
    .withColumn('latitude', tweetdf._12.cast(FloatType())) \
    .withColumn('longitude', tweetdf._13.cast(FloatType())) \

#new_tweetdf.createTempView("tweets")

result_file = open(result_path, 'w')

#8.a)
number_of_tweets = tweetdf.count()
result_file.write(str(number_of_tweets)+'\n')

#8.b)
number_of_users = new_tweetdf.select('username').distinct().count()
result_file.write(str(number_of_users)+'\n')

#spark.sql("SELECT DISTINCT COUNT(username) FROM tweets").show()

#8.c)
number_of_countries = new_tweetdf.select('country_name').distinct().count()
result_file.write(str(number_of_countries)+'\n')

#spark.sql("SELECT DISTINCT COUNT(country_name) FROM tweets").show()

#8.d)
number_of_places = new_tweetdf.select('place_name').distinct().count()
result_file.write(str(number_of_places)+'\n')

#spark.sql("SELECT DISTINCT COUNT(place_name) FROM tweets").show()

#8.e)
number_of_languages = new_tweetdf.select('language').distinct().count()
result_file.write(str(number_of_languages)+'\n')

#spark.sql("SELECT DISTINCT COUNT(language) FROM tweets").show()

#8.f)
min_latitude = new_tweetdf.agg({'latitude': 'min'}).collect()[0]
max_latitude = new_tweetdf.agg({'latitude': 'max'}).collect()[0]
result_file.write(str(float(min_latitude['min(latitude)']))+'\n'+str(float(max_latitude['max(latitude)']))+'\n')

#8.e)
min_longitude = new_tweetdf.agg({'longitude': 'min'}).collect()[0]
max_longitude = new_tweetdf.agg({'longitude': 'max'}).collect()[0]
result_file.write(str(min_longitude['min(longitude)'])+'\n'+str(max_longitude['max(longitude)'])+'\n')

result_file.close()