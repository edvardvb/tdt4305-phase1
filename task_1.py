from utils import setup
from constants import header
from functools import reduce

conf, sc, tweets, result_path = setup(1, sample=False)

result_file = open(result_path, 'w')

#1.a)
number_of_tweets = tweets.count()
print('Number of tweets:' + str(number_of_tweets)) #Number of tweets when not sampled: 2715066
result_file.write(str(number_of_tweets)+'\n')

#1.b)
distinct_users = tweets.map(lambda x: x[header.index('username')]).distinct() #Number of distinct users when not sampled: 583299
print('Number of distinct users: ' + str(distinct_users.count()))
result_file.write(str(distinct_users.count())+'\n')

#1.c)
distinct_countries = tweets.map(lambda x: x[header.index('country_name')]).distinct() #Number of distinct countries when not sampled: 70
print('Number of distinct countries: ' + str(distinct_countries.count()))
result_file.write(str(distinct_countries.count())+'\n')

#1.d)
distinct_placenames = tweets.map(lambda x: x[header.index('place_name')]).distinct() #Number of distinct place names when not sampled: 23121
print('Number of distinct place names: ' + str(distinct_placenames.count()))
result_file.write(str(distinct_placenames.count())+'\n')

#1.e)
distinct_languages = tweets.map(lambda x: x[header.index('language')]).distinct() #Number of distinct languages when not sampled: 46
print('Number of distinct languages used in tweets: ' + str(distinct_languages.count()))
result_file.write(str(distinct_languages.count())+'\n')

#1.f)
min_latitude = tweets.map(lambda x: x[header.index('latitude')]).map(lambda x: float(x)).min()
print('The minimum latitude is: ' + str(min_latitude))
result_file.write(str(min_latitude)+'\n')

#1.g)
max_latitude = tweets.map(lambda x: x[header.index('latitude')]).map(lambda x: float(x)).max()
print('The maxmimum latitude is: ' + str(max_latitude))
result_file.write(str(max_latitude)+'\n')

#1.h)
min_longitude = tweets.map(lambda x: x[header.index('longitude')]).map(lambda x: float(x)).min()
print('The minimum longitude is: ' + str(min_longitude))
result_file.write(str(min_longitude)+'\n')

#1.i)
max_longitude =  tweets.map(lambda x: x[header.index('longitude')]).map(lambda x: float(x)).max()
print('The maximum longitude is: ' + str(max_longitude))
result_file.write(str(max_longitude)+'\n')

#1.j)
#returns a tuple with (sum of tweet lengths, count of tweets)
sum_count = tweets.map(lambda x: x[header.index('tweet_text')])\
    .map(lambda x: len(x))\
    .aggregate(
        (0,0.0),
        (lambda x, y: (x[0]+y,x[1]+1)),
        (lambda rdd1, rdd2: (rdd1[0]+rdd2[0], rdd1[1]+rdd2[1])))

#I would like to do this reduce as an actual spark-method, not a python builtin
#This could also be done as: avg_tweet_length = sum_count[0]/sum_count[1]
avg_tweet_length = reduce(lambda x, y: x/y, sum_count
print('The average length of a tweet in characters is: ' + str(avg_tweet_length))
result_file.write(str(avg_tweet_length)+'\n')

#1.k)
sum_count = tweets.map(lambda x: x[header.index('tweet_text')])\
    .map(lambda x: x.lower().split(' '))\
    .map(lambda x: len(x))\
    .aggregate(
        (0,0.0),
        (lambda x, y: (x[0]+y,x[1]+1)),
        (lambda rdd1, rdd2: (rdd1[0]+rdd2[0], rdd1[1]+rdd2[1])))

#This could also be done as: avg_tweet_length_in_words = sum_count[0]/sum_count[1]
avg_tweet_length_in_words = reduce(lambda x, y: x/y, sum_count)
print('The average length of a tweet in words is: ' + str(avg_tweet_length_in_words))
result_file.write(str(avg_tweet_length_in_words)+'\n')

result_file.close()
