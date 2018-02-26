from utils import get_tweets, get_conf, get_context

conf = get_conf('Task_1')
sc = get_context(conf)
tweets = get_tweets(sc)
header = ['utc_time', 'country_name', 'country_code', 'place_type', \
            'place_name', 'language', 'username', 'user_screen_name', \
            'timezone_offset', 'number_of_friends', 'tweet_text', \
            'latitude', 'longitude']

#1.a)
print('Number of tweets:' + str(tweets.count())) #Number of tweets when not sampled: 2715066

#1.b)
distinct_users = tweets.map(lambda x: x[header.index('username')]).distinct() #Number of distinct users when not sampled: 583299
print('Number of distinct users: ' + str(distinct_users))

#1.c)
distinct_countries = tweets.map(lambda x: x[header.index('country_name')]).distinct() #Number of distinct countries when not sampled: 70
print('Number of distinct countries: ' + str(distinct_countries.count()))

#1.d)
distinct_placenames = tweets.map(lambda x: x[header.index('place_name')]).distinct() #Number of distinct place names when not sampled: 23121
print('Number of distinct place names: ' + str(distinct_placenames.count()))

#1.e)
distinct_languages = tweets.map(lambda x: x[header.index('language')]).distinct() #Number of distinct languages when not sampled: 46
print('Number of distinct languages used in tweets: ' + str(distinct_languages.count()))

#1.f)
min_latitude = tweets.map(lambda x: x[header.index('latitude')]).min()
print('The minimum latitude is: ' + str(min_latitude))

#1.g)
max_latitude = tweets.map(lambda x: x[header.index('latitude')]).max()
print('The maxmimum latitude is: ' + str(max_latitude))

#1.h)
min_longitude = tweets.map(lambda x: x[header.index('longitude')]).min()
print('The minimum longitude is: ' + str(min_longitude))

#1.i)
max_longitude =  tweets.map(lambda x: x[header.index('longitude')]).max()
print('The maximum longitude is: ' + str(max_longitude))