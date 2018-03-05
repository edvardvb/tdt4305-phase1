from utils import setup
from constants import header
from operator import add

conf, sc, tweets, result_path = setup(6, sample=True)
stop_words = get_stop_words()

hei = tweets.filter(lambda x: (x[header.index('country_code')] == 'US'))\
    .map(lambda x: x[header.index('tweet_text')])\
    .flatMap(lambda x: x.split())\
    .filter(lambda word: len(word) > 2)\
    .flatMap(lambda x: [(x, 1) for i in x])\
    .reduceByKey(add)\
    .takeOrdered(10, key=lambda x: -x[1])
print(hei)

def split_into_words(tweet):
    words = tweet.lower().split(' ')
    final_words = []
    for word in words:
        if not word in stop_words and len(word) > 1:
            final_words.append(word)
    return final_words