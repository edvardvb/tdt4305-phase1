from collections import Counter

from utils import setup, get_stop_words
from constants import header


conf, sc, tweets, result_path = setup(7, sample=False)

def remove_stop_words(words):
    stop_words = get_stop_words()
    final_words = []
    for word in words:
        if len(word) > 1 and word not in stop_words:
            final_words.append(word)
    return final_words

#Metode for å fjerne ord. Tar inn liste, returner ny liste uten stoppord.

def get_ten_most_frequent(words):
    result = dict()
    for word in words:
        if result.get(word):
            result[word] += 1
        else: result[word] = 1
    result = dict(Counter(result).most_common(10))
    return [(key, value) for key, value in result.items()]

#Metode for å hente top 10 ord fra ei liste. Returnerer dict med ord og antall

def get_formatting(element):
    result = element[0] + '\t'
    for e in element[1]:
        result += f'{e[0]}\t{e[1]}\t'
    return result[:-1]

#Metode for å formatere topp ord fra en by til en string med byen og ordene

top_5_cities = tweets.filter(lambda x: x[header.index('country_code')] == 'US' and x[header.index('place_type')] == 'city')\
    .map(lambda x: (x[header.index('place_name')], 1, x[header.index('tweet_text')]))\
    .keyBy(lambda x: x[0])\
    .aggregateByKey((0, ''), (lambda x, y: (x[0]+y[1], x[1]+' '+y[2])), (lambda rdd1, rdd2: (rdd1[0]+rdd2[0], rdd1[1]+rdd2[1])))\
    .sortBy(lambda x: x[1], False)\
    .map(lambda x: (x[0], x[1][0], x[1][1]))\
    .top(5, key=lambda x: x[1])\

#Filtrer på tweets fra byer i USA, henter byen, telle-greie, tweet text. Byen som keys
#Aggreger for å telle antall tweets og kombinere tweets til én streng
#Sorterer for å finne byene med flest tweets
#Mapper til (by, antall, tweet-streng) og henter topp 5 byer

sc.parallelize(top_5_cities)\
    .sortBy(lambda x: x[1], False)\
    .map(
        lambda x: (
        x[0],
        get_ten_most_frequent(remove_stop_words(x[2].lower().split(" "))))
    )\
    .map(lambda x: get_formatting(x))\
    .coalesce(1)\
    .saveAsTextFile(result_path)

#Lager en ny RDD med resultatet fra forrige. Hvorfor det?
#Og hvorfor sorterer vi på samme igjen?
#Mapper til (by, topp 10 ord). Splitter strengen, lowercase, og fjerner stoppord. Henter top 10 ordself.
#Mapper til riktig format, coalescer og lagrer til fil
