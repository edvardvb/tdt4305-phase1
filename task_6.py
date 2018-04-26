from utils import setup, get_stop_words
from constants import header
from operator import add

conf, sc, tweets, result_path = setup(6, sample=False)
stop_words = get_stop_words()
result_file = open(result_path, 'w')

top_10 = tweets.filter(lambda x: (x[header.index('country_code')] == 'US'))\
    .flatMap(lambda x: x[header.index('tweet_text')].lower().split(" "))\
    .filter(lambda word: len(word) >= 2 and not word in stop_words)\
    .map(lambda x: (x, 1))\
    .reduceByKey(add)\
    .takeOrdered(10, key=lambda x: -x[1])\

for line in top_10:
    result_file.write(f'{line[0]}\t{line[1]}\n')

#Henter tweets fra usa, flatmapper til en liste med alle ordene i disse tweetsene
#Filtrerer ut ord med minimum lengde 2, og ut med stoppord
#mapper til (ord, telle-greie),
#reducer på keys for å telle antall ganger hvert ord dukker opp
#henter topp 10 ord med takeOrdered, og printer til fil
