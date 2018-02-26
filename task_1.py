from utils import get_rdd, get_conf, get_context

conf = get_conf('Task_1')
sc = get_context(conf)

#a)
rdd = get_rdd(sc) #Number of tweets: 2715066
#print(rdd.count()) 

#b)
user_rdd = rdd.map(lambda x: x.split('\t')[7]).distinct() #Number of distinct users: 583299
#print(user_rdd.count())

#c)
country_rdd = rdd.map(lambda x: x.split('\t')[1]).distinct() #Number of distinct countries: 70
#print(country_rdd.count())

#d)
place_name_rdd = rdd.map(lambda x: x.split('\t')[4]).distinct() #Number of distinct place names: 23121
#print(place_name_rdd.count())

#e)
language_rdd = rdd.map(lambda x: x.split('\t')[5]).distinct() #Number of distinct languages: 46
#print(language_rdd.count())