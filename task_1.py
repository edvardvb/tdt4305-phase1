from utils import get_rdd, get_conf, get_context

conf = get_conf('Task_1')
sc = get_context(conf)

#a)
rdd = get_rdd(sc)
print(rdd.count()) #Number of tweets: 2715066

#b)
new_rdd = rdd.map(lambda x: x.split('\t')[7]).distinct() #Number of distinct users: 583299
print(new_rdd.count())