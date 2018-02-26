from utils import get_rdd, get_conf, get_context

conf = get_conf('Task_1')
sc = get_context(conf)
rdd = get_rdd(sc)

print(rdd.sample(False, 0.1, 5).collect())
