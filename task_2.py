from utils import setup
from constants import header

conf, sc, tweets, result_path = setup(2, sample=True)

''' I think this is the correct solution, but not 100% sure.
Probably a better way to format the tuples for the text file?
And it is most likely a better way to do the "first by count, then by name"-sorting
 '''
tweets.map(lambda x: (x[header.index('country_name')], 1))\
    .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: (rdd1+rdd2)))\
    .sortByKey()\
    .sortBy(lambda t: t[1], False)\
    .map(lambda x: "%s\t%s" %(x[0], x[1])).coalesce(1).saveAsTextFile(result_path)

#Landet bli key, har med '1' som telle-ting på greia.
#Sorterer først på key, deretter på antall.
#Det gjør at sorteringen på key bevares om noen har samme antallself.
#Mapper til riktig format, coalescer til 1 partisjon og lagrer.
