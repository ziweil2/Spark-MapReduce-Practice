#!/usr/bin/env python

import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def parser(line):
    line = line.split(':')
    page = line[0].strip()
    links = line[1].strip().split() if len(line) > 1 else []
    return [(link, 1) for link in links if link != page]

linkCounts = lines.flatMap(parser)
popularityCounts = linkCounts.reduceByKey(lambda a, b: a + b)

leagueIds = sc.textFile(sys.argv[2], 1)
leaguePages = popularityCounts.filter(lambda x: x[0] in leagueIds)

sortedLeaguePages = leaguePages.sortBy(lambda x: (x[1], x[0]))

pageRanks = sortedLeaguePages.zipWithIndex().map(lambda x: (x[0][0], x[1] + 1))

result = pageRanks.sortBy(lambda x: x[0])

output = open(sys.argv[3], "w")

# write results to output file. Foramt for each line: (key + \t + value +"\n")
for page, rank in result:
    output.write(f"{page}\t{rank}\n")

sc.stop()

