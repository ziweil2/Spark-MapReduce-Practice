#!/usr/bin/env python

import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1])

def parser(line):
    line = line.split(':')
    page = line[0].strip()
    links = line[1].strip().split() if len(line) > 1 else []
    return [(link, 1) for link in links if link != page]

linkCounts = lines.flatMap(parser)
popularityCounts = linkCounts.reduceByKey(lambda a, b: a + b).sortBy(lambda x: (x[1], x[0]))

leagueIds = sc.textFile(sys.argv[2], 1).collect()
leagueCounts = popularityCounts.filter(lambda x: x[0] in leagueIds).collect()

currentRank = 0
currentCount = 0
counter = 0
pageRanks = []

for page, count in leagueCounts:
    if count > currentCount and counter > 0:
        currentRank = counter
        pageRanks.append((page, currentRank))
    else:
        pageRanks.append((page, currentRank))

    currentCount = count
    counter += 1

pageRanks.sort(key=lambda x: x[0])

output = open(sys.argv[3], "w")

# write results to output file. Foramt for each line: (key + \t + value +"\n")
for page, rank in pageRanks:
    output.write(f"{page}\t{rank}\n")

sc.stop()
