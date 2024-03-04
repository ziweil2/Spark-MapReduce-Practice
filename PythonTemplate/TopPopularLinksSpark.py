#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def parser(line):
    line = line.split(':')
    page = line[0].strip()
    links = line[1].strip().split() if len(line) > 1 else []
    return (page, links)

parsedLines = lines.map(parser)
linkCounts = parsedLines.flatMap(lambda x: [(link,1) for link in x[1] if link != x[0]]) \
                        .reduceByKey(lambda a, b: a + b)

topLinks = linkCounts.sortBy(lambda x: x[1], ascending=False).take(10)
result = sorted(topLinks, key=lambda x: (x[0], x[1]))

output = open(sys.argv[2], "w")
for link, count in result:
    output.write(f"{link}\t{count}\n")

sc.stop()

