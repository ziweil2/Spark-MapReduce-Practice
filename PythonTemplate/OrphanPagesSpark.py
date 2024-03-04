#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def parser(line):
    line = line.split(':')
    page = line[0].strip()
    links = [link.strip() for link in line[1].split() if len(line) > 1 and line[1].strip() != "" and link.strip() != 
page]
    return (page, links)

parsedLines = lines.map(parser)

pages = parsedLines.map(lambda x: (x[0], 0))
references = parsedLines.flatMap(lambda x: x[1])
referenced = references.map(lambda x: (x, 1))

orphanPages = pages.union(referenced).reduceByKey(lambda a,b: a + b).filter(lambda x: x[1] == 0)

# Collect the orphan page IDs
orphans = orphanPages.sortByKey().collect()

output = open(sys.argv[2], "w")
for orphan in orphans:
    output.write(f"{orphan}\n")

sc.stop()

