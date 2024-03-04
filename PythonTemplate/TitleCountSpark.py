#!/usr/bin/env python

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
    stopWords = set(f.read().splitlines())

with open(delimitersPath) as f:
    delimiters = f.read().strip()

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

def split_line(line):
    for delimiter in delimiters:
        line = line.replace(delimiter, ' ')
    return line.split()

def filter_stop_words(word):
    return word.lower() not in stopWords

wordCounts = lines.flatMap(split_line) \
    .filter(filter_stop_words) \
    .map(lambda word: (word.lower(), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[1], x[0])) \
    .sortByKey(False)

# Collect the results
results = wordCounts.collect()

outputFile = open(sys.argv[4],"w")

for count, word in results:
    outputFile.write(f"{word}\t{count}\n")

outputFile.close()
sc.stop()
