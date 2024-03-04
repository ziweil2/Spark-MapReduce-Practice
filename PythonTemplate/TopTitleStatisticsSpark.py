#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

def parser(line):
    count = line.split('\t')[1]
    return int(count)

attributes = lines.map(parser)
sum_val = attributes.reduce(lambda a, b: a + b)
count = attributes.count()
mean_val = int(sum_val) / count
min_val = attributes.min()
max_val = attributes.max()
variance = float(attributes.map(lambda x: (x - mean_val) ** 2).reduce(lambda a, b: a + b)) / count

outputFile = open(sys.argv[2], "w")

outputFile.write('Mean\t%s\n' % int(mean_val))
outputFile.write('Sum\t%s\n' % int(sum_val))
outputFile.write('Min\t%s\n' % min_val)
outputFile.write('Max\t%s\n' % max_val)
outputFile.write('Var\t%s\n' % int(variance))

sc.stop()

