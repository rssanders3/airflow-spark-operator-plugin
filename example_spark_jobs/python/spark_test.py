from pyspark import SparkContext, SparkConf
import sys

APP_NAME = "Spark_Test_Python"

args = sys.argv[1:]
print 'Number of arguments:', len(args), 'arguments.'
print 'Argument List:', str(args)

conf = SparkConf().setAppName(APP_NAME)
sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd_filtered = rdd.filter(lambda entry: entry > 3)
print(rdd_filtered.collect())
