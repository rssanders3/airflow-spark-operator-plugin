library(SparkR)

sc = sparkR.init()
rdd = SparkR:::parallelize(sc, 1:5)
SparkR:::collect(rdd)