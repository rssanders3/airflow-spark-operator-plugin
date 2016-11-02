library(SparkR)

sc = sparkR.init()
lines = SparkR:::textFile(sc, "hdfs:///user/cloudera/express-deployment.json")
collect(lines)