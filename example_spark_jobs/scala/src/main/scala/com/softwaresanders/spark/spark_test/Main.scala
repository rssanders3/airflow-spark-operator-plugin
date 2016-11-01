package com.softwaresanders.spark.spark_test

import java.util

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by robertsanders on 10/31/16.
 */
object Main {

  val APP_NAME = "Spark_Test_Scala"

  def main (args: Array[String]) {

    println("Number of arguments: " + args.length + " arguments.")
    println("Argument List: " + args)

    val sparkConf = new SparkConf()
    sparkConf.setAppName(APP_NAME)
    val sc = new SparkContext(sparkConf)

    val rdd = sc.parallelize(1 to 5)
    val rddFiltered = rdd.filter(entry => entry > 3)
    println(util.Arrays.toString(rddFiltered.collect()))
  }

}
