package com.hackpro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object SparkContextVsSparkSession extends App {

  // Logger
  Logger.getLogger("org").setLevel(Level.ERROR)

  //  Spark Context
  val sparkContext = new SparkContext("local", "SparkContext")
  val sourceFile = sparkContext.textFile("src/main/resources/Used_Bikes.csv")
  println("*********************** SparkContext Results ************************")
  sourceFile.take(5).foreach(println)
  sparkContext.stop()

  // SQLContext
  val sparkContext1 = new SparkContext("local", "SQLContext")
  val sqlContext = new SQLContext(sparkContext1)
  val sqlContextSourceFiles = sqlContext.read.csv("src/main/resources/Used_Bikes.csv")
  println("*********************** Spark SQLContext Results ************************")
  sqlContextSourceFiles.show(5, truncate = false)
  sparkContext1.stop()

  // Spark Session
  val sparkSession = SparkSession.builder().master("local").appName("SparkSession").getOrCreate()
  sparkSession.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://192.168.228.128:9000")
  val sparkSessionSourceFiles = sparkSession.read.csv("/user/vengat/datasets/Used_Bikes.csv")

  println("*********************** SparkSession Results ************************")
  sparkSessionSourceFiles.show(5, truncate = false)
  sparkSession.stop()
}
