package com.hackpro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MultipleSparkContextInOneJVM extends App {

  // Logger
  Logger.getLogger("org").setLevel(Level.ERROR)

  //  First Context
  val firstSparkContext = new SparkContext("local", "firstSparkContext")
  val sourceRDD = firstSparkContext.parallelize(Seq("Vengat", "Senthil", "Mano", "Jei"))
  sourceRDD.foreach(println)
  firstSparkContext.stop()

  //  Second Context
  val secondSparkContext = new SparkContext("local", "secondSparkContext")
  val source2RDD = secondSparkContext.parallelize(Seq("1", "2", "3", "4", "5"))
  source2RDD.foreach(println)
  secondSparkContext.stop()


}
