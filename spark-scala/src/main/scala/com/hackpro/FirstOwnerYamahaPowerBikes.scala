package com.hackpro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FirstOwnerYamahaPowerBikes extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  // 1. set the sparkContext
  val sparkContext = new SparkContext("local", "FirstOwnerYamahaPowerBikes")

  // 2. To read the csv files
  val sourceBikesRDD = sparkContext.textFile("src/main/resources/Used_Bikes.csv").repartition(3)

  // 3. Parse the line and split into columns
  val splittedRDD = sourceBikesRDD.map(line => line.split(",").map(column => column.trim))

  //  4. Filter records based on Usecase Criteria
  val resultRDD = splittedRDD.filter(bike => bike(4).equalsIgnoreCase("First Owner")
    && bike(6).toDouble > 150 && bike(7).equalsIgnoreCase("Yamaha"))

  // 5. Collect the distinct results
  val collectedResult = resultRDD.map(bike => (bike(0), bike(4))).distinct().collect()

  // 6. Print the results
  collectedResult.foreach(println)

  // 7. Take count on collected results
  println("Count of the final Datasets " + resultRDD.count())

  scala.io.StdIn.readLine()

}
