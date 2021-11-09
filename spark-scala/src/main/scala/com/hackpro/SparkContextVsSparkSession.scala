package com.hackpro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}

import java.io.File

object SparkContextVsSparkSession extends App {

  // Logger
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Schema
  val bike_schema = StructType(
    List(
      StructField("model", StringType),
      StructField("price", DoubleType),
      StructField("location", StringType),
      StructField("kms_driven", DoubleType),
      StructField("owner", StringType),
      StructField("age", DoubleType),
      StructField("cc", DoubleType),
      StructField("brand", StringType)
    )
  )

  //  Spark Context
  println("\n*********************** SparkContext Results ************************")
  val sparkContext = new SparkContext("local", "SparkContext")
  val sourceFile = sparkContext.textFile("src/main/resources/Used_Bikes.csv")
  sourceFile.take(5).foreach(println)
  sparkContext.stop()

  // SQLContext
  println("\n*********************** Spark SQLContext Results ************************")
  val sparkContext1 = new SparkContext("local", "SQLContext")
  val sqlContext = new SQLContext(sparkContext1)
  val sqlContextSourceFiles = sqlContext.read.schema(bike_schema).csv("src/main/resources/Used_Bikes.csv")
  sqlContextSourceFiles.createOrReplaceTempView("t_used_bikes")
  val royalEnfieldBikes = sqlContext.sql(
    """
      |select * from t_used_bikes
      |where brand = 'Royal Enfield'
      |""".stripMargin)
  royalEnfieldBikes.show(5, truncate = false)
  sparkContext1.stop()

  // Hive Context
  /* val sparkContext2 = new SparkContext("local", "HiveContext")
   val hiveContext = new HiveContext(sparkContext2)*/

  // Spark Session
  val sparkSession = SparkSession.builder().master("local").appName("SparkSession").getOrCreate()
  sparkSession.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://192.168.228.128:9000")
  val sparkSessionSourceFiles = sparkSession.read.schema(bike_schema).csv("/user/vengat/datasets/Used_Bikes.csv")

  println("\n*********************** SparkSession Results ************************")
  sparkSessionSourceFiles.show(5, truncate = false)

  println("\n*********************** SparkSession SQL Results ************************")
  sparkSessionSourceFiles.createOrReplaceTempView("t_used_bikes_list")
  val tvsBikes = sparkSession.sql(
    """
      |select * from t_used_bikes_list
      |where brand = 'TVS'
      |""".stripMargin)
  tvsBikes.show(5, truncate = false)
  sparkSession.stop()

  println("\n*********************** SparkSession with Hive Results ************************")
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  val hiveSession = SparkSession
    .builder()
    .master("local")
    .appName("hiveSession")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()
  hiveSession.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://hadoop-master:9000")

  import hiveSession.sql

  sql(
    """
      |CREATE TABLE IF NOT EXISTS bikes_db.USED_BIKES (model STRING, price DOUBLE, location STRING, kms_driven DOUBLE,
      |owner STRING, age DOUBLE, cc DOUBLE, brand STRING)
      |""".stripMargin)

  sql(
    """
      |LOAD DATA LOCAL INPATH '/user/vengat/datasets/Used_Bikes.csv' INTO TABLE bikes_db.USED_BIKES
      |""".stripMargin)


  sql(
    """
      |select * from used_bikes where brand = 'Honda'
      |""".stripMargin)
    .show(10, truncate = false)


}
