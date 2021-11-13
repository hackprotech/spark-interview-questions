package com.hackpro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}

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
  val sparkContext = new SparkContext("local", "SparkContext")
  val sourceFile = sparkContext.textFile("src/main/resources/Used_Bikes.csv")
  println("\n*********************** SparkContext Results ************************")
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
  println("\n*********************** Spark HiveContext Results ************************")
  val sparkContext2 = new SparkContext("local", "HiveContext")
  val hiveContext = new HiveContext(sparkContext2)

  import hiveContext.{sql => contextHiveSQL}

  contextHiveSQL("create database IF NOT EXISTS organization")
  contextHiveSQL(
    """
      |CREATE TABLE IF NOT EXISTS organization.employee
      |(emp_id INTEGER, emp_name STRING, role STRING)
      |""".stripMargin)

  contextHiveSQL(
    """
      |INSERT INTO organization.employee(emp_id, emp_name, role)
      |values(1, 'Mano', 'UI Developer'), (2, 'Senthil', 'Data Scientist'), (3, 'Vengat', 'Data Engineer')
      |""".stripMargin)

  contextHiveSQL("select * from organization.employee").show(10, truncate = false)
  sparkContext2.stop()


  // Spark Session
  val sparkSession = SparkSession.builder().master("local").appName("SparkSession").getOrCreate()
  val sparkSessionSourceFiles = sparkSession.read.schema(bike_schema).csv("src/main/resources/Used_Bikes.csv")

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
  val hiveSession = SparkSession
    .builder()
    .master("local")
    .appName("hiveSession")
    .enableHiveSupport()
    .getOrCreate()

  import hiveSession.sqlContext.{sql => sessionHiveSQL}

  sessionHiveSQL("create database IF NOT EXISTS video_games")
  sessionHiveSQL(
    """
      |CREATE TABLE IF NOT EXISTS video_games.battle_royal_games
      |(game_name STRING, compatibility STRING, price DOUBLE)
      |""".stripMargin)

  sessionHiveSQL(
    """
      |INSERT INTO video_games.battle_royal_games(game_name, compatibility, price)
      |values('CALL OF DUTY', 'PC/MOBILE', 2000.00), ('PUBG', 'PC/MOBILE', 2500.10), ('FORTNITE', 'PC/MOBILE/PS5', 3000.50)
      |""".stripMargin)

  sessionHiveSQL("select * from video_games.battle_royal_games").show(10, truncate = false)


}
