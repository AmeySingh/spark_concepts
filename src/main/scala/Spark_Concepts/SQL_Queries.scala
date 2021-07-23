package Spark_Concepts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object SQL_Queries {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQLExampleApp")
      .config("spark.master","local")
      .getOrCreate()
    //viewing metadata
    //spark.catalog.listDatabases()
    // Path to data set
    val csvFile = "/Users/ameyc/Documents/Study_Notes/departuredelays.csv"
    // Read and create a temporary view
    // Infer schema (note that for larger files you may want to specify the schema)
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)
    // Create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")

    // flights who's distance is greater then 1,000miles
    spark.sql("""SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC""").show(10)

    //all the flight between San Fransico(SFO) and Chicago(ord)
    spark.sql("""SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC""").show(10)

    //df.withColumn("date", to_timestamp(col("date"),"M[m] hh")).show(10, false)
    // val res = df.select("date", unix_timestamp("date", "yyyy/MM/dd HH:mm:ss").cast(TimestampType).as("timestamp"))
    //Creating Views

  }
}
