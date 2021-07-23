package Spark_Concepts

import org.apache.spark.sql.SparkSession


object SQL_Queries2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQLExampleApp")
      .config("spark.master","local")
      .getOrCreate()

    // creating SQL database
    spark.sql("SET spark.sql.legacy.createHiveTableByDefault= false")
    spark.sql("CREATE  DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")
    spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination INT)")

  }

}
