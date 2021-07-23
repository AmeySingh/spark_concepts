package Spark_Concepts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types._

object Chapter3_JSONRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Example3.json")
      .config("spark.master", "local")
      .getOrCreate()
    if (args.length <= 0) {
      println("usuage Example3.json <file path to blogs.json>")

      System.exit(1)
    }
    //path to jason
    val jsonFile = args(0)
    //Schema progametically
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)
    ))
    //Creating a DF to read from json file
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    //showing dataframe
    blogsDF.show(false)
    //print the schema
    println(blogsDF.printSchema)
    println(blogsDF.schema)
    //printing all coloumn
    println(blogsDF.columns)
    // particular coloumn
    println(blogsDF.col("Hits"))
    //using experssion to compute a value
    print(blogsDF.select(expr("Hits * 2")).show(5))
    // using col to compute a value
    println(blogsDF.select(col("Hits") * 2).show(2))


  }
}
