package Spark_Concepts


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Chapter3R_WFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
    .builder()
    .appName("reading.csv")
    .config("spark.master","local")
    .getOrCreate()

    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDispostion", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", StringType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighbourhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)
    ))
    val sfFireFile = "/Users/ameyc/Documents/Study_Notes/sf-fire-calls.csv"
    var fireDF = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)

      fireDF.show(5,false)
    //Writing  file in
    //fireDF.write.parquet("/Users/ameyc/Documents/Study_Notes/Fire.parquet")

    //Transformations
   val fewFireDF = fireDF
      .select("IncidentNumber","AvailableDtTm","CallType")
      .where(col("CallType")  === "Medical Incident")
    fewFireDF.show(5,false)
    fireDF.select("CallType").where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes").show()
    //selecting distinct vlaue
   fireDF.select("CallType").distinct().show(10,false)
    //Renaming,adding & dropping coloumn
  }
}