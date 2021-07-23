import org.apache.spark.sql.{SaveMode, SparkSession}
//Spark session includes Spark context,Sql context, Streaming context.


object TestSparkSession {

  def main(args:Array[String]):Unit={
    val session = SparkSession.builder()
      .appName("App Demo")
      .config("spark.master","local")
      .getOrCreate();

    //val sc = session.sparkContext
    //val clicks  = sc.textFile("",4
    val ssc = session.sqlContext
    //since spark json reads only oneline json we specify the multiline true for multiple line json
    //val clk = ssc.read.option("multiline","true").json("src/main/resources/click.json")
    val imprs = ssc.read.option("multiline","true").json("src/main/resources/impression.json")
    imprs.groupBy("app_id","country_code")
    //val result =clk.groupBy("impression_id").sum("revenue").show()
    //result.rdd.saveAsTextFile("file:///C:/SparkScalaCourse/spark_concepts/src/main/resources/output1")
    //result.write.format("json").save("file:///C:/SparkScalaCourse/spark_concepts/src/main/resources/output1")
    //result.write.mode(SaveMode.valueOf(Double)).text("src/main/resources/output3")
    //result.write.text("src/main/resources/output3")



  }

}
