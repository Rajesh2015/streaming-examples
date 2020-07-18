package com.dsm.streaming.file

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SqlDemo {

  val dataPath = "/Users/sidhartha.ray/Documents/workspace/streaming-examples/src/main/resources/datasets/droplocation"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "/")
    val sparkSession = SparkSession.builder().master("local[*]").appName("Crime Data Stream").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val schema = StructType(
      StructField("city_code", StringType, true) ::
        StructField("city", StringType, true) ::
        StructField("major_category", StringType, true) ::
        StructField("minor_category", StringType, true) ::
        StructField("value", StringType, true) ::
        StructField("year", StringType, true) ::
        StructField("month", StringType, true) :: Nil)

    val fileStreamDF = sparkSession.readStream
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)  // Rate limiting
      .schema(schema)
      .csv(dataPath)

    fileStreamDF.createOrReplaceTempView("CrimeData")

    println("Is the stream ready?" + fileStreamDF.isStreaming)

    val categoryDF = sparkSession.sql("SELECT major_category, value FROM CrimeData WHERE year = '2016'")


    val convictionsPerCategory = categoryDF.groupBy("major_category")
    .agg("value" -> "sum")
    .withColumnRenamed("sum(value)", "convictions")
    .orderBy("convictions")

    val query = convictionsPerCategory.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 30)
      .start()
      .awaitTermination()

    sparkSession.close()

  }
}
