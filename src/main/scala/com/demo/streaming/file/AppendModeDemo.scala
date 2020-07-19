package com.demo.streaming.file

import com.demo.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object AppendModeDemo {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "/")
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Streaming Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val dataPath = s"s3n://${s3Config.getString("s3_bucket")}/droplocation"

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
      .schema(schema)
      .csv(dataPath)

    println("Is the stream ready? " + fileStreamDF.isStreaming)

    fileStreamDF.printSchema()

    val trimmedDF = fileStreamDF
//      .filter("city = 'Southwark' and minor_category = 'Possession Of Drugs'")
      .select("city", "year", "month", "value")
      .withColumnRenamed("value","convictions")

    val query = trimmedDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 30)
      .start()
      .awaitTermination()

  }
}
