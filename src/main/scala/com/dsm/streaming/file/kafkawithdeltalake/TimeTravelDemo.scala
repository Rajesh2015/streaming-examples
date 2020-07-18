package com.dsm.streaming.file.kafkawithdeltalake

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object TimeTravelDemo {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.hadoop.fs.s3a.access.key", s3Config.getString("access_key"))
      .config("spark.hadoop.fs.s3a.secret.key", s3Config.getString("secret_access_key"))
      .appName("DeltaTable Example")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)
    import spark.implicits._

    val deltaTablePath = s"s3a://${s3Config.getString("s3_bucket")}/delta_Kafka_data"

    val step = "version"
    step match {
      case "all_versions" =>
        val deltaDf = DeltaTable.forPath(spark, deltaTablePath)
        deltaDf
          .history()
          .orderBy($"version")
          .show(false)

      case "version" =>
        spark
          .read
          .format("delta")
         // .option("versionAsOf", 11)
          .load(deltaTablePath)
          .show()
    }
  }
}
