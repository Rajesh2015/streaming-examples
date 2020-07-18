package com.dsm.streaming.file.kafkawithdeltalake

import com.typesafe.config.ConfigFactory
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object KafkaConsumerWithDeltaTable {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.hadoop.fs.s3a.access.key", s3Config.getString("access_key"))
      .config("spark.hadoop.fs.s3a.secret.key", s3Config.getString("secret_access_key"))
      .appName("Crime Data Stream").getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    val deltaKafkaTable = s"s3a://${s3Config.getString("s3_bucket")}/delta_Kafka_data"
    val checkPointdir = s"s3a://${s3Config.getString("s3_bucket")}/checkpointdir"

    val inputDf = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ec2-54-194-95-122.eu-west-1.compute.amazonaws.com:9092") //.option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "kafka-topic")
      .option("startingOffsets", "earliest")
      .option("checkpointLocation", checkPointdir)
      .load()
      .withColumn("value", col("value").cast(StringType))
      .withColumn("txn_id", split(col("value"), ",").getItem(0))
      .withColumn("amount", split(col("value"), ",").getItem(1)).
      withColumn("status", split(col("value"), ",").getItem(2)).
      drop("key", "value", "topic", "partition", "timestampType", "offset")
    //Initial Schema
    val inititalDf = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], inputDf.schema)
    inititalDf.show()
    inititalDf
      .write
     // .option("mergeSchema", "true")
      .mode(SaveMode.Ignore)//If exits should not create initial table
      .format("delta")
      .save(deltaKafkaTable)

    val consoleOutput = inputDf
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    val deltaOut = inputDf.writeStream
      .format("delta")
      .outputMode("append")
      .foreachBatch { (inputDf: DataFrame, batchid: Long) => {
        val deltaTable = DeltaTable.forPath(sparkSession, deltaKafkaTable)
        deltaTable.alias("delta_kafka")
          .merge(inputDf.alias("updatedf"), "delta_kafka.txn_id = updatedf.txn_id")
          .whenMatched()
          .updateExpr(Map("timestamp" -> "updatedf.timestamp", "amount" -> "updatedf.amount", "status" -> "updatedf.status"))
          .whenNotMatched()
          .insertExpr(Map("timestamp" -> "updatedf.timestamp","txn_id"->"updatedf.txn_id","amount" -> "updatedf.amount", "status" -> "updatedf.status"))
          .execute()

      }
      }.option("checkpointLocation", checkPointdir)
      .start(deltaKafkaTable)
    deltaOut.awaitTermination()
    consoleOutput.awaitTermination()
  }


}
