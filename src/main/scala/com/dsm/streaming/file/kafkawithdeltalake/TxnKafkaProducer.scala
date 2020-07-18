package com.dsm.streaming.file.kafkawithdeltalake

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.StdIn.readLine

class TxnKafkaProducer {
  val newArgs: Array[String] = Array("20", "kafka-topic", "ec2-54-194-95-122.eu-west-1.compute.amazonaws.com:9092")
  val events: Int = newArgs(0).toInt
  val topic: String = newArgs(1)
  val brokers: String = newArgs(2)

  def configureKafkaProducer(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }
  def sendMessage(): Unit = {
    val listofmessage = List(
      "txn01,1000,100",
      "txn02,100,101",
      "txn03,5000,102",
      "txn01,3000,103",
      "txn02,1000,100",
       "txn04,4000,100")
    val props = configureKafkaProducer()
    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    var counter = 0
    while (counter < listofmessage.size) {
      println("Press 1 to send message and any other key to exit")
      readLine() match {
        case "1" => {
          val key = "txnid" + events.toString
          val msg = listofmessage(counter)
          val data = new ProducerRecord[String, String](topic, key, msg)
          producer.send(data)
          counter += 1
          print(s"Message Sent--> ${msg}")
          System.out.println("sent per second: " + counter * 1000 / (System.currentTimeMillis() - t))
        }
        case _ => {
          producer.close()
          System.exit(1)
        }

      }
      //    for (nEvents <- Range(0, events)) {

    }
  }
}
object  TxnKafkaProducer extends App {
  var producer = new TxnKafkaProducer();
  producer.configureKafkaProducer()
  producer.sendMessage()
}
