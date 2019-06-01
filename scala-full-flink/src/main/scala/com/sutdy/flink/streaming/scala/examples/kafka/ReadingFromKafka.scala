package com.sutdy.flink.streaming.scala.examples.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * https://blog.csdn.net/lmalds/article/details/51780950 编写Flink程序，实现consume kafka的数据（demo）
  */
object ReadingFromKafka {
//  private val ZOOKEEPER_HOST = "192.168.174.101:2181,192.168.174.102:2181,192.168.174.103:2181"
//  private val KAFKA_BROKER = "192.168.174.101:9092,192.168.174.102:9092,192.168.174.103:9092"
  private val ZOOKEEPER_HOST = "192.168.174.101:2181"
  private val KAFKA_BROKER = "192.168.174.101:9092"
  private val TRANSACTION_GROUP = "JSH-YD"
  private val TOPIC_NAME = "JSH-YD-2"

  def main(args: Array[String]): Unit = {
    println("aaa....")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    //隐式转换
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // TODO,虚拟机NET方式，连接不上kafka，本地kafka测试成功，待验证桥接方式kafka连接；消息乱码待解决
    val kafkaProps: Properties = new Properties()
    kafkaProps.setProperty("zookeeper.connect", "127.0.0.1:2181")
    kafkaProps.setProperty("bootstrap.servers", "127.0.0.1:9092")
    kafkaProps.setProperty("group.id", "JSH-YD-2")
    kafkaProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.setProperty("auto.offset.reset", "latest")

    println("bbb....")
    //topicd的名字是new，schema默认使用SimpleStringSchema()即可，对应服务器kafka0.11t版本
    val myConsumer = new FlinkKafkaConsumer011[String](TOPIC_NAME, new SimpleStringSchema(), kafkaProps)
    val transaction: DataStream[String] = env.addSource(myConsumer)
//        transaction.addSink(tup => {
//          println("abc...",tup)
//        })
//    transaction.print()
//    transaction.print("ewfwwe...")
    transaction.writeAsText("/data/flink/kafka-data")

    println("ccc....")

    env.execute("kafka flink")
    println("ddd....")
  }


}


//class CustomWatermarkEmitter extends AssignerWithPunctuatedWatermarks[String]{
//  override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long): Watermark = {
//    return new Watermark(1L)
//  }
//
//  override def extractTimestamp(element: String, previousElementTimestamp: java.lang.Long): java.lang.Long = {
//    if(element != null && element.contains(",")) {
//
//      return java.lang.Long.parseLong(element.split(",")(0))
//    }
//    return 0
//  }
//}






















