package com.sutdy.flink.streaming.scala.examples.socket

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.descriptors.Kafka
import org.apache.flink.table.sources.{CsvTableSource, TableSource}

import scala.language.postfixOps

/**
  * https://blog.csdn.net/aA518189/article/details/83992129
  */
object flinkSQLTest1 {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //获取table
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //读取数据源
    val source1 = env.readTextFile("/data/flink/flink_data/person.txt")
    val source2: DataStream[Person1] = source1.map(x => {
      val split = x.split(" ")
      (Person1(split(0), split(1)))
    })
    //将DataStream转化成Table
    val table1: Table = tableEnv.fromDataStream(source2)
    //注册表，表名为：person
    tableEnv.registerTable("person", table1)
    //获取表中所有信息
    val rs: Table = tableEnv.sqlQuery("select *  from person where score = 'wemen' ")
    val stream: DataStream[String] = rs
      //过滤获取name这一列的数据
      .select("name")
      //将表转化成DataStream
      .toAppendStream[String]
    stream.print()
    //查询多列
    val stream1: DataStream[(String, String)] = rs.select("name,score").toAppendStream[(String, String)]
    stream1.print()

    env.execute("flinkSQL")
  }

}

/**
  * 定义样例类封装数据
  */
case class Person1(name: String, score: String)

/**
  * https://stackoverflow.com/questions/45295749/flink-csvtablesource-streaming
  */
object flinkSQLTest2 {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //获取table
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //读取数据源
    val csvtable: CsvTableSource = CsvTableSource.builder().path("/data/flink/flink_data/person-1.txt")
      //      .ignoreFirstLine() // 忽略首行
      .fieldDelimiter(",")
      .field("name", Types.STRING)
      .field("score", Types.STRING)
      .build()

    tableEnv.registerTableSource("person", csvtable)
    val table1: Table = tableEnv.scan("person").where("score = 'wemen'").select("name,score")

    val stream: DataStream[Person1] = tableEnv.toAppendStream[Person1](table1)
    stream.print()

    env.execute("flinkSQL-csv")
  }
}

/**
  * json解析
  * https://blog.csdn.net/baifanwudi/article/details/87883278
  * https://blog.csdn.net/lxhandlbb/article/details/83449399
  */
object flinkSQLTest3 {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //获取table
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //读取数据源
//    tableEnv.connect(new Kafka()).withFormat()


  }
}