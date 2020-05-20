package com.yjz.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import scala.util.Random

// 定义样例类，传感器 id，时间戳，温度
case class SensorReading(id : String ,timestamp : Long ,temperature : Double)

object SensorReading{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.从自定义的集合中读取数据
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
    stream1.print("stream1").setParallelism(1)

//    env.fromElements("直接添加数据")

    // 2.从文件读取数据
    val stream2 = env.readTextFile("/Users/loay/idea-workspace/sparktrain/FlinkTest/src/main/resources/hello.txt")

    stream2.print("stream2").setParallelism(1)

    // 3、以 kafka 消息队列的数据作为来源
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "node101:9092,node102:9092,node103:9092" )
//    properties.setProperty("group.id", "consumer-group")
//    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset", "latest")
//    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("test",new SimpleStringSchema(),properties))
//    stream3.flatMap(_.split(" "))
//        .filter(_.nonEmpty)
//        .map((_,1))
//        .keyBy(0)
//        .sum(1)
//        .print("stream3").setParallelism(1)

    //自定义数据源
    val stream4 = env.addSource(new MySource())
    stream4.print("stream4").setParallelism(1)
    env.execute("chennl job")
  }
}

class MySource() extends SourceFunction[SensorReading]{
  // 定义一个flag ，表示数据源是否正常运行
  var running: Boolean = true
  // 正常生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数据
    val range = new Random()
    // 初始化定义一组温度数据
    var curTemp = 1.to(10).map(
      // nextGaussian 高斯随机数 （正态分布）c
      i =>("sensor_"+ i,60 + range.nextGaussian() * 10)
    )

    // 产生数据量
    while (running) {
      // 在前一次的基础上更新温度值
      curTemp = curTemp.map(
        t => (t._1,t._2 + range.nextGaussian())
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t =>sourceContext.collect(SensorReading(t._1,curTime,t._2))
      )
      Thread.sleep(1000)
    }
  }
  // 获取数据源生成
  override def cancel(): Unit = {
    running = false
  }
}