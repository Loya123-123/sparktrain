package com.yjz.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object WindowTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 周期水位时间 ，默认是200
    env.getConfig.setAutoWatermarkInterval(100L)

//    val inPutPath = "/Users/loay/idea-workspace/sparktrain/FlinkTest/src/main/resources/sensor.txt"
//    val stream = env.readTextFile(inPutPath)
    val host = "localhost"
    val port = 7777
    val stream = env.socketTextStream(host,port)


    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })


    val minTemperwindowStream = dataStream
      //      .assignAscendingTimestamps(_.timestamp * 1000)  // 指定时间字段
      //      .assignTimestampsAndWatermarks(new MyAssigner())  // 自定义乱序 水位
      // 自定义水位
      // 时间线 左闭右开
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
    })
      .map(
      data => (data.id,data.temperature))
      .keyBy(_._1)
//      .timeWindow(Time.seconds(10))  // 滚动窗口
      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8))) //东八区时间回推 八小时 采用国际时间
//      .timeWindow(Time.seconds(10),Time.seconds(5))  // 滑动窗口

      .reduce((x,y)=>(x._1,x._2.min(y._2)))

    dataStream.print("input data")
    minTemperwindowStream.print("output data")

    env.execute("window job")

  }
}

class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading]{
  // 延迟
  val bound = 6000
  // 最大时间戳
  var maxTs = Long.MinValue
  // 取 当前时间戳和数据时间最大的一个
  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)


  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    // 取 当前时间戳和数据时间最大的一个
    maxTs = maxTs.max(t.timestamp * 1000)
    t.timestamp * 1000
  }
}