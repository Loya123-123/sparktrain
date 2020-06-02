package com.yjz.apitest



import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    // 设置基础环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 并行度 1
    env.setParallelism(1)
    // 滚动窗口， 水位线 事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 检查点时间间隔
    env.enableCheckpointing(60000)
    // 一致性模式
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    // 超时放弃当前检查点
    env.getCheckpointConfig.setCheckpointTimeout(100000)
    // 检查点报错时是否接受任务 。默认 true
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 检查点并行数
    //    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 检查点间隔时间 ， 毫秒 ，和并行度冲突
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    // 检查点是否存储。
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    // 重启策略 尝试次数、重启间隔
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.seconds(300), org.apache.flink.api.common.time.Time.seconds(10)))

    val input = env.socketTextStream("localhost", 7777)
    val dataStream = input.map(data => {
      val arrayData = data.split(",")
      SensorReading(arrayData(0).trim, arrayData(1).trim.toLong, arrayData(2).trim.toDouble)
    }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    })
    dataStream.print("input data")

//    val processData = dataStream
//      .keyBy(_.id)
//      .process(new TempChangeAlert(10.0))

    val processData2 = dataStream
      .keyBy(_.id)
      .flatMap(new TempChangeAlert2(10.0))

    val processData3 = dataStream
        .keyBy(_.id)
        .flatMapWithState[(String,Double,Double),Double]{
      // 如果没有状态的话，也就是没有数据来过，那么就将当前数据温度值存入状态
      case (input : SensorReading,None) => (List.empty,Some(input.temperature))
      // 如果有状态，就应该与上次的温度值比较差值，如果大于阈值就输出报警
      case (input : SensorReading , lastTemp : Some[Double]) =>
        val diff = (lastTemp.get - input.temperature).abs
        if (diff > 10.0){
          (List((input.id,lastTemp.get,input.temperature)) , Some(input.temperature))
        }else {
          (List.empty,Some(input.temperature))
        }
    }

    processData3.print("processed data")


    env.execute("job start")
  }



}

// flatMap 富函数
class TempChangeAlert2(i: Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)] {

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()

    val diff = (lastTemp - value.temperature).abs

    if (diff > i){
      out.collect(value.id,lastTemp,value.temperature)
    }
    lastTempState.update(value.temperature)
  }

}

// 实现key 状态函数

class TempChangeAlert (threshold:Double) extends KeyedProcessFunction[String,SensorReading,(String,Double,Double)]{
  // 定义温度状态
  lazy val lastTempState :ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String,Double,Double)]#Context, out: Collector[(String,Double,Double)]): Unit = {
      // 获取上一次温度值
    val lastTemp = lastTempState.value()
    // 上次和这次的差值
    val diff = (lastTemp - value.temperature).abs
     // 如果差值大于预期 就输出
    if(diff > threshold){
      out.collect(value.id, lastTemp ,value.temperature)

    }

    // 更新状态
    lastTempState.update(value.temperature)

  }
}