package com.yjz.WC

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)

    val host : String = params.get("host")
    val port : Int = params.getInt("port")

    // 接受一个socket
    // 在本地终端创建 端口 nc -lk 7777
    val dataStream = env.socketTextStream(host,port)

    // 每条数据进行处理 定义处理流程
    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)
    // 默认 并行度 为 开发环境 CPU 数量 。setParallelism 设置并行度
    wordCountDataStream.print()

    // 启动 executor
    env.execute("stream start job ")
  }
}
