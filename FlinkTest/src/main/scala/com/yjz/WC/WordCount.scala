package com.yjz.WC

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文本中读取数据
    val inputPath = "/Users/loay/idea-workspace/sparktrain/FlinkTest/src/main/resources/hello.txt"

    val inputDataSet = env.readTextFile(inputPath)

    // 切分之后做count
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    wordCountDataSet.print()
  }
}
