package com.yjz.apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

case class Student (uid : Long,name : String,age: Int)

object TableAPITest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input = env.socketTextStream("localhost",7777)

    val tableEnv = TableEnvironment.getTableEnvironment(env)


    val dataStream = input.map{data =>
    val arraydate = data.split(",")
      Student(arraydate(0).trim.toLong,arraydate(1).trim,arraydate(2).trim.toInt)
  }
    val studentTable  = tableEnv.fromDataStream(dataStream)
      .select("uid,name,age").filter("age=25")

    val midchDataStream = studentTable.toAppendStream[(Long,String,Int)].print()
//    tableEnv.fromDataStream()
    env.execute("tableJson")
  }
}
