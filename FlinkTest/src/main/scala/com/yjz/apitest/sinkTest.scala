package com.yjz.apitest

import java.sql.{DriverManager, PreparedStatement}
import java.{sql, util}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object sinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new MySource())
    val dataStream = stream.map(data => SensorReading(data.id,data.timestamp,data.temperature))
//    dataStream.addSink(new FlinkKafkaProducer011[String]("node101:9092,node102:9092,node103:9092","test",new SimpleStringSchema()))

    val httpHost = new util.ArrayList[HttpHost]()
    httpHost.add(new HttpHost("123.57.235.215", 9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHost,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data : " + t)
          // 包装成一个map 或者 jsonObject

          val json = new util.HashMap[String,String]()

          json.put("sid",t.id)
          json.put("temperature",t.temperature.toString)
          json.put("timestamp",t.timestamp.toString)
          // 创建 index request
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("doc")
            .source(json)
          print(indexRequest)
          // 利用 Request index 发送请求 写入数据

          requestIndexer.add(indexRequest)

          println("data saved ")
        }
      }
    )
    dataStream.addSink(new MyJdbcSink())
    env.execute("kafka pro start")
  }
}

class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  // 定义sql连接 预编译
  var conn: sql.Connection = _
  var insertStmt : PreparedStatement = _
  var updateStmt : PreparedStatement = _



  // open 主要是创建连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://node101:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
    // 调用连接，执行 sql
  }
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, value.temperature)

    updateStmt.setString(2, value.id)
    updateStmt.execute()
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}











