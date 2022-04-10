package com.bigdatalabs.flinkapps.source

/*
 * @Author: Anand
 * @Date: 2022/04/10
 * @Description: Flink Kafka Source and Sink with New Kafka Source and Sink , Flink-1.14.4

 */

import org.apache.flink.api.java.utils.ParameterTool

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object flinkStreamTable {

  def main(args: Array[String]): Unit = {

    if (args.length < 6) {
      System.err.println(
        s"""
           |Usage: --topic_source <source topic name> --topic_sink <sink topic name> --groupId <group name> --symb <symbol> --high <number> --low <number>
        """.stripMargin)
      System.exit(1)
    }

    //fetch Inputs
    val _params = ParameterTool.fromArgs(args)
    val _topic_source = _params.get("topic_source")
    val _topic_sink = _params.get("topic_sink")
    val _groupId = _params.get("groupId")
    val _brokers = "localhost:9092,localhost:9093,localhost:9094"

    //Thresholds
    val _symb = _params.get("symb")
    val _open = _params.get("open")
    val _high = _params.get("high")
    val _low = _params.get("low")
    val _close = _params.get("close")

    println("Awaiting Stream . . .")
    println("=======================================================================\n")
    println(
      "TOPIC SOURCE : " + _topic_source +","
        + "TOPIC SINK: " + _topic_sink + "|"
        + "GROUP: " + _groupId + ","
        + "SYMB: " + _symb + ","
        + "HIGH: " + _high
        + "," + "LOW: " + _low
    )

    // create environments of both APIs
    val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val _tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(_env)

    // create a DataStream
    val _dataStream = _env.fromElements(
      Row.of("Alice", Int.box(12)),
      Row.of("Bob", Int.box(10)),
      Row.of("Alice", Int.box(100))
    )(Types.ROW(Types.STRING, Types.INT))

    // interpret the insert-only DataStream as a Table
    val _inputTable = _tableEnv.fromDataStream(_dataStream).as("name", "score")

    // register the Table object as a view and query it
    // the query contains an aggregation that produces updates
    _tableEnv.createTemporaryView("InputTable", _inputTable)

    val _resultTable = _tableEnv.sqlQuery("SELECT name, SUM(score) FROM InputTable GROUP BY name")

    _resultTable.printSchema()

    // interpret the updating Table as a changelog DataStream
    val _resultStream = _tableEnv.toChangelogStream(_resultTable)

    // add a printing sink and execute in DataStream API
    _resultStream.print()

    _env.execute()

  }

}