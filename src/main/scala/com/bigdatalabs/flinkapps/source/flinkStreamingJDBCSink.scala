package com.bigdatalabs.flinkapps.source

import com.bigdatalabs.flinkapps.entities.model._

import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.formats.avro.AvroWriters
import org.apache.avro.Schema

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.configuration.Configuration

//import org.apache.flink.connector.jdbc._
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import java.sql.PreparedStatement

import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.runtime.fs.hdfs.HadoopDataOutputStream
import org.apache.flink.table.planner.expressions.CurrentTime

import java.io.{FileInputStream, FileNotFoundException, IOException}
import java.sql.{Connection, DriverManager, PreparedStatement}

//Streaming to JDBC Sink
object flinkStreamingJDBCSink {

  def main(args: Array[String]): Unit = {

    //Vars for Property file
    var _propFile: FileInputStream = null
    var _params: ParameterTool = null

    //Check for Propoerties File
    try {
      _propFile= new FileInputStream("src/main/resources/flinkApps.properties")
      _params = ParameterTool.fromPropertiesFile(_propFile)
    } catch {
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Had an IOException trying to read that file")
    } finally {

    }

    //fetch Kafka Parameters
    val _topic_source: String = "loc-flnk-src" //_params.get("LOC_KFKA_SRC")
    val _topic_sink: String = "loc-flnk-snk"//_params.get("LOC_KFKA_SNK")
    val _groupId: String = "kfka-flnk"//_params.get("KFKA_CONS_GRP")
    val _brokers: String = _params.get("BOOTSTRAP_SERVERS")

    //Print Params
    println("Awaiting Stream . . .")
    print("=======================================================================\n")
    println(
      "[TOPIC SOURCE : " + _topic_source +","
        + "TOPIC SINK: " + _topic_sink + ","
        + "GROUP: " + _groupId + "]"
    )

    val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment //Entry Point

    _env.getConfig.setGlobalJobParameters(_params)
    _env.enableCheckpointing(10000) // start a checkpoint every 10000 ms
    _env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000)//Pause between Check Points - milli seconds
    _env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)// set mode to exactly-once (this is the default)
    _env.getCheckpointConfig.setCheckpointTimeout(60000)// checkpoints have to complete within one minute, or are discarded
    _env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)// prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
    _env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)// allow only one checkpoint to be in progress at the same time
    _env.getConfig.setAutoWatermarkInterval(2000)// generate a Watermark every second
  //_env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//Deprecated,Event Time, Ingestion Time, Processing Time
    _env.setParallelism(1)

    //INput Stream
    //val _inputStream = _env.readTextFile("file:///home/bdluser/Dataops/dataSources/sensor_data.csv")
    //_inputStream.print()

    //Kafka Params
    val _fromSource = KafkaSource.builder[String]
      .setBootstrapServers(_brokers)
      .setTopics(_topic_source)
      .setGroupId(_groupId)
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setStartingOffsets(OffsetsInitializer.latest())
      .build()

    //Receive from Kafka
    val _inputStream = _env.fromSource(_fromSource, WatermarkStrategy.noWatermarks(), "New Kafka Source from 1.14.4")
    //_inputStream.print()

    //Parse data from Input
    val _dataStream = _inputStream.map(
      _rowData => {
        val _dataArray = _rowData.split(",")
        sensorReading(_dataArray(0).trim,_dataArray(1).toLong,_dataArray(2).toFloat)
      }
    ).map(
      _parsedRecord =>
        sensorReading(
          _parsedRecord.sensorId,_parsedRecord.sensorTStamp,_parsedRecord.sensorTemp
        )
    ).filter(Y => Y.sensorTemp <= -10 || Y.sensorTemp >= 50)
    //.filter(Y => Y.sensorId == "sensor_1" && Y.sensorTemp <= -10 || Y.sensorTemp >= 50)

    //Publish Result, Filter, Aggregate
    //val _resultStream = _dataStream.map(y=> y.sensorId + "," + y.sensorTStamp + "," + y.sensorTemp)
    //_resultStream.print()

    //Sink Data to Database
    _dataStream.addSink(new mmyJDBCSink())

    /*
    *    _dataStream.map(_parsedRecord =>
      sensorReading(
        _parsedRecord.sensorId,_parsedRecord.sensorTStamp,_parsedRecord.sensorTemp)
    )
    * */

    _dataStream.addSink(
     JdbcSink.sink(
          "INSERT INTO flinkops.t_flnk_sensordata(sensor_id, sensor_ts,sensor_temp) VALUES (?,?,?);", new JdbcStatementBuilder[sensorReading] {
         override def accept(statement: PreparedStatement, sr: sensorReading): Unit = {
              statement.setString(1, sr.sensorId)
              statement.setLong(2, System.currentTimeMillis()/1000)//statement.setLong(2, sr.sensorTStamp)
              statement.setFloat(3, sr.sensorTemp)
            }},
    JdbcExecutionOptions.builder()
      .withBatchSize(1000)
      .withBatchIntervalMs(200)
      .withMaxRetries(5)
      .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:mysql://localhost:3306/flinkops")
          //.withDriverName("org.postgresql.Driver")
          .withUsername("root")
          .withPassword("sqlpwd")
          .build()
      )
    )

    _env.execute("flink streaming to mysql")

  }
}

//JDBC , Insert and Update Code Blocks
private class mmyJDBCSink() extends RichSinkFunction[sensorReading]{

  var _connParams: Connection = _
  var _insertStmt: PreparedStatement = _
  var _updateStmt: PreparedStatement = _

  //Open Conn
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val _connStr = "jdbc:mysql://localhost:3306/flinkops"
    val _userName = "root"
    val _passwd = "sqlpwd"

    _connParams = DriverManager.getConnection(_connStr, _userName, _passwd)
    _insertStmt = _connParams.prepareStatement("INSERT INTO flinkops.t_flnk_sensordata(sensor_id, sensor_ts,sensor_temp) VALUES (?,?,?);")
    _updateStmt = _connParams.prepareStatement("UPDATE flinkops.t_flnk_sensordata set sensor_ts=?,sensor_temp=? WHERE sensor_id=?;")
  }
  //Insert or Update Data
  override def invoke(value: sensorReading, context: SinkFunction.Context): Unit = {

    _updateStmt.setLong(1, System.currentTimeMillis()/1000)//value.sensorTStamp)
    _updateStmt.setFloat(2,value.sensorTemp)
    _updateStmt.setString(3, value.sensorId)
    _updateStmt.execute()

    if(_updateStmt.getUpdateCount==0) {
      _insertStmt.setString(1,value.sensorId)
      _insertStmt.setLong(2,System.currentTimeMillis()/1000)//value.sensorTStamp)
      _insertStmt.setFloat(3,value.sensorTemp)

      _insertStmt.execute()

    }
  }
  //Close Connections
  override def close(): Unit = {
    _updateStmt.close()
    _insertStmt.close()
    _connParams.close()
  }

}