package com.bigdatalabs.flinkapps.source

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.DataStream

import java.sql.SQLException

//Output Formats
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.io.{FileInputStream, FileNotFoundException, IOException}

//new Kafak Source API since in 1.14.4
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

//import org.apache.flink.connector.jdbc._
import java.sql.{Connection, DriverManager, PreparedStatement}

//Data Model
//import com.bigdatalabs.flinkapps.entities.model.sensorReading
import com.bigdatalabs.flinkapps.entities.model._


object flinkStreamingJDBCSink {

  //Global Vars for Property file
  private var _propFile: FileInputStream = _
  private var _params: ParameterTool = _
  private var _prop_file_path: String = _

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: --resourcefile <file path>
        """.stripMargin)
      System.exit(1)
    }
    else {
      //Fetch Property File Path from Input Parameter
      _prop_file_path = args(0)
      println("==============================================================================================================================================")
      println("RESOURCE FILE:" + _prop_file_path)
      println("==============================================================================================================================================")
    }

    //Check for Propoerties File
    try {
      //Fetch Properties File
      _propFile = new FileInputStream(_prop_file_path)
    } catch {
      case e: FileNotFoundException => println("COULD NOT FIND THE RESOURCE FILE.")
        println(e.printStackTrace())
        System.exit(100)
      case e: IOException => println("Had an IOException trying to read that file")
        println(e.printStackTrace())
        System.exit(200)
    } finally {

      //get parameters from
      _params = ParameterTool.fromPropertiesFile(_propFile)

      //fetch Kafka Parameters
      val _topic_source = _params.get("LOC_KFKA_SRC")
      val _topic_sink = _params.get("LOC_KFKA_SNK")
      val _groupId = _params.get("KFKA_CONS_GRP")
      val _brokers = _params.get("BOOTSTRAP_SERVERS")

      //Print Params
      println("==============================================================================================================================================")
      println(
        "[TOPIC SOURCE : " + _topic_source + ","
          + "TOPIC SINK: " + _topic_sink + ","
          + "GROUP: " + _groupId + ","
          + "BROKERS:" + _brokers + "]"
      )
      println("==============================================================================================================================================")
      println("Awaiting Stream . . .")

      //Flink Specific Settings.
      val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment //Entry Point
      _env.setRuntimeMode(RuntimeExecutionMode.STREAMING) //BATCH, AUTOMATIC
      _env.getConfig.setGlobalJobParameters(_params)
      _env.enableCheckpointing(10000) // start a checkpoint every 10000 ms
      _env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000) //Pause between Check Points - milli seconds
      _env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // set mode to exactly-once (this is the default)
      _env.getCheckpointConfig.setCheckpointTimeout(60000) // checkpoints have to complete within one minute, or are discarded
      _env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
      _env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) // allow only one checkpoint to be in progress at the same time
      _env.getConfig.setAutoWatermarkInterval(1000) // generate a Watermark every second
      //_env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//Deprecated,Event Time, Ingestion Time, Processing Time

      //Kafka Params
      val _from_loc_kfka_src = KafkaSource.builder[String]
        .setBootstrapServers(_brokers)
        .setTopics(_topic_source)
        .setGroupId(_groupId)
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .setStartingOffsets(OffsetsInitializer.latest())
        //.setStartingOffsets(OffsetsInitializer.earliest())
        //.setStartingOffsets(OffsetsInitializer.timestamp(1651400400L))
        .build()

      //Receive from Kafka
      val _inputStream: DataStream[String] = _env.fromSource(_from_loc_kfka_src, WatermarkStrategy.noWatermarks(), "New Kafka Source from 1.15.0")

      //Parse data from Input
      val _filteredStream = _inputStream.map(
        _rowData => {
          val _dataArray = _rowData.split(",")
          sensorReading(_dataArray(0).trim, _dataArray(1).toLong, _dataArray(2).toFloat)
        }
      ).map(
        _parsedRecord =>
          sensorReading(
            _parsedRecord.sensorId, _parsedRecord.sensorTStamp, _parsedRecord.sensorTemp
          )
      )//.filter("Use SQL Filter Conditions")

      //Add JDBC Sink for interset and Update Operations
      _filteredStream.addSink(new myJDBCSinkUpsert())

      //Execute
      _env.execute("Flink Batch JDBC Insert and Update on 1.15.0")
    }
  }

  //JDBC Sink Implementation
  class myJDBCSinkUpsert() extends RichSinkFunction[sensorReading] {
    Console.out.println("Entering JDBC Block")

    var _connParams: Connection = _
    var _insertSQL: String = _
    var _updateSQL: String = _

    var _insertStmt: PreparedStatement = _
    var _updateStmt: PreparedStatement = _


    private val _driverName = "org.postgresql.Driver"
    private val _connStr = "jdbc:postgresql://localhost:5432/dataopsdb"
    private val _userName = "dopsuser"
    private val _passwd = "dopspwd"

    //Open Conn
    override def open(parameters: Configuration): Unit = {

      try{
        super.open(parameters)
        Class.forName(_driverName)
        _connParams = DriverManager.getConnection(_connStr, _userName, _passwd)
      }
      catch {
        case e: SQLException => println("SQL Exception : Check If Connection is Valid")
          println("==============================================================================================================================================")
          println(e.printStackTrace())
          System.exit(100)
      }
      finally {
        Console.out.println("Connection is Valid")

        //Prepare Parameterable Statements,Use Callable Statement for calling Stored Procedures
        _insertSQL = //For Insert
          s"""
             |INSERT INTO streamingdb.t_flnk_temperature(sensor_id, sensor_ts,sensor_temp,iteration)
             |VALUES (?,?,?,?);
             |""".stripMargin

        _updateSQL = //For Update
          s"""
             |UPDATE streamingdb.t_flnk_temperature
             |SET sensor_ts=?,sensor_temp= sensor_temp + ?, iteration=iteration + ?
             |WHERE sensor_id=?;
             |""".stripMargin

        //Prepare Template Statements for INSERT and UPDATE
        _insertStmt= _connParams.prepareStatement(_insertSQL)
        //OR
        _updateStmt= _connParams.prepareStatement(_updateSQL)
      }
    }

    //Insert or Update Data
    override def invoke(sensorvalue: sensorReading, context: SinkFunction.Context): Unit = {

      val _iterator: Int = 1

      //default Update
      _updateStmt.setLong(1, System.currentTimeMillis() / 1000) //value.sensorTStamp)
      _updateStmt.setFloat(2, sensorvalue.sensorTemp)
      _updateStmt.setInt(3, _iterator)
      _updateStmt.setString(4, sensorvalue.sensorId)
      //Execute
      _updateStmt.execute()

      //Insert
      if (_updateStmt.getUpdateCount == 0) {
        _insertStmt.setString(1, sensorvalue.sensorId)
        _insertStmt.setLong(2, System.currentTimeMillis() / 1000) //value.sensorTStamp)
        _insertStmt.setFloat(3, sensorvalue.sensorTemp)
        _insertStmt.setInt(4, _iterator)
        //Execute
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
}
