package com.bigdatalabs.flinkapps.source

import com.bigdatalabs.flinkapps.entities.model.SensorReading

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.JdbcSink
import org.apache.flink.table.planner.expressions.CurrentTime

import java.sql.{Connection, DriverManager, PreparedStatement}

object flinkJDBCSink {

  def main(args: Array[String]): Unit = {

    val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment //Entry Point
    _env.setParallelism(1)

    _env.enableCheckpointing(1000) // start a checkpoint every 10000 ms
    //_env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)//Pause between Check Points - milli seconds
    //_env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)// set mode to exactly-once (this is the default)
    //_env.getCheckpointConfig.setCheckpointTimeout(60000)// checkpoints have to complete within one minute, or are discarded
    //_env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)// prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
    //_env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)// allow only one checkpoint to be in progress at the same time
    //_env.getConfig.setAutoWatermarkInterval(2000)// generate a Watermark every second

    val _inputStream = _env.readTextFile("file:///home/bdluser/Dataops/dataSources/sensor_data.csv")

    //_inputStream.print()

    //Source from Text File
    val _dataStream = _inputStream.map(
      _rowData => {
          val _dataArray = _rowData.split(",")
        SensorReading(_dataArray(0).trim,_dataArray(1).toLong,_dataArray(2).toFloat)
      }
    )
    //Sink Data to Database
    _dataStream.addSink(new myJDBCSink())

    _env.execute("flink to mysql")

  }

}

class myJDBCSink() extends RichSinkFunction[SensorReading]{

  var _connParams: Connection = _
  var _insertStmt: PreparedStatement = _
  var _updateStmt: PreparedStatement = _

  //Open Conn
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    var _connStr = "jdbc:mysql://localhost:3306/flinkops"
    var _userName = "root"
    var _passwd = "sqlpwd"

    _connParams = DriverManager.getConnection(_connStr, _userName, _passwd)
    _insertStmt = _connParams.prepareStatement("INSERT INTO flinkops.t_flnk_tempreture(sensor_id, sensor_ts,sensor_temp) VALUES (?,?,?);")
    _updateStmt = _connParams.prepareStatement("UPDATE flinkops.t_flnk_tempreture set sensor_ts=?,sensor_temp=? WHERE sensor_id=?;")
  }

  //Insert or Update Data
  override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = {

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