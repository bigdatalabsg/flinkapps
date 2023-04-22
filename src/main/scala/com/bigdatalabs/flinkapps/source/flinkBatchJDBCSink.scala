package com.bigdatalabs.flinkapps.source

import com.bigdatalabs.flinkapps.entities.model.sensorReading
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.io.{FileInputStream, FileNotFoundException, IOException}
import java.sql.{Connection, DriverManager, PreparedStatement}

object flinkBatchJDBCSink {
    def main(args: Array[String]): Unit = {

        val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment //Entry Point
        _env.setParallelism(1)

        _env.enableCheckpointing(1000) // start a checkpoint every 10000 ms
        _env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000) //Pause between Check Points - milli seconds
        _env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // set mode to exactly-once (this is the default)
        _env.getCheckpointConfig.setCheckpointTimeout(60000) // checkpoints have to complete within one minute, or are discarded
        _env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
        _env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) // allow only one checkpoint to be in progress at the same time
        _env.getConfig.setAutoWatermarkInterval(2000) // generate a Watermark every second

        var _propFile: FileInputStream = null
        var _params: ParameterTool = null

        //Check for Propoerties File
        try {
            _propFile = new FileInputStream("src/main/resources/flinkApps.properties")
            _params = ParameterTool.fromPropertiesFile(_propFile)
        } catch {
            case e: FileNotFoundException => println("Couldn't find that file.")
            case e: IOException => println("Had an IOException trying to read that file")
        } finally {

        }

        val _batch_file_path = _params.get("SRC_FILE_PATH")

        //Input Stream
        val _inputStream: DataStream[String] = _env.readTextFile(_batch_file_path)
        _inputStream.print()

        //Read Each Line from File, Split at Comma and apply schema
        val _parsedStream: DataStream[sensorReading] = _inputStream.map(
            _readLine => {
                val _arr_sensor_reading = _readLine.split(",")
                sensorReading(
                    //xchange, symbol, trdate, open, high, low, close, volumen, adjusted close
                    _arr_sensor_reading(0).trim, _arr_sensor_reading(1).toLong, _arr_sensor_reading(2).toFloat
                )
            })

        //Sink Data to Database
        _parsedStream.addSink(new myBatchJDBCSink)

        _env.execute("Flink Batch JDBC Insert and Update")

    }

    //JDBC , Insert and Update Code Blocks

    private class myBatchJDBCSink() extends RichSinkFunction[sensorReading] {

        var _connParams: Connection = _
        var _insertStmt: PreparedStatement = _
        var _updateStmt: PreparedStatement = _

        //Open Conn
        override def open(parameters: Configuration): Unit = {
            super.open(parameters)

            val _driverName = "org.postgresql.Driver"
            val _connStr = "jdbc:postgresql://localhost:5432/dataopsdb"
            val _userName = "dopsuser"
            val _passwd = "dopspwd"

            _connParams = DriverManager.getConnection(_connStr, _userName, _passwd)

            //Prepare Statements
            _updateStmt = _connParams.prepareStatement("UPDATE flinkdb.t_flnk_tempreture set sensor_ts=?,sensor_temp=? WHERE sensor_id=?;")
            _insertStmt = _connParams.prepareStatement("INSERT INTO flinkdb.t_flnk_tempreture(sensor_id, sensor_ts,sensor_temp) VALUES (?,?,?);")

        }

        //Insert or Update Data
        override def invoke(value: sensorReading, context: SinkFunction.Context): Unit = {

            //default Update
            _updateStmt.setLong(1, System.currentTimeMillis() / 1000) //value.sensorTStamp)
            _updateStmt.setFloat(2, value.sensorTemp)
            _updateStmt.setString(3, value.sensorId)

            //Execute
            _updateStmt.execute()

            //Insert
            if (_updateStmt.getUpdateCount == 0) {
                _insertStmt.setString(1, value.sensorId)
                _insertStmt.setLong(2, System.currentTimeMillis() / 1000) //value.sensorTStamp)
                _insertStmt.setFloat(3, value.sensorTemp)

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