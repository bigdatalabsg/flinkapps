package com.bigdatalabs.flinkapps.source

import com.bigdatalabs.flinkapps.entities.model.sensorReading
import org.apache.flink.api.common.RuntimeExecutionMode
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

        _env.enableCheckpointing(1000) // start a checkpoint every 10000 ms
        _env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000) //Pause between Check Points - milli seconds
        //_env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // set mode to exactly-once (this is the default)
        _env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        _env.getCheckpointConfig.setCheckpointTimeout(60000) // checkpoints have to complete within one minute, or are discarded
        _env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
        _env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) // allow only one checkpoint to be in progress at the same time
        _env.getConfig.setAutoWatermarkInterval(2000) // generate a Watermark every second

        _env.setParallelism(1)

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

            System.out.println("Start Processing at" + " " + System.currentTimeMillis())

            val _batch_file_path = _params.get("SRC_FILE_PATH")

            //Input Stream
            val _inputStream: DataStream[String] = _env.readTextFile(_batch_file_path)
            //_inputStream.print()

            //Read Each Line from File, Split at Comma and apply schema to Raw Data
            val _parsedStream: DataStream[sensorReading] = _inputStream.map(
                _readLine => {
                    val _arr_sensor_reading = _readLine.split(",")
                    sensorReading(
                        //Resolve to sensor_id, timestamp,tempreture
                        _arr_sensor_reading(0).trim, _arr_sensor_reading(1).toLong, _arr_sensor_reading(2).toFloat
                    )
                })

            /*
            //For Readability
            val _sensorReadings : DataStream[String] = _parsedStream.map(
                _sensorEntry => {
                    _sensorEntry.sensorId + "," + _sensorEntry.sensorTStamp + "," + _sensorEntry.sensorTemp
                }
            )//.filter("As Required")

            */

            //Sink Data to Database
            _parsedStream.addSink(new myBatchJDBCSink)
            _env.execute("Flink Batch JDBC Insert and Update")

            System.out.println("End Processing at" + " " + System.currentTimeMillis())
        }
    }

    //JDBC , Insert and Update Code Blocks
    class myBatchJDBCSink() extends RichSinkFunction[sensorReading] {

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

            Class.forName("org.postgresql.Driver")
            _connParams = DriverManager.getConnection(_connStr, _userName, _passwd)

            //Prepare Template Statements for INSERT and UPDATE
            _updateStmt = _connParams.prepareStatement("UPDATE streamingdb.t_flnk_temperature set sensor_ts=?,sensor_temp= sensor_temp + ?,iteration=iteration + ? WHERE sensor_id=?;")
            _insertStmt = _connParams.prepareStatement("INSERT INTO streamingdb.t_flnk_temperature(sensor_id, sensor_ts,sensor_temp,iteration) VALUES (?,?,?,?);")

        }

        //Insert or Update Data
        override def invoke(sensorvalue: sensorReading, context: SinkFunction.Context): Unit = {

            var _iterator: Int = 1

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