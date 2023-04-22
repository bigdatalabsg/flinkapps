package com.bigdatalabs.flinkapps.source

import com.bigdatalabs.flinkapps.entities.model.sensorReading
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

import java.io.{FileInputStream, FileNotFoundException, IOException}
import java.sql.PreparedStatement

object flinkTableJDBC {
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

        _parsedStream.addSink(
            JdbcSink.sink(
                "INSERT INTO flinkdb.t_flnk_tempreture(sensor_id, sensor_ts,sensor_temp) VALUES (?,?,?);", new JdbcStatementBuilder[sensorReading] {
                    override def accept(statement: PreparedStatement, sr: sensorReading): Unit = {
                        statement.setString(1, sr.sensorId)
                        statement.setLong(2, sr.sensorTStamp)
                        statement.setFloat(3, sr.sensorTemp)
                    }
                },
                JdbcExecutionOptions.builder()
                  .withBatchSize(1000)
                  .withBatchIntervalMs(200)
                  .withMaxRetries(5)
                  .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                  .withUrl("jdbc:postgresql://localhost:5432/dataopsdb?currentSchema=flinkdb")
                  .withDriverName("com.postgresql.Driver")
                  .withUsername("dopsuser")
                  .withPassword("dopspwd")
                  .build()
            ))

        _env.execute("Flink Batch JDBC")

    }
}