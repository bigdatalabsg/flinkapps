package com.bigdatalabs.flinkapps.source

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.DataStream

//Output Formats
import java.io.{FileInputStream, FileNotFoundException, IOException}

//Model
import com.bigdatalabs.flinkapps.entities.model.sensorReading
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//new Kafak Source API new in 1.14.4
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

//import org.apache.flink.connector.jdbc._
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}

import java.sql.PreparedStatement

//Streaming to JDBC Sink
object flinkStreamingJDBCSink02 {

    def main(args: Array[String]): Unit = {

        //Vars for Property file
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

        //fetch Kafka Parameters
        val _topic_source: String = "loc-flnk-src" //_params.get("LOC_KFKA_SRC")
        val _topic_sink: String = "loc-flnk-snk" //_params.get("LOC_KFKA_SNK")
        val _groupId: String = "kfka-flnk" //_params.get("KFKA_CONS_GRP")
        val _brokers: String = _params.get("BOOTSTRAP_SERVERS")

        //Print Params
        println("Awaiting Stream . . .")
        print("=======================================================================\n")
        println(
            "[TOPIC SOURCE : " + _topic_source + ","
              + "TOPIC SINK: " + _topic_sink + ","
              + "GROUP: " + _groupId + "]"
        )

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
          .setStartingOffsets(OffsetsInitializer.latest()) //.setStartingOffsets(OffsetsInitializer.earliest())
          //.setStartingOffsets(OffsetsInitializer.timestamp(1651400400L))
          .build()

        //Receive from Kafka
        val _inputStream: DataStream[String] = _env.fromSource(_from_loc_kfka_src, WatermarkStrategy.noWatermarks(), "New Kafka Source from 1.14.4")
        _inputStream.print()

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
        ) //.filter(Y => Y.sensorTemp <= -10 || Y.sensorTemp >= 50)
        //.filter(Y => Y.sensorId == "sensor_1" && Y.sensorTemp <= -10 || Y.sensorTemp >= 50)

        //Publish Result, Filter, Aggregate
        //val _resultStream = _filteredStream.map(y=> y.sensorId + "," + y.sensorTStamp + "," + y.sensorTemp)
        //_resultStream.print()

        //Insert into RDBMS
        _filteredStream.addSink(
            JdbcSink.sink(
                "INSERT INTO flinkdb.t_flnk_tempreture(sensor_id, sensor_ts,sensor_temp) VALUES (?,?,?);", new JdbcStatementBuilder[sensorReading] {
                    override def accept(statement: PreparedStatement, sr: sensorReading): Unit = {
                        statement.setString(1, sr.sensorId)
                        statement.setLong(2, System.currentTimeMillis() / 1000) //statement.setLong(2, sr.sensorTStamp)
                        statement.setFloat(3, sr.sensorTemp)
                    }
                },
                JdbcExecutionOptions.builder()
                  .withBatchSize(1000)
                  .withBatchIntervalMs(200)
                  .withMaxRetries(5)
                  .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                  .withUrl("jdbc:postgresql://localhost:5432/dataopsdb")
                  .withDriverName("org.postgresql.Driver")
                  .withUsername("dopsuser")
                  .withPassword("dopspwd")
                  .build()
            )
        )

        _env.execute("flink streaming to mysql")

    }
}