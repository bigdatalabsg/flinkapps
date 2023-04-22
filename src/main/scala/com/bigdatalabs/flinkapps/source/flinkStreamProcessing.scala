package com.bigdatalabs.flinkapps.source

/*
 * @Author: Anand
 * @Date: 2022/04/03
 * @Description: Flink Kafka Source and Sink with New Kafka Source and Sink , Flink-1.15.0
 */

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.DataStream

//Output Formats
import java.io.{FileInputStream, FileNotFoundException, IOException}
import java.util.{Properties, UUID}

//Model
import com.bigdatalabs.flinkapps.common.dateFormatter.{convertStringToDate, extractYr}
import com.bigdatalabs.flinkapps.entities.model.dailyPrices
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//new Kafak Source API new in 1.14.4
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

object flinkStreamProcessing {

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
        val _topic_source: String = _params.get("LOC_KFKA_SRC")
        val _topic_sink: String = _params.get("LOC_KFKA_SNK")
        val _groupId: String = _params.get("KFKA_CONS_GRP")
        val _brokers: String = _params.get("BOOTSTRAP_SERVERS")

        //Alert Thresholds
        val _symb: String = _params.get("SYMB")
        val _open: Float = _params.getFloat("OPEN")
        val _high: Float = _params.getFloat("HIGH")
        val _low: Float = _params.getFloat("LOW")
        val _close: Float = _params.getFloat("CLOSE")

        val _loc_file_snk_path = _params.get("LOC_FILE_SINK_PATH")

        //Print Params
        println("Awaiting Stream . . .")
        print("=======================================================================\n")
        println(
            "[TOPIC SOURCE : " + _topic_source + ","
              + "TOPIC SINK: " + _topic_sink + ","
              + "GROUP: " + _groupId + "]" + "|" + "["
              + "SYMB: " + _symb + ","
              + "OPEN: " + _open + ","
              + "HIGH: " + _high + ","
              + "LOW: " + _low + ","
              + "CLOSE: " + _close + "]"
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
        val _InputStream: DataStream[String] = _env.fromSource(_from_loc_kfka_src, WatermarkStrategy.noWatermarks(), "New Kafka Source from 1.15.0")
        _InputStream.print()

        //Read Each Line from Kafka Stream, Split at Comma and apply schema
        val _parsedStream: DataStream[dailyPrices] = _InputStream.map(
            _readLine => {
                val _arr_daily_prices = _readLine.split(",")
                dailyPrices(
                    //xchange, symbol, trdate, open, high, low, close, volumen, adjusted close
                    _arr_daily_prices(0), _arr_daily_prices(1), _arr_daily_prices(2),
                    _arr_daily_prices(3).toFloat, _arr_daily_prices(4).toFloat, _arr_daily_prices(5).toFloat, _arr_daily_prices(6).toFloat,
                    _arr_daily_prices(7).toInt, _arr_daily_prices(8).toFloat
                )
            })

        val _trade: DataStream[dailyPrices] = _parsedStream
          .map(_parsedRecord =>
              dailyPrices(
                  _parsedRecord.xchange, _parsedRecord.symbol, _parsedRecord.trdate,
                  _parsedRecord.open, _parsedRecord.high, _parsedRecord.low, _parsedRecord.close,
                  _parsedRecord.volume, _parsedRecord.adj_close))

        //Filter, Apply Intercepting Logic
        val _filteredStream = _trade.filter(x => x.symbol == "ABB" || x.symbol == "IBM" || x.symbol == "CAT")

        //Test for Filtered Data
        //_filteredStream.print()

        //val _test = _trade.map(y=> y.xchange + "," + y.symbol + "," + y.trdate + "," + y.open + "," + y.high + "," + y.low + "," + y.close + "," + y.volume + "," + y.adj_close)

        //Apply Filters amd Trigger/Logic/Aggregation
        val _filteredStream01: DataStream[String] = _trade
          .filter(x => x.symbol == "ABB" || x.symbol == "IBM" || x.symbol == "CAT")
          /*.filter(x => x.symbol == _symb && (x.high >= _high.toFloat || x.low <= _low.toFloat))*/
          .map(y => System.currentTimeMillis() / 1000 + "," + _topic_source + ","
            + y.xchange + "," + y.symbol + "," + y.trdate + ","
            + y.open + "," + y.high + "," + y.low + "," + y.close + ","
            + y.volume + "," + y.adj_close + "," + (y.close - y.open)
          )
        //Test for Filtered Data
        //_filteredStream01.print()

        val _filteredStream02 =
            _parsedStream.filter(x =>
                x.symbol == "ABB" || x.symbol == "IBM" || x.symbol == "CAT" &&
                  x.high == _high || x.low == _low &&
                  extractYr(convertStringToDate(x.trdate)) >= 2010 && extractYr(convertStringToDate(x.trdate)) <= 2011
            ).map(y => System.currentTimeMillis() / 1000 + "," + _topic_source + ","
              + y.xchange + "," + y.symbol + "," + y.trdate + ","
              + y.open + "," + y.high + "," + y.low + "," + y.close + ","
              + y.volume + "," + y.adj_close + "," + (y.close - y.open)
            )

        //Test for Filtered Data
        //_filteredStream02.print()

        //publish to the Producer - Result Topic
        val _producerProp = new Properties()

        // added for idempotency
        _producerProp.setProperty("transaction.timeout.ms", "10000")
        _producerProp.setProperty("isolation.level", "read_committed")
        _producerProp.setProperty("enable.auto.commit", "false")
        _producerProp.setProperty("enable.idempotence", "true")
        _producerProp.setProperty("transaction.id", UUID.randomUUID().toString)

        //Set Consumer propperties
        val _to_loc_kfka_snk = KafkaSink.builder()
          .setBootstrapServers(_brokers)
          .setKafkaProducerConfig(_producerProp)
          .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE) //EXACTLY_ONCE, has issues with producer
          .setTransactionalIdPrefix(_groupId + "-" + _from_loc_kfka_src)
          .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(_topic_sink)
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
          ).build()

        //Publish to Kafka Producrer
        _filteredStream01.sinkTo(_to_loc_kfka_snk)

        //Sink Data to File
        //_filteredStream01.writeAsText(_loc_file_snk_path + "/" + "flinkoutput.txt", WriteMode.OVERWRITE).setParallelism(1)
        _env.execute("new flink-Kafka-Source 1.15.0")

    }
}