package com.bigdatalabs.flinkapps.source

/*
 * @Author: Anand
 * @Date: 2022/04/03
 * @Description: Flink Kafka Source and Sink with New Kafka Source and Sink , Flink-1.14.4

 */


import java.util.{Properties, UUID}
import scala.util.Random

//Model
import com.bigdatalabs.flinkapps.entities.model.trade
import com.bigdatalabs.flinkapps.common.dateFormatter.{convertStringToDate, extractYr}

import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.CheckpointingMode

//new Kafak Source API new in 1.14.4
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.serialization.SimpleStringSchema

import org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.eventtime.WatermarkStrategy

import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema

object flinkContinuousProcessing {

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
    print("=======================================================================\n")
    println(
      "TOPIC SOURCE : " + _topic_source +","
        + "TOPIC SINK: " + _topic_sink + "|"
        + "GROUP: " + _groupId + ","
        + "SYMB: " + _symb + ","
        + "HIGH: " + _high
        + "," + "LOW: " + _low
    )


    val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // start a checkpoint every 10000 ms
    _env.enableCheckpointing(120000)
    //Pause between Check Points - milli seconds
    // make sure 500 ms of progress happen between checkpoints
    _env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000)
    // set mode to exactly-once (this is the default)
    _env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // checkpoints have to complete within one minute, or are discarded
    _env.getCheckpointConfig.setCheckpointTimeout(60000)
    // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
    _env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    // allow only one checkpoint to be in progress at the same time
    _env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // generate a Watermark every second
    _env.getConfig.setAutoWatermarkInterval(10000)

    //Event Time, Ingestion Time, Processing Time
    //_env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) - Deprecated

    val _topicSource = KafkaSource.builder[String]
      .setBootstrapServers(_brokers)
      .setTopics(_topic_source)
      .setGroupId(_groupId)
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setStartingOffsets(OffsetsInitializer.latest())
      .build()


       val _InputStream = _env.fromSource(_topicSource, WatermarkStrategy.noWatermarks(), "New Kafka Source not Kafka Consumer")
      //_InputStream.print()

      //Split Stream into Columns separated by comma
      val _parsedStream = _InputStream.map(
        lines => {
          val columns = lines.split(",")
          trade(
            columns(0), columns(1), columns(2),
            columns(3).toFloat,
            columns(4).toFloat,
            columns(5).toFloat,
            columns(6).toFloat,
            columns(7).toInt,
            columns(8).toFloat
          )
        })

      //Apply model from Entity Case Class
      val _trade = _parsedStream.map(record =>
        trade(record.xchange, record.symb, record.trdate,
          record.open, record.high, record.low, record.close,
          record.volume,
          record.adj_close))

    //Filter, Apply Intercepting Logic
    /*
    val _filteredStream = _trade
     .filter(x => x.symb == "ABB" || x.symb == "IBM")
    //val _test = _trade.map(y=> y.xchange + "," + y.symb + "," + y.trdate + "," + y.open + "," + y.high + "," + y.low + "," + y.close + "," + y.volume + "," + y.adj_close)
    */

      //Alter Filters amd Trigger
      val _filteredStream = _trade
        .filter(x =>
          x.symb == _symb && (x.high >= _high.toFloat || x.low <= _low.toFloat)
        ).map(y => (System.currentTimeMillis() + Random.nextInt()) + "," + y.xchange + ","
        + y.symb + "," + y.trdate + "," + y.open + ","
        + y.high + "," + y.low + "," + y.close + ","
        + y.volume + "," + y.adj_close + "," + (y.close - y.open))

    /*
    val _filteredStream = _trade
      .filter(x => x.symb == "ABB" || x.symb == "IBM" &&
        x.high == _high || x.low== _low &&
        extractYr(convertStringToDate(x.trdate)) >= 2010 &&
        extractYr(convertStringToDate(x.trdate)) <= 2011
      ).map(y=> y.xchange + ","
      + y.symb + "," + y.trdate + "," + y.open + ","
      + y.high + "," + y.low + "," + y.close + ","
      + y.volume + "," + y.adj_close + "," + (y.close-y.open))
    */

    //Test for Filtered Data
    _filteredStream.print()

    //publish to the Producer - Result Topic
    val _producerProp = new Properties()
    _producerProp.setProperty("transaction.timeout.ms", "10000")

    // added for idempotency
    _producerProp.setProperty("isolation.level","read_committed")
    _producerProp.setProperty("enable.auto.commit", "false")
    _producerProp.setProperty("enable.idempotence","true")
    //_producerProp.setProperty("transaction.id",UUID.randomUUID().toString)

       val _topicSink = KafkaSink.builder()
      .setBootstrapServers(_brokers)
      .setKafkaProducerConfig(_producerProp)
      .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE) //EXACTLY_ONCE, has issues with producer
      .setTransactionalIdPrefix(_groupId + "-" + _topicSource)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(_topic_sink)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      ).build()

    //Publish to Kafka Producrer
    _filteredStream.sinkTo(_topicSink)

    _env.execute("new flink-Kafka-Source 1.14.4")
  }
}