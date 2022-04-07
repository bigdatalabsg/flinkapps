package com.bigdatalabs.flinkapps.source

/*
 * @Author: Anand
 * @Date: 2022/04/03
 * @Description: Flink Kafka Source and Sink with New Kafka Source and Sink , Flink-1.14.4

 */


import com.bigdatalabs.flinkapps.entities.model.trade
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.Properties

//new Kafak Source API
import org.apache.flink.api.common.eventtime.WatermarkStrategy

import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.serialization.SimpleStringSchema

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
    val _open =_params.get("open")
    val _high =_params.get("high")
    val _low =_params.get("low")
    val _close =_params.get("close")

    //kafka broker properties
    val _kfkaprop = new Properties()
    //_kfkaprop.setProperty("zookeeper.connect","localhost:2181/kafka") //zookeeper
    _kfkaprop.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") //bootstrap
    _kfkaprop.setProperty("group.id", _groupId) // kafka group
    _kfkaprop.setProperty("auto.offset.reset", "latest")

    val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val _source = KafkaSource.builder[String]
      .setBootstrapServers(_brokers)
      .setTopics(_topic_source)
      .setGroupId(_groupId)
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setStartingOffsets(OffsetsInitializer.earliest)
      .build()

    val _InputStream = _env.fromSource(_source, WatermarkStrategy.noWatermarks(), "New Kafka Source not Kafka Consumer")

    //_InputStream.print()

    //Split Stream into Columns
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

    //Apply Schema from Entity Case Class
    val _trade = _parsedStream.map(record =>
      trade(record.xchange, record.symb, record.trdate,
        record.open, record.high, record.low, record.close,
        record.volume,
        record.adj_close))

    //Filter, Apply Intercepting Logic

    /*val _keyedStream = _trade
     .filter(x => x.symb == "ABB" || x.symb == "IBM")*/

    //Alter Trigger
    val _keyedStream = _trade
      .filter(x=>
        x.symb == _symb && (x.high >= _high.toFloat || x.low <= _low.toFloat)
      )

    /*val _keyedStream = _trade
      .filter(x => x.symb == "ABB" || x.symb == "IBM" &&
        x.high == _high || x.low== _low &&
        extractYr(convertStringToDate(x.trdate)) >= 2010 &&
        extractYr(convertStringToDate(x.trdate)) <= 2011
      )*/

    //Test for Filtered Data
    _keyedStream.print()

    val properties = new Properties()
    properties.setProperty("transaction.timeout.ms", "10000")

    val _sink = KafkaSink.builder()
      .setBootstrapServers(_brokers)
      .setKafkaProducerConfig(properties)
      .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setTransactionalIdPrefix("my-trx-id-prefix")
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(_topic_sink)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      ).build()

    _keyedStream.map(_.toString).sinkTo(_sink)

    _env.execute("new flink-Kafka-Source")

  }
}