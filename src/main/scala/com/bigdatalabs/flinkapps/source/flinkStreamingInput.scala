package com.bigdatalabs.flinkapps.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.datastream.{DataStream, KeyedStream}
import org.apache.flink.streaming.api.CheckpointingMode

//Entity
//import com.anrisu.flinkapps.entities.model.{trade,atmlog}
import com.bigdatalabs.flinkapps.entities.model.trade
//Common

object flinkStreamingInput {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: --topic_source <source topic name> --topic_sink <sink topic name> --groupId <group name> --high <number> --low <number>
        """.stripMargin)
      System.exit(1)
    }

    //fetch Inputs
    val _params = ParameterTool.fromArgs(args)
    val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val _topic_source = _params.get("topic_source")
    val _topic_sink = _params.get("topic_sink")
    val _groupId = _params.get("groupId")

    //Thresholds
    val _open =_params.get("open")
    val _high =_params.get("high")
    val _low =_params.get("low")
    val _close =_params.get("close")

    println("Awaiting Stream . . .")
    print("=======================================================================\n")

    println("TOPIC SOURCE : " + _topic_source +"," + "TOPIC SINK : " + _topic_sink + "," + "GROUP : "+ _groupId + "," + "HIGH :" + _high + "," + "LOW :" + _low)

    // start a checkpoint every 10000 ms
    _env.enableCheckpointing(10000)

    //Pause between Check Points - milli seconds
    // make sure 500 ms of progress happen between checkpoints
    _env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    // set mode to exactly-once (this is the default)
    _env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // checkpoints have to complete within one minute, or are discarded
    _env.getCheckpointConfig.setCheckpointTimeout(60000)

    // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
    _env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    // allow only one checkpoint to be in progress at the same time
    _env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // generate a Watermark every second
    _env.getConfig.setAutoWatermarkInterval(5000)

    //kafka broker properties
    val _kfkaprop = new Properties()
    //_kfkaprop.setProperty("zookeeper.connect","localhost:2181/kafka") //zookeeper
    _kfkaprop.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") //bootstrap
    _kfkaprop.setProperty("group.id", _groupId) // kafka group
    _kfkaprop.setProperty("auto.offset.reset", "latest")

    // create a Kafka consumer
    val _kfkaconsumer = new FlinkKafkaConsumer[String](_topic_source, new SimpleStringSchema(), _kfkaprop)

    //Add a DataStream, from Consumer
    val _stream = _env.addSource(_kfkaconsumer)

    // Test for stream
    //_stream.print()

    //Split Stream into Columns
    val _parsedStream = _stream.map(
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

    val _keyedStream = _trade
      .filter(x=>
        x.symb == "IBM" &&
          (x.high >= _high.toFloat || x.low <= _low.toFloat)
      )

    _keyedStream.print()

    /*val _keyedStream = _trade
      .filter(x => x.symb == "ABB" || x.symb == "IBM")*/

    /*val _keyedStream = _trade
      .filter(x => x.symb == "ABB" || x.symb == "IBM" &&
        x.high == _high || x.low== _low &&
        extractYr(convertStringToDate(x.trdate)) >= 2010 &&
        extractYr(convertStringToDate(x.trdate)) <= 2011
      )*/

    //Test for Filtered Data
    _keyedStream.print()

    /*
    //Define a Producer
    val _kfkaproducer = new FlinkKafkaProducer[String](_topic_sink, new SimpleStringSchema(), _kfkaprop)

    //This is a workaround, until a Custom Serializer can be Developed, after which there is no need to convert to String
    //Publish to Kafka Topic for Filtered Data
    _keyedStream.map(_.toString).addSink(_kfkaproducer)

    */

    //Execute
    _env.execute("flink-Kafka-Input-Output")

    print("=======================================================================\n")
  }
}