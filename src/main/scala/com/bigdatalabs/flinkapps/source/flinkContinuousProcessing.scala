package com.bigdatalabs.flinkapps.source

/*
 * @Author: Anand
 * @Date: 2022/04/03
 * @Description: Flink Kafka Source and Sink with New Kafka Source and Sink , Flink-1.14.4

 */


import org.apache.flink.api.common.state.StateTtlConfig.TtlTimeCharacteristic
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.runtime.fs.hdfs.HadoopDataOutputStream

import java.io.{FileInputStream, FileNotFoundException, IOException}
import java.util.{Properties, UUID}
import scala.util.Random

//Model
import com.bigdatalabs.flinkapps.entities.model.{_ctrade}
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

		//Vars for Property file
		var _propFile: FileInputStream = null
		var _params: ParameterTool = null

		//Check for Propoerties File
		try {
			_propFile= new FileInputStream("src/main/resources/flinkApps.properties")
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

		//Print Params
		println("Awaiting Stream . . .")
		print("=======================================================================\n")
		println(
			"[TOPIC SOURCE : " + _topic_source +","
				+ "TOPIC SINK: " + _topic_sink + ","
				+ "GROUP: " + _groupId + "]" + "|" + "["
				+ "SYMB: " + _symb + ","
				+ "OPEN: " + _open + ","
				+ "HIGH: " + _high + ","
				+ "LOW: " + _low + ","
				+ "CLOSE: " + _close + "]"
		)

		val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment //Entry Point
		_env.getConfig.setGlobalJobParameters(_params)
		_env.enableCheckpointing(120000) // start a checkpoint every 10000 ms
		_env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000)//Pause between Check Points - milli seconds
		_env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)// set mode to exactly-once (this is the default)
		_env.getCheckpointConfig.setCheckpointTimeout(60000)// checkpoints have to complete within one minute, or are discarded
		_env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)// prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
		_env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)// allow only one checkpoint to be in progress at the same time
		_env.getConfig.setAutoWatermarkInterval(2000)// generate a Watermark every second
		//_env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//Deprecated,Event Time, Ingestion Time, Processing Time

		//Kafka Params
		val _fromSource = KafkaSource.builder[String]
			.setBootstrapServers(_brokers)
			.setTopics(_topic_source)
			.setGroupId(_groupId)
			.setValueOnlyDeserializer(new SimpleStringSchema())
			.setStartingOffsets(OffsetsInitializer.latest())
			.build()

		//Receive from Kafka
		val _InputStream = _env.fromSource(_fromSource, WatermarkStrategy.noWatermarks(), "New Kafka Source from 1.14.4")
		//_InputStream.print()

		//Read Each Line from Kafka Stream, Split at Comma
		val _parsedStream = _InputStream.map(
			_readLine => {
				val _arr_daily_prices = _readLine.split(",")
				_ctrade(
					_arr_daily_prices(0), _arr_daily_prices(1), _arr_daily_prices(2),
					_arr_daily_prices(3).toFloat,_arr_daily_prices(4).toFloat,_arr_daily_prices(5).toFloat,_arr_daily_prices(6).toFloat,
					_arr_daily_prices(7).toInt,_arr_daily_prices(8).toFloat
				)
			})

		/*.map(record =>
			_ctrade(
				record.xchange,record.symbol,record.trdate,
				record.open,record.high,record.low,record.close,
				record.volume,record.adj_close))*/

		//Apply Schema from Entity Case Class
		val _trade= _parsedStream.map(record =>
			_ctrade(
				record.xchange,record.symbol,record.trdate,
				record.open,record.high,record.low,record.close,
				record.volume,record.adj_close))

		//Filter, Apply Intercepting Logic
		/*
		val _filteredStream = _trade
		 .filter(x => x.symb == "ABB" || x.symb == "IBM")
		//val _test = _trade.map(y=> y.xchange + "," + y.symb + "," + y.trdate + "," + y.open + "," + y.high + "," + y.low + "," + y.close + "," + y.volume + "," + y.adj_close)
		*/

		//Alter Filters amd Trigger
		val _filteredStream = _trade
			.filter(x =>
				x.symbol == _symb && (x.high >= _high.toFloat || x.low <= _low.toFloat)
			).map(y => System.currentTimeMillis() + "," + _topic_source + "," + y.xchange + ","
			+ y.symbol + "," + y.trdate + "," + y.open + ","
			+ y.high + "," + y.low + "," + y.close + ","
			+ y.volume + "," + y.adj_close + "," + (y.close - y.open))
			//Convert to Object
		/*	.map(record =>
				_ctrade(
					record.xchange,record.symbol,record.trdate,
					record.open,record.high,record.low,record.close,
					record.volume,record.adj_close))
		)*/

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

		// added for idempotency
		_producerProp.setProperty("transaction.timeout.ms", "10000")
		_producerProp.setProperty("isolation.level","read_committed")
		_producerProp.setProperty("enable.auto.commit", "false")
		_producerProp.setProperty("enable.idempotence","true")
		_producerProp.setProperty("transaction.id",UUID.randomUUID().toString)

		val _toSink = KafkaSink.builder()
			.setBootstrapServers(_brokers)
			.setKafkaProducerConfig(_producerProp)
			.setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE) //EXACTLY_ONCE, has issues with producer
			.setTransactionalIdPrefix(_groupId + "-" + _fromSource)
			.setRecordSerializer(KafkaRecordSerializationSchema.builder()
				.setTopic(_topic_sink)
				.setValueSerializationSchema(new SimpleStringSchema())
				.build()
			).build()

		//Publish to Kafka Producrer
		_filteredStream.sinkTo(_toSink)
		_filteredStream.writeAsText("/tmp/setPara", WriteMode.OVERWRITE).setParallelism(1)

		_env.execute("new flink-Kafka-Source 1.14.4")

	}
}