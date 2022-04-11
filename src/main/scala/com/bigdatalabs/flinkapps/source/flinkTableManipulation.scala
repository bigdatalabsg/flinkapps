package com.bigdatalabs.flinkapps.source

import org.apache.flink.api.scala._
import com.bigdatalabs.flinkapps.entities.model.trade
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object flinkTableManipulation {

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
    // set the batch runtime mode
    _env.setRuntimeMode(RuntimeExecutionMode.STREAMING)


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


    val _tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(_env)
    val _inputTable = _tableEnv.fromDataStream(_trade)//.as("xchange","symbol","trdate","oopen","high","low","close","volume","adj-close")

    //_inputTable.printSchema()

    _tableEnv.createTemporaryView("flinkappDB.t_flnk_daily_prices",_inputTable)
    //_tableEnv.createTable("flinkappDB.t_flnk_daily_prices",_inputTable)
    //val _resultTable = _tableEnv.sqlQuery("select * from t_flnk_daily_prices")
    val _resultTable = _tableEnv.sqlQuery("SELECT symb, YEAR(CAST(trdate AS DATE)) AS yearr, min(high) as MIN_HIGH ,max(high) AS MAX_HIGH FROM flinkappDB.t_flnk_daily_prices GROUP BY symb, YEAR(CAST(trdate AS DATE))")
    //val _resultTable = _tableEnv.sqlQuery("SELECT * FROM t_flnk_daily_prices")

    //_resultTable.printSchema()

    val _resultStream = _tableEnv.toChangelogStream(_resultTable)
    //val _resultStream = _tableEnv.toDataStream(_resultTable)

    _resultStream.print()

    _env.execute("Table API")

  }

}










/*

package com.bigdatalabs.flinkapps.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, _}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.apache.flink.util.CloseableIterator

import java.time.LocalDate
import scala.collection.JavaConverters._

/**
 * Example for getting started with the Table & SQL API in Scala.
 *
 * The example shows how to create, transform, and query a table. It should give a first impression
 * about the look-and-feel of the API without going too much into details. See the other examples for
 * using connectors or more complex operations.
 *
 * In particular, the example shows how to
 *   - setup a [[TableEnvironment]],
 *   - use the environment for creating example tables, registering views, and executing SQL queries,
 *   - transform tables with filters and projections,
 *   - declare user-defined functions,
 *   - and print/collect results locally.
 *
 * The example executes two Flink jobs. The results are written to stdout.
 */
object flinkTableManipulation {

  def main(args: Array[String]): Unit = {

   // val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // setup the unified API
    // in this case: declare that the table programs should be executed in batch mode
    val settings = EnvironmentSettings.newInstance()
      .inBatchMode()
      .build()

    val _env = TableEnvironment.create(settings)

    //val _env = TableEnvironment.create(settings)

    // create a table with example data without a connector required
    val rawCustomers = _env.fromValues(
      row("Guillermo Smith", LocalDate.parse("1992-12-12"), "4081 Valley Road", "08540", "New Jersey", "m", true, 0, 78, 3),
      row("Valeria Mendoza", LocalDate.parse("1970-03-28"), "1239  Rainbow Road", "90017", "Los Angeles", "f", true, 9, 39, 0),
      row("Leann Holloway", LocalDate.parse("1989-05-21"), "2359 New Street", "97401", "Eugene", null, true, null, null, null),
      row("Brandy Sanders", LocalDate.parse("1956-05-26"), "4891 Walkers-Ridge-Way", "73119", "Oklahoma City", "m", false, 9, 39, 0),
      row("John Turner", LocalDate.parse("1982-10-02"), "2359 New Street", "60605", "Chicago", "m", true, 12, 39, 0),
      row("Ellen Ortega", LocalDate.parse("1985-06-18"), "2448 Rodney Street", "85023", "Phoenix", "f", true, 0, 78, 3)
    )



    // handle ranges of columns easily
    val truncatedCustomers = rawCustomers.select(withColumns(1 to 7))

    // name columns
    val namedCustomers = truncatedCustomers
      .as("name", "date_of_birth", "street", "zip_code", "city", "gender", "has_newsletter")

    // register a view temporarily
    _env.createTemporaryView("customers", namedCustomers)

    // use SQL whenever you like
    // call execute() and print() to get insights
    _env
      .sqlQuery("""
                  |SELECT
                  |  COUNT(*) AS `number of customers`,
                  |  AVG(YEAR(date_of_birth)) AS `average birth year`
                  |FROM `customers`
                  |""".stripMargin
      )
      .execute()
      .print()

    // or further transform the data using the fluent Table API
    // e.g. filter, project fields, or call a user-defined function
    val youngCustomers = _env
      .from("customers")
      .filter($"gender".isNotNull)
      .filter($"has_newsletter" === true)
      .filter($"date_of_birth" >= LocalDate.parse("1980-01-01"))
      .select(
        $"name".upperCase(),
        $"date_of_birth",
        call(classOf[AddressNormalizer], $"street", $"zip_code", $"city").as("address")
      )

    // use execute() and collect() to retrieve your results from the cluster
    // this can be useful for testing before storing it in an external system
    var iterator: CloseableIterator[Row] = null
    try {
      iterator = youngCustomers.execute().collect()
      val actualOutput = iterator.asScala.toSet

      val expectedOutput = Set(
        Row.of("GUILLERMO SMITH", LocalDate.parse("1992-12-12"), "4081 VALLEY ROAD, 08540, NEW JERSEY"),
        Row.of("JOHN TURNER", LocalDate.parse("1982-10-02"), "2359 NEW STREET, 60605, CHICAGO"),
        Row.of("ELLEN ORTEGA", LocalDate.parse("1985-06-18"), "2448 RODNEY STREET, 85023, PHOENIX")
      )

      if (actualOutput == expectedOutput) {
        println("SUCCESS!")
      } else {
        println("FAILURE!")
      }
    } finally {
      if (iterator != null) {
        iterator.close()
      }
    }
  }

  /**
   * We can put frequently used procedures in user-defined functions.
   *
   * It is possible to call third-party libraries here as well.
   */
  class AddressNormalizer extends ScalarFunction {

    // the 'eval()' method defines input and output types (reflectively extracted)
    // and contains the runtime logic
    def eval(street: String, zipCode: String, city: String): String = {
      normalize(street) + ", " + normalize(zipCode) + ", " + normalize(city)
    }

    private def normalize(s: String) = {
      s.toUpperCase.replaceAll("\\W", " ").replaceAll("\\s+", " ").trim
    }
  }
}
 */

