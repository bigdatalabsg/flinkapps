package com.bigdatalabs.flinkapps.source

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

import java.sql.PreparedStatement

object flinkTableJDBC02 {

    def main(args: Array[String]): Unit = {

        val _env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment //Entry Point

        //===============================Begin Mandatory Block================================================================
        _env.enableCheckpointing(120000) // start a checkpoint every 10000 ms
        _env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000) //Pause between Check Points - milli seconds
        _env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // set mode to exactly-once (this is the default)
        _env.getCheckpointConfig.setCheckpointTimeout(60000) // checkpoints have to complete within one minute, or are discarded
        _env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
        _env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) // allow only one checkpoint to be in progress at the same time
        _env.getConfig.setAutoWatermarkInterval(2000) // generate a Watermark every second
        //_env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//Deprecated,Event Time, Ingestion Time, Processing Time
        //===============================End Mandatory Block================================================================

        _env.fromElements(
            new Book(101L, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
            new Book(102L, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
            new Book(103L, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
            new Book(104L, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
        ).addSink(
            JdbcSink.sink(
                "insert into flinkdb.t_flnk_book (bookid, booktitle, authors, bookyear) values (?, ?, ?, ?)", new JdbcStatementBuilder[Book] {
                    override def accept(statement: PreparedStatement, book: Book): Unit = {
                        statement.setLong(1, book.bookId)
                        statement.setString(2, book.bookTitle)
                        statement.setString(3, book.Authors)
                        statement.setInt(4, book.bookYear)
                    }
                },
                JdbcExecutionOptions.builder()
                  .withBatchSize(1000)
                  .withBatchIntervalMs(200)
                  .withMaxRetries(5)
                  .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                  .withUrl("jdbc:postgresql://localhost:5432/dataopsdb")
                  .withDriverName("com.postgresql.Driver")
                  .withUsername("dopsuser")
                  .withPassword("dopspwd")
                  .build()
            ))

        _env.execute("Flink JDBC")

    }

    //
    class Book(
                val bookId: Long,
                val bookTitle: String,
                val Authors: String,
                val bookYear: Integer
              )
}