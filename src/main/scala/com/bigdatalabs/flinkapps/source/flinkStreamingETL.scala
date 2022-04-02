package com.bigdatalabs.flinkapps.source

object flinkStreamingETL {
  def main(args: Array[String]): Unit = {
 if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: --topic_source <source topic name> --topic_sink <sink topic name> --groupId <group name> --high <number> --low <number>
        """.stripMargin)
      System.exit(1)
    }
  }
}
