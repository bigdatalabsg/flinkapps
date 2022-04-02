package com.bigdatalabs.flinkapps.entities

object model {

  case class trade(
                    xchange: String,
                    symb: String,
                    trdate: String,
                    open: Float,
                    high: Float,
                    low: Float,
                    close: Float,
                    volume: Integer,
                    adj_close: Float)

  case class atmlog (tran_id: String,
                     tran_dt: String,
                     area: String,
                     latitude: Float,
                     longitude:Float,
                     tran_typ:String,
                     tran_amt: Float)

  }

/*
private class ProducerStringSerializationSchema(var topic: String) extends KafkaSerializationSchema[trade] {
  override def serialize(element: trade, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]]
}
*/
/*
private[flink] class FlinkKafkaCodecSerializationSchema[T: TypeInformation](outlet: CodecOutlet[T], topic: String)
  extends KafkaSerializationSchema[T] {
  override def serialize(value: T, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] =
    outlet.partitioner match {
      case RoundRobinPartitioner => // round robin - no key
        new ProducerRecord(topic, outlet.codec.encode(value))
      case _ => // use the key
        new ProducerRecord(topic, outlet.partitioner(value).getBytes(), outlet.codec.encode(value))
    }
}

*/