package spark.streaming.potato.quickstart

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spark.streaming.potato.plugins.kafka.source._
import spark.streaming.potato.plugins.kafka.sink._
import spark.streaming.potato.template.KafkaSourceTemplate

object Kafka2KafkaDemo extends KafkaSourceTemplate[(String, String)] {
  override def initKafka(ssc: StreamingContext): (DStream[(String, String)], OffsetsManager) =
    KafkaSourceUtil.kvDStream(ssc)

  override def doWork(args: Array[String]): Unit = {
    getStream.map { f =>
      new ProducerRecord("test1", f._1, f._2)
    }.saveToKafka(getConf)
  }
}
