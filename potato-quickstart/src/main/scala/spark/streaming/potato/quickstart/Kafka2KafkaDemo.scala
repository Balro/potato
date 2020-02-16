package spark.streaming.potato.quickstart

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spark.streaming.potato.plugins.kafka.source.KafkaSource
import spark.streaming.potato.plugins.kafka.source.offsets.OffsetsManager
import spark.streaming.potato.template.template.KafkaSourceTemplate
import spark.streaming.potato.plugins.kafka.KafkaConfigKeys._
import spark.streaming.potato.plugins.kafka.sink.KafkaSinkImplicits._

object Kafka2KafkaDemo extends KafkaSourceTemplate[(String, String)] {
  override def initKafka(ssc: StreamingContext): (DStream[(String, String)], OffsetsManager) =
    KafkaSource.kvDStream(ssc)

  override def doWork(args: Array[String]): Unit = {
    stream.map { f =>
      new ProducerRecord("test1", f._1, f._2)
    }.saveToKafka(conf.getAllWithPrefix(KAFKA_PRODUCER_CONFIG_PREFIX).toMap)
  }
}
