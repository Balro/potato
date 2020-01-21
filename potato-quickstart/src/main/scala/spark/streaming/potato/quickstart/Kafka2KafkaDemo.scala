package spark.streaming.potato.quickstart

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spark.streaming.potato.plugins.kafka.source.KafkaSource
import spark.streaming.potato.plugins.kafka.source.offsets.OffsetsManager
import spark.streaming.potato.template.template.KafkaSourceTemplate
import spark.streaming.potato.plugins.kafka.KafkaImplicits._
import spark.streaming.potato.plugins.kafka.KafkaConfigKeys._
import spark.streaming.potato.plugins.kafka.utils.OffsetsUtilImplicits._

object Kafka2KafkaDemo extends KafkaSourceTemplate[(String, String)] {
  override def initKafka(ssc: StreamingContext): (DStream[(String, String)], OffsetsManager) =
    KafkaSource.kvDStream(ssc)

  override def doWork(args: Array[String]): Unit = {
    stream.map { f =>
      TimeUnit.SECONDS.sleep(f._2.length)
      new ProducerRecord("test2", f._1, f._2)
    }.foreachRDD { rdd =>
      rdd.saveToKafka(conf.getAllWithPrefix(PRODUCER_CONFIG_PREFIX).toMap)
    }
  }
}
