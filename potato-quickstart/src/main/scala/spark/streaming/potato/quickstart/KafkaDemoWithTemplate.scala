package spark.streaming.potato.quickstart

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spark.streaming.potato.plugins.kafka.source._
import spark.streaming.potato.template.KafkaSourceTemplate

object KafkaDemoWithTemplate extends KafkaSourceTemplate[String] {
  override def initKafka(ssc: StreamingContext): (DStream[String], OffsetsManager) =
    KafkaSourceUtil.valueDStream(ssc)

  override def doWork(args: Array[String]): Unit = {
    getStream.flatMap(f => f.split("\\s")).map((_, 1)).reduceByKey(_ + _).print()
  }
}
