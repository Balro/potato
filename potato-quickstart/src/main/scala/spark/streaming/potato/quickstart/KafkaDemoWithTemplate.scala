package spark.streaming.potato.quickstart

import spark.streaming.potato.source.kafka.KafkaSource
import spark.streaming.potato.template.KafkaSourceTemplate

object KafkaDemoWithTemplate extends KafkaSourceTemplate[String](KafkaSource.valueDStream) {
  override def doWork(args: Array[String]): Unit = {
    stream.flatMap(f => f.split("\\s")).map((_, 1)).reduceByKey(_ + _).print()
  }
}
