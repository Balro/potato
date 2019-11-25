package spark.streaming.potato.source.kafka.imp

import kafka.api.OffsetCommitRequest
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}
import spark.streaming.potato.source.kafka.KafkaOffsetManager

class KafkaBrokerOffsetManager(val ssc: StreamingContext, kafkaParams: Map[String, String]) extends KafkaOffsetManager {
  val kafkaCluster = new KafkaCluster(kafkaParams)
  var offsetRangesCache: Array[OffsetRange] = _

  override def cacheOffset(offsetRanges: Array[OffsetRange]): Unit = offsetRangesCache = offsetRanges

  override def fromOffset: Map[TopicAndPartition, Long] = ???

  override def commitOffset(): Unit = ???
}
