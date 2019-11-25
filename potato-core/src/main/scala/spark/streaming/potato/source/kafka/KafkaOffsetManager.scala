package spark.streaming.potato.source.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.OffsetRange

trait KafkaOffsetManager {
  def cacheOffset(offsetRanges: Array[OffsetRange])

  def fromOffset:Map[TopicAndPartition, Long]

  def commitOffset()
}
