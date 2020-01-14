package spark.streaming.potato.core.source.kafka.offsets.storage

import kafka.common.TopicAndPartition
import org.apache.spark.internal.Logging
import spark.streaming.potato.core.source.kafka.offsets.OffsetsStorage

/**
 * NoneOffsetsStorage 不对offsets做任何操作，每次load时均触发reset。
 */
class NoneOffsetsStorage extends OffsetsStorage with Logging {
  override def save(groupId: String, offsets: Map[TopicAndPartition, Long]): Boolean = {
    logInfo("NoneOffsetsStorage save nothing.")
    true
  }

  override def load(groupId: String, taps: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    logInfo("NoneOffsetsStorage load nothing but -1, it will be reset all the time.")
    taps.map { tap =>
      tap -> -1L
    }.toMap
  }
}
