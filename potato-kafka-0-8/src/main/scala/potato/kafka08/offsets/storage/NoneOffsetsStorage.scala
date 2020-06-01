package potato.kafka08.offsets.storage

import kafka.common.TopicAndPartition
import org.apache.spark.internal.Logging

/**
 * NoneOffsetsStorage 不存储offsets，每次load时均触发reset后的值。
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
