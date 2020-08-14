package potato.kafka010.offsets.storage

import java.util.Properties

import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.spark.internal.Logging
import potato.kafka010.offsets.utils.KafkaConsumerOffsetsUtil

/**
 * OffsetsStorage特质，用于存储offsets。
 */
trait OffsetsStorage {
  def save(groupId: String, offsets: Map[TopicPartition, Long]): Boolean

  def load(groupId: String, taps: Set[TopicPartition]): Map[TopicPartition, Long]
}

/**
 * OffsetsStorage的新consumerApi实现。
 */
class KafkaOffsetsStorage(props: Properties) extends OffsetsStorage with Logging {
  override def save(groupId: String, offsets: Map[TopicPartition, Long]): Boolean = {
    try {
      KafkaConsumerOffsetsUtil.commitOffsets(props, offsets, groupId)
      true
    } catch {
      case e: KafkaException =>
        logWarning("Commit offset failed.", e)
        false
      case e: Exception => throw e
    }

  }

  override def load(groupId: String, tps: Set[TopicPartition]): Map[TopicPartition, Long] = {
    KafkaConsumerOffsetsUtil.fetchOffsets(props, tps, groupId)
  }
}