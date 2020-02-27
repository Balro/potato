package spark.potato.kafka.offsets.storage

import kafka.cluster.BrokerEndPoint
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import org.apache.spark.internal.Logging
import spark.potato.kafka.utils.OffsetsUtil

/**
 * OffsetsStorage特质，用于存储offsets。
 */
trait OffsetsStorage {
  def save(groupId: String, offsets: Map[TopicAndPartition, Long]): Boolean

  def load(groupId: String, taps: Set[TopicAndPartition]): Map[TopicAndPartition, Long]
}

/**
 * OffsetsStorage的kafka_zookeeper实现。
 */
class ZookeeperOffsetsStorage(seeds: Set[BrokerEndPoint], config: ConsumerConfig) extends OffsetsStorage with Logging {
  override def save(groupId: String, offsets: Map[TopicAndPartition, Long]): Boolean = {
    val (ret, errs) = OffsetsUtil.commitOffsetsOnZookeeper(seeds, groupId, offsets)(config)
    errs.foreach { err => logWarning("commit on zookeeper found err", err) }
    ret
  }

  override def load(groupId: String, taps: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    OffsetsUtil.fetchOffsetsOnZookeeper(seeds, groupId, taps)(config)
  }
}

/**
 * OffsetsStorage的kafka_broker实现。
 */
class KafkaOffsetsStorage(seeds: Set[BrokerEndPoint], config: ConsumerConfig) extends OffsetsStorage with Logging {
  override def save(groupId: String, offsets: Map[TopicAndPartition, Long]): Boolean = {
    val (ret, errs) = OffsetsUtil.commitOffsetsOnKafka(seeds, groupId, offsets)(config)
    errs.foreach { err => logWarning("commit on kafka found err", err) }
    ret
  }

  override def load(groupId: String, taps: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    OffsetsUtil.fetchOffsetsOnKafka(seeds, groupId, taps)(config)
  }
}