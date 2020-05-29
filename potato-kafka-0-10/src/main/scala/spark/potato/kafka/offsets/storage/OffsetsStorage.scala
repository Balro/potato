package spark.potato.kafka.offsets.storage

import java.util.Properties

import kafka.cluster.BrokerEndPoint
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.spark.internal.Logging
import spark.potato.kafka.offsets.{KafkaConsumerOffsetsUtil, SimpleConsumerOffsetsUtil}

/**
 * OffsetsStorage特质，用于存储offsets。
 */
trait OffsetsStorage {
  def save(groupId: String, offsets: Map[TopicPartition, Long]): Boolean

  def load(groupId: String, taps: Set[TopicPartition]): Map[TopicPartition, Long]
}

/**
 * OffsetsStorage的kafka_zookeeper实现。
 *
 * @deprecated 新版本kafka建议使用kafka存储，废弃zookeeper存储。
 */
class ZookeeperOffsetsStorage(seeds: Map[String, Int], config: ConsumerConfig) extends OffsetsStorage with Logging {

  import spark.potato.kafka.offsets.SimpleConsumerOffsetsUtilImplicits.mapToBrokerEndPoints

  override def save(groupId: String, offsets: Map[TopicPartition, Long]): Boolean = {
    val (ret, errs) = SimpleConsumerOffsetsUtil.commitOffsetsOnZookeeper(seeds, groupId, {
      offsets.map(f => TopicAndPartition(f._1.topic(), f._1.partition()) -> f._2)
    })(config)
    errs.foreach { err => logWarning("commit on zookeeper found err", err) }
    ret
  }

  override def load(groupId: String, taps: Set[TopicPartition]): Map[TopicPartition, Long] = {
    SimpleConsumerOffsetsUtil.fetchOffsetsOnZookeeper(seeds, groupId, {
      taps.map(f => TopicAndPartition(f.topic(), f.partition()))
    })(config).map(f => new TopicPartition(f._1.topic, f._1.partition) -> f._2)
  }
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