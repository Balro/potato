package potato.kafka010.offsets

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import spark.potato.kafka.exception.InvalidConfigException

/**
 * 使用kafka新ConsumerApi的OffsetsUtil实现，仅支持操作kafka存储的offsets。
 */
object KafkaConsumerOffsetsUtil {
  val invalidOffset: Long = -1L

  import scala.collection.JavaConversions._

  /**
   * 检验提供的offsets是否在可用范围内，同时检查是否有遗漏的partition。
   * 当出现上述情况时，如果关闭reset时，则抛出[[OffsetOutOfRangeException]]。
   * 如果开启reset，则将对应partition根据[[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG]]进行重置。
   *
   * @param reset 是否进行重置。
   */
  def validatedOffsets(props: Properties, pos: Map[TopicPartition, Long], reset: Boolean = true): Map[TopicPartition, Long] = {
    val aps = getTopicsPartition(props, pos.groupBy(_._1.topic()).keySet)
    val ps = pos.keySet
    val earliest: Map[TopicPartition, Long] = getEarliestOffsets(props, aps)
    val latest: Map[TopicPartition, Long] = getLatestOffsets(props, aps)
    aps.map { f =>
      val eo = earliest(f)
      val lo = latest(f)
      val po = pos.getOrElse(f, invalidOffset)
      if (ps.contains(f) && po >= eo && po <= lo) {
        f -> po
      } else {
        if (reset) {
          f -> {
            props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) match {
              case "earliest" => eo
              case "latest" => lo
              case _ => throw InvalidConfigException(s"Config not found ${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}.")
            }
          }
        } else
          throw new OffsetOutOfRangeException(s"Offset $f:$po out of range [$eo,$lo].")
      }
    }.toMap
  }

  /**
   * 创建一个新的properti对象，并更新group.id到指定值。
   */
  private def groupedProps(props: Properties, group: String): Properties = {
    val groupedProps = new Properties()
    groupedProps.putAll(props)
    groupedProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)
    groupedProps
  }

  /**
   * 提交offsets到kafka。
   */
  def commitOffsets(props: Properties, pos: Map[TopicPartition, Long], group: String): Unit = {
    withConsumer(groupedProps(props, group)) { consumer =>
      consumer.commitSync(pos.map(f => f._1 -> new OffsetAndMetadata(f._2)))
    }
  }

  /**
   * 获取已提交至kafka的offsets。
   */
  def fetchTopicOffsets(props: Properties, topic: Set[String], group: String): Map[TopicPartition, Long] = {
    fetchOffsets(props, getTopicsPartition(props, topic), group)
  }

  /**
   * 获取已提交至kafka的offsets。
   */
  def fetchOffsets(props: Properties, tps: Set[TopicPartition], group: String): Map[TopicPartition, Long] = {
    withConsumer(groupedProps(props, group)) { consumer =>
      tps.map(f => f -> {
        val om = consumer.committed(f)
        if (om == null)
          invalidOffset
        else
          om.offset()
      }).toMap
    }
  }

  /**
   * 获取最新的offsets。
   */
  def getLatestOffsets(props: Properties, tps: Set[TopicPartition]): Map[TopicPartition, Long] = {
    getOffsets(props, tps, "latest")
  }

  /**
   * 获取最早的offsets。
   */
  def getEarliestOffsets(props: Properties, tps: Set[TopicPartition]): Map[TopicPartition, Long] = {
    getOffsets(props, tps, "earliest")
  }

  /**
   * 获取offsets。
   *
   * @param policy earliest"或"latest"。
   */
  def getOffsets(props: Properties, tps: Set[TopicPartition], policy: String): Map[TopicPartition, Long] = {
    withConsumer(props) { consumer =>
      consumer.assign(tps)
      policy match {
        case "earliest" => consumer.seekToBeginning(tps)
        case "latest" => consumer.seekToEnd(tps)
      }
      tps.map(f => f -> consumer.position(f)).toMap
    }
  }

  def getTopicsPartition(props: Properties, topics: Set[String] = Set.empty): Set[TopicPartition] = {
    getTopicsInfo(props, topics).map(f => new TopicPartition(f.topic(), f.partition()))
  }

  /**
   * 获取给定topic的信息，如topics未指定货为空，则获取全部topic的信息。
   */
  def getTopicsInfo(props: Properties, topics: Set[String] = Set.empty): Set[PartitionInfo] = {
    withConsumer(props) { consumer =>
      if (topics.isEmpty)
        consumer.listTopics().flatMap(f => f._2).toSet
      else
        consumer.listTopics().filter(f => topics.contains(f._1)).flatMap(f => f._2).toSet
    }
  }

  /**
   * 由于此处使用consumer并非用于poll数据，因此反序列化类直接设置为默认值String。
   */
  def withConsumer[R](props: Properties)(f: KafkaConsumer[String, String] => R): R = {
    val consumer = new KafkaConsumer[String, String](props)
    val ret = f(consumer)
    consumer.close()
    ret
  }
}
