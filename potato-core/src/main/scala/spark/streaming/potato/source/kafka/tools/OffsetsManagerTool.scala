package spark.streaming.potato.source.kafka.tools

import java.util.concurrent.LinkedBlockingQueue

import kafka.cluster.BrokerEndPoint
import kafka.common.{InvalidConfigException, TopicAndPartition}
import org.apache.kafka.common.KafkaException
import spark.streaming.potato.source.kafka.utils.OffsetsUtil
import spark.streaming.potato.source.kafka.utils.OffsetsUtilImplicits._

import scala.collection.mutable

class OffsetsManager(conf: OffsetsManagerConfig) {
  private val storage: OffsetsStorage = initStorage
  private val subscriptions: Set[TopicAndPartition] = initSubscriptions
  private val groupId = conf.groupId
  private val offsetsCache = new LinkedBlockingQueue[(Long, TopicPartitionOffset)](conf.offsetsCacheSize)

  private def initSubscriptions: Set[TopicAndPartition] = {
    OffsetsUtil.getTopicAndPartitions(conf.bootstrapServers, conf.subscribeTopics)
  }

  private def initStorage: OffsetsStorage = {
    conf.storageType match {
      case "kafka" => new KafkaOffsetsStorage()
      case "zookeeper" => new ZookeeperOffsetsStorage()
      case unknown => throw new KafkaException(s"storage type not supported: $unknown")
    }
  }

  def getOffsets(topics: Seq[String], reset: Boolean = true): Map[TopicAndPartition, Long] = ???

  def getLag(topics: Seq[String] = Seq.empty[String]): Map[String, Long] = ???

  def updateOffsets: Unit = ???

  private def groupByTopic(offsets: Map[TopicAndPartition, Long]) = {
    offsets.groupBy { f =>
      f._1.topic
    }
  }

  case class TopicPartitionOffset(topic: String, partition: Int, offset: Long)

  implicit def stringToBrokers(str: String): Set[BrokerEndPoint] = {
    str.trim.split(",").map { bs =>
      val b = bs.split(":").map(_.trim)
      BrokerEndPoint(OffsetsUtil.unknownBrokerId, b(0), b(1).toInt)
    }.toSet
  }

}

class OffsetsManagerConfig(conf: Map[String, String]) {
  private val cleanedConf = init()
  private val requiredKey = Set(
    OffsetsManagerConfig.POTATO_KAFKA_OFFSETS_STORAGE_KEY,
    OffsetsManagerConfig.POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY,
    OffsetsManagerConfig.GROUP_ID_KEY,
    OffsetsManagerConfig.OFFSET_RESET_POLICY,
    OffsetsManagerConfig.BOOTSTRAP_SERVERS_KEY
  )

  private val mapKey = Map(
    OffsetsManagerConfig.OFFSET_RESET_POLICY -> Set("earliest" -> "smallest", "latest" -> "largest")
  )

  private def init(): Map[String, String] = {
    requiredKey.foreach { k =>
      conf.get(k) match {
        case None => throw new InvalidConfigException(s"configuration $k not found.")
      }
    }
    val mapConf = mutable.Map.empty[String, String]
    mapKey.foreach { k =>
      k._2.foreach { kv =>
        conf.get(k._1) match {
          case Some(kv._1) => mapConf += (k._1 -> kv._2)
        }
      }
    }

    conf ++ mapConf
  }

  val storageType: String = cleanedConf(OffsetsManagerConfig.POTATO_KAFKA_OFFSETS_STORAGE_KEY)
  val subscribeTopics: Set[String] = cleanedConf(OffsetsManagerConfig.POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY)
    .split(",").map(_.trim).toSet
  val groupId: String = cleanedConf(OffsetsManagerConfig.GROUP_ID_KEY)
  val offsetResetPolicy: String = cleanedConf(OffsetsManagerConfig.OFFSET_RESET_POLICY)
  val bootstrapServers: String = cleanedConf(OffsetsManagerConfig.BOOTSTRAP_SERVERS_KEY)
  val offsetsCacheSize: Int = cleanedConf.getOrElse(
    OffsetsManagerConfig.OFFSETS_CACHE_SIZE_KEY, OffsetsManagerConfig.OFFSETS_CACHE_SIZE_DEFAULT).toInt
}

object OffsetsManagerConfig {
  val POTATO_KAFKA_OFFSETS_STORAGE_KEY = "potato.kafka.offsets.storage"
  val POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY = "potato.kafka.subscribe.topics"
  val GROUP_ID_KEY = "group.id"
  val OFFSET_RESET_POLICY = "auto.offset.reset"
  val BOOTSTRAP_SERVERS_KEY = "bootstrap.servers"
  val OFFSETS_CACHE_SIZE_KEY = "offsets.cache.size"
  val OFFSETS_CACHE_SIZE_DEFAULT = "1"

  implicit def toMap(config: OffsetsManagerConfig): Map[String, String] = {
    config.cleanedConf
  }
}

trait OffsetsStorage {
  def save(offsets: Map[TopicAndPartition, Long]): Boolean

  def load(topics: Seq[String]): Map[TopicAndPartition, Long]
}

class ZookeeperOffsetsStorage extends OffsetsStorage {
  override def save(offsets: Map[TopicAndPartition, Long]): Boolean = ???

  override def load(topics: Seq[String]): Map[TopicAndPartition, Long] = ???
}

class KafkaOffsetsStorage extends OffsetsStorage {
  override def save(offsets: Map[TopicAndPartition, Long]): Boolean = ???

  override def load(topics: Seq[String]): Map[TopicAndPartition, Long] = ???
}
