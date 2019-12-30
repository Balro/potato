package spark.streaming.potato.source.kafka.tools

import kafka.cluster.BrokerEndPoint
import kafka.common.{InvalidConfigException, TopicAndPartition}
import kafka.consumer.ConsumerConfig
import org.apache.kafka.common.KafkaException
import org.apache.spark.streaming.kafka.OffsetRange
import spark.streaming.potato.source.kafka.utils.OffsetsUtil
import spark.streaming.potato.source.kafka.utils.OffsetsUtilImplicits.{defaultConfig => _, _}

import scala.collection.mutable

class OffsetsManager(conf: OffsetsManagerConfig) {
  private val storage: OffsetsStorage = initStorage
  private val subscriptions: Set[TopicAndPartition] = initSubscriptions
  private val groupId = conf.groupId
  private val offsetsCache = new mutable.LinkedHashMap[Long, Map[TopicAndPartition, Long]]()
  private implicit val offsetUtilConf: ConsumerConfig = conf

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

  def getStartOffsets(reset: Boolean = true): Map[TopicAndPartition, Long] = {
    val loaded = storage.load(groupId, subscriptions)
    OffsetsUtil.validatedOrResetOffsets(conf.bootstrapServers, loaded)
  }

  def getLag(topics: Seq[String] = Seq.empty[String]): Map[String, Long] = {
    if (offsetsCache.isEmpty)
      throw new Exception("OffsetManager has not cached any offsets.")

    val latest = groupByTopic(OffsetsUtil.getLatestOffsets(conf.bootstrapServers,
      OffsetsUtil.getTopicAndPartitions(conf.bootstrapServers, subscriptions.map(_.topic))))
    val earliest = groupByTopic(OffsetsUtil.getEarliestOffsets(conf.bootstrapServers,
      OffsetsUtil.getTopicAndPartitions(conf.bootstrapServers, subscriptions.map(_.topic))))
    val current = offsetsCache.last._2
    val filter: ((String, Map[TopicAndPartition, Long])) => Boolean =
      if (topics.isEmpty) _ => true
      else f => topics.contains(f._1)

    latest.filter(filter).map { topicAndTapO =>
      topicAndTapO._1 -> topicAndTapO._2.map { tapO =>
        current.get(tapO._1) match {
          case Some(offset) => tapO._2 - offset
          case None => tapO._2 - earliest(topicAndTapO._1)(tapO._1)
        }
      }.sum
    }
  }

  def cacheOffsets(time: Long, offsetRanges: Seq[OffsetRange]): Unit = {
    offsetsCache += time -> offsetRanges.map { offsetRange =>
      TopicAndPartition(offsetRange.topic, offsetRange.partition) -> offsetRange.untilOffset
    }.toMap
  }

  def updateOffsets(offsets: Map[TopicAndPartition, Long]): Unit = storage.save(groupId, offsets)

  def updateOffsetsByDelay(delay: Long): Unit = {
    while (offsetsCache.nonEmpty && offsetsCache.last._1 - offsetsCache.head._1 >= conf.offsetsAutoUpdateDelay) {
      val head = offsetsCache.head
      updateOffsets(head._2)
      offsetsCache -= head._1
    }
  }

  private def groupByTopic(offsets: Map[TopicAndPartition, Long]): Map[String, Map[TopicAndPartition, Long]] = {
    offsets.groupBy { f =>
      f._1.topic
    }
  }

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
      if (!conf.contains(k))
        throw new InvalidConfigException(s"configuration $k not found.")
    }
    val mapConf = mutable.Map.empty[String, String]
    mapKey.foreach { k =>
      k._2.foreach { kv =>
        if (conf.contains(k._1))
          mapConf += (k._1 -> kv._2)
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
  val offsetsAutoUpdate: Boolean = cleanedConf.getOrElse(
    OffsetsManagerConfig.OFFSETS_AUTO_UPDATE_KEY, OffsetsManagerConfig.OFFSETS_AUTO_UPDATE_DEFAULT).toBoolean
  val offsetsAutoUpdateDelay: Long = cleanedConf.getOrElse(
    OffsetsManagerConfig.OFFSETS_AUTO_UPDATE_DELAY_KEY, OffsetsManagerConfig.OFFSETS_AUTO_UPDATE_DELAY_DEFAULT).toLong
}

object OffsetsManagerConfig {
  val POTATO_KAFKA_OFFSETS_STORAGE_KEY = "potato.kafka.offsets.storage"
  val POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY = "potato.kafka.subscribe.topics"
  val GROUP_ID_KEY = "group.id"
  val OFFSET_RESET_POLICY = "auto.offset.reset"
  val BOOTSTRAP_SERVERS_KEY = "bootstrap.servers"
  val OFFSETS_AUTO_UPDATE_KEY = "offsets.auto.update"
  val OFFSETS_AUTO_UPDATE_DEFAULT = "true"
  val OFFSETS_AUTO_UPDATE_DELAY_KEY = "offsets.auto.update.delay"
  val OFFSETS_AUTO_UPDATE_DELAY_DEFAULT = "0"

  implicit def toMap(config: OffsetsManagerConfig): Map[String, String] = {
    config.cleanedConf
  }

  implicit def toConsumerConfig(config: OffsetsManagerConfig): ConsumerConfig = {
    new ConsumerConfig(config.cleanedConf)
  }
}

trait OffsetsStorage {
  def save(groupId: String, offsets: Map[TopicAndPartition, Long]): Boolean

  def load(groupId: String, tap: Set[TopicAndPartition]): Map[TopicAndPartition, Long]
}

class ZookeeperOffsetsStorage extends OffsetsStorage {
  override def save(groupId: String, offsets: Map[TopicAndPartition, Long]): Boolean = ???

  override def load(groupId: String, tap: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = ???
}

class KafkaOffsetsStorage extends OffsetsStorage {
  override def save(groupId: String, offsets: Map[TopicAndPartition, Long]): Boolean = ???

  override def load(groupId: String, tap: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = ???
}
