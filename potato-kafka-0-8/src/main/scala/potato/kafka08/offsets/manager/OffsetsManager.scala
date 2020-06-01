package potato.kafka08.offsets.manager

import kafka.cluster.BrokerEndPoint
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import org.apache.kafka.common.KafkaException
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka.OffsetRange
import spark.potato.kafka.conf._
import spark.potato.kafka.offsets.storage._
import spark.potato.kafka.utils.OffsetsUtil
import spark.potato.kafka.exception._

import scala.collection.mutable

/**
 * KafkaOffsets 管理工具。提供offsets基本管理功能。
 *
 * @param conf 专用配置类，对SparkConf进行了简单包装，便于访问。
 */
class OffsetsManager(conf: OffsetsManagerConf) extends Logging {
  def this(sparkConf: SparkConf, kafkaParams: Map[String, String] = Map.empty) {
    this(new OffsetsManagerConf(sparkConf, kafkaParams))
  }

  private implicit val offsetUtilConf: ConsumerConfig = conf

  // 获取kafka全部broker信息。
  private[offsets] val brokers = OffsetsUtil.findBrokers(conf.bootstrapServers)
  private val groupId = conf.groupId

  // 初始化offsets存储。
  private val storage: OffsetsStorage = conf.storageType match {
    case "kafka" => new KafkaOffsetsStorage(brokers, offsetUtilConf)
    case "zookeeper" => new ZookeeperOffsetsStorage(brokers, offsetUtilConf)
    case "hbase" => new HBaseOffsetsStorage(
      conf.getOrElse(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_TABLE_KEY, POTATO_KAFKA_OFFSETS_STORAGE_HBASE_TABLE_DEFAULT),
      conf.subPrefixConf(POTATO_KAFKA_OFFSETS_STORAGE_HBASE_PREFIX)
    )
    case "none" => new NoneOffsetsStorage
    case unknown => throw new KafkaException(s"storage type not supported: $unknown")
  }

  // 初始化topic订阅信息。
  private[offsets] val subscriptions: Set[TopicAndPartition] = OffsetsUtil.getTopicAndPartitions(brokers, conf.subscribeTopics)

  logInfo(s"OffsetsManager initialized: brokers -> $brokers, groupId -> $groupId, storage -> $storage, " +
    s"subscriptions -> $subscriptions")

  // 缓存offsets信息，用于延迟提交offsets。
  private val offsetsCache = new mutable.LinkedHashMap[Long, Map[TopicAndPartition, Long]]()

  def currentCache: Map[Long, Map[TopicAndPartition, Long]] = offsetsCache.toMap

  /**
   * 获取已提交的offsets。
   *
   * @param reset 如已提交的offsets不再可用offsets范围内，是否重置。
   */
  def committedOffsets(reset: Boolean = true): Map[TopicAndPartition, Long] = {
    val loaded = storage.load(groupId, subscriptions)
    val ret = if (reset)
      OffsetsUtil.validatedOrResetOffsets(brokers, loaded)
    else
      loaded
    logInfo(s"Get committedOffsets -> $ret")
    ret
  }

  /**
   * 获取给定topic的延迟。
   *
   * @param topics    需要获取延迟的topic名称，如未指定或指定空集合，则获取当前消费的所有topic延迟。
   * @param committed 是否根据已提交的offsets计算延迟，如配置为false，则从offsets缓存中获取延迟(缓存的offsets尚未提交)。
   */
  def getLag(topics: Seq[String] = Seq.empty[String], committed: Boolean = true): Map[String, Long] = {
    if (!committed && offsetsCache.isEmpty)
      throw NotCacheAnyOffsetsException("OffsetManager has not cached any offsets.")

    // 获取可用offsets范围。
    val latest = groupByTopic(OffsetsUtil.getLatestOffsets(brokers,
      OffsetsUtil.getTopicAndPartitions(brokers, subscriptions.map(_.topic))))
    val earliest = groupByTopic(OffsetsUtil.getEarliestOffsets(brokers,
      OffsetsUtil.getTopicAndPartitions(brokers, subscriptions.map(_.topic))))

    val current =
      if (committed) {
        committedOffsets(false)
      } else {
        currentCache.last._2
      }

    // 过滤未指定的topic并计算延迟。
    latest.filter { tto =>
      if (topics.isEmpty) true
      else topics.contains(tto._1)
    }.map { tto =>
      tto._1 -> tto._2.map { tapO =>
        current.get(tapO._1) match {
          case Some(offset) => tapO._2 - offset
          case None =>
            logWarning(s"Partition not in cache -> $tapO")
            tapO._2 - earliest(tto._1)(tapO._1)
        }
      }.sum
    }
  }

  /**
   * 缓存offsets。
   *
   * @param time 当前计算rdd的生成时间。
   */
  def cacheOffsets(time: Long, offsetRanges: Seq[OffsetRange]): Unit = {
    val offsets = offsetRanges.map { offsetRange =>
      TopicAndPartition(offsetRange.topic, offsetRange.partition) -> offsetRange.untilOffset
    }.toMap
    offsetsCache += time -> offsets
    logInfo(s"CachedOffsets -> {$time -> $offsets}, current cache -> $currentCache")
  }

  /**
   * 更新offsets。
   */
  def updateOffsets(offsets: Map[TopicAndPartition, Long]): Unit = {
    storage.save(groupId, offsets)
    logInfo(s"Offsets updated, groupId -> $groupId, offsets -> $offsets")
  }

  /**
   * 一定延迟后更新offsets。
   *
   * @param time  当前计算rdd的生成时间。
   * @param delay 提交offsets的延后时间，会在offsetsCache中扫描所有时间戳小于 (time - delay) 的offsets并依次提交。
   */
  def updateOffsetsByDelay(time: Long, delay: Long = conf.offsetsAutoUpdateDelay): Unit = {
    logInfo(s"Start update offsets by:time -> $time, delay -> $delay")
    while (offsetsCache.nonEmpty && time - offsetsCache.head._1 >= delay) {
      val head = offsetsCache.head
      updateOffsets(head._2)
      offsetsCache -= head._1
      logInfo(s"Updated offsets -> $head")
    }
  }

  /**
   * 将传入的offsets根据topic进行聚合。
   */
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
