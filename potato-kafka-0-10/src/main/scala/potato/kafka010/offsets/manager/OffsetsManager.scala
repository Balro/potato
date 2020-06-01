package potato.kafka010.offsets.manager

import java.util.Properties

import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.OffsetRange
import spark.potato.kafka.conf._
import spark.potato.kafka.offsets.storage._
import spark.potato.kafka.offsets.KafkaConsumerOffsetsUtil

import scala.collection.mutable

/**
 * KafkaOffsets 管理工具。提供offsets基本管理功能。
 *
 * @param conf 专用配置类，对SparkConf进行了简单包装，便于访问。
 */
class OffsetsManager(conf: SparkConf, kafkaParams: Map[String, String] = Map.empty) extends Logging {
  val kafkaConf = new PotatoKafkaConf(conf, kafkaParams)
  val consumerProps: Properties = kafkaConf.toConsumerProperties

  // 初始化offsets存储。
  private val storage: OffsetsStorage = kafkaConf.offsetsStorageType match {
    case "kafka" => new KafkaOffsetsStorage(consumerProps)
    case "zookeeper" =>
      logWarning("ZookeeperOffsetsStorage is deprecated and not recommended.")
      new ZookeeperOffsetsStorage(
        kafkaConf.bootstrapServers.split(",").map(f => f.split(":")(0) -> f.split(":")(1).toInt).toMap,
        kafkaConf.toSimpleConsumerConfig)
    case "hbase" => new HBaseOffsetsStorage(kafkaConf.offsetsStorageHBaseTable, kafkaConf.offsetsStorageHBaseFamily, kafkaConf.hbaseConf)
    case "none" => new NoneOffsetsStorage
    case unknown => throw new KafkaException(s"storage type not supported: $unknown")
  }

  // 初始化topic订阅信息。
  private[offsets] val subscriptions: Set[TopicPartition] = KafkaConsumerOffsetsUtil.getTopicsInfo(consumerProps, kafkaConf.subscribeTopics).map(f => new TopicPartition(f.topic(), f.partition()))

  logInfo(s"OffsetsManager initialized: groupId -> ${kafkaConf.groupId}, storage -> $storage, " +
    s"subscriptions -> $subscriptions")

  // 缓存offsets信息，用于延迟提交offsets。
  private val offsetsCache = new mutable.LinkedHashMap[Long, Map[TopicPartition, Long]]()

  def currentCache: Map[Long, Map[TopicPartition, Long]] = offsetsCache.toMap

  def getCachedOffsets(time: Long): Map[TopicPartition, Long] = {
    offsetsCache.get(time) match {
      case Some(offsets) => offsets
      case None => Map.empty
    }
  }

  /**
   * 获取已提交的offsets。
   *
   * @param reset 如已提交的offsets不再可用offsets范围内，是否重置。
   */
  def committedOffsets(reset: Boolean = true): Map[TopicPartition, Long] = {
    val loaded = storage.load(kafkaConf.groupId, subscriptions)
    val ret = if (reset)
      KafkaConsumerOffsetsUtil.validatedOffsets(consumerProps, loaded)
    else
      loaded
    logInfo(s"Get committedOffsets -> $ret")
    ret
  }

  /**
   * 获取给定topic的延迟。
   */
  def getLagByTopic(ts: Set[String]): Map[String, Long] = {
    getLag(subscriptions.filter(f => ts.contains(f.topic()))).groupBy(_._1.topic())
      .map(f => f._1 -> f._2.foldLeft(0L) { (r, t) => r + t._2 })
  }

  /**
   * 获取给定分区的延迟。
   */
  def getLag(tps: Set[TopicPartition] = subscriptions): Map[TopicPartition, Long] = {
    val cur = committedOffsets(false)
    println(s"cur $cur")
    KafkaConsumerOffsetsUtil.getLatestOffsets(consumerProps, tps).map { f =>
      f._1 -> (f._2 - cur(f._1))
    }
  }

  /**
   * 缓存offsets。
   *
   * @param time 当前计算rdd的生成时间。
   */
  def cacheOffsets(time: Long, offsetRanges: Seq[OffsetRange]): Unit = {
    val offsets = offsetRanges.map { offsetRange =>
      new TopicPartition(offsetRange.topic, offsetRange.partition) -> offsetRange.untilOffset
    }.toMap
    offsetsCache += time -> offsets
    logInfo(s"CachedOffsets -> {$time -> $offsets}, current cache -> $currentCache")
  }

  /**
   * 更新offsets。
   */
  def updateOffsets(offsets: Map[TopicPartition, Long]): Unit = {
    storage.save(kafkaConf.groupId, offsets)
    logInfo(s"Offsets updated, groupId -> ${kafkaConf.groupId}, offsets -> $offsets")
  }

  /**
   * 根据延迟更新offsets。
   *
   * @param time  当前计算rdd的生成时间。
   * @param delay 提交offsets的延后时间，会在offsetsCache中扫描所有时间戳小于 (time - delay) 的offsets并依次提交。
   */
  def updateOffsetsByDelay(time: Long, delay: Long = kafkaConf.offsetsStorageUpdateDelay): Unit = {
    logInfo(s"Start update offsets by:time -> $time, delay -> $delay")
    while (offsetsCache.nonEmpty && time - offsetsCache.head._1 >= delay) {
      val head = offsetsCache.head
      updateOffsets(head._2)
      offsetsCache -= head._1
      logInfo(s"Updated offsets -> $head")
    }
  }
}
