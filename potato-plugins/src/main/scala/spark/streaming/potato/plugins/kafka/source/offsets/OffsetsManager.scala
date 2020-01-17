package spark.streaming.potato.plugins.kafka.source.offsets

import kafka.cluster.BrokerEndPoint
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import org.apache.kafka.common.KafkaException
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka.OffsetRange
import spark.streaming.potato.plugins.kafka.source.offsets.storage.{HBaseOffsetsStorage, NoneOffsetsStorage}
import spark.streaming.potato.plugins.kafka.utils.OffsetsUtil
import spark.streaming.potato.plugins.kafka.KafkaConfigKeys._

import scala.collection.mutable

class OffsetsManager(conf: OffsetsManagerConf) extends Logging {
  private implicit val offsetUtilConf: ConsumerConfig = conf

  private[offsets] val brokers = OffsetsUtil.findBrokers(conf.bootstrapServers)
  private val groupId = conf.groupId
  private val storage: OffsetsStorage = conf.storageType match {
    case "kafka" => new KafkaOffsetsStorage(brokers, offsetUtilConf)
    case "zookeeper" => new ZookeeperOffsetsStorage(brokers, offsetUtilConf)
    case "hbase" => new HBaseOffsetsStorage(
      conf.getOrElse(HBASE_TABLE_KEY, HBASE_TABLE_DEFAULT),
      conf.subPrefixConf(HBASE_CONF_PREFIX)
    )
    case "none" => new NoneOffsetsStorage
    case unknown => throw new KafkaException(s"storage type not supported: $unknown")
  }
  private[offsets] val subscriptions: Set[TopicAndPartition] = OffsetsUtil.getTopicAndPartitions(brokers, conf.subscribeTopics)

  logInfo(s"OffsetsManager initialized: brokers -> $brokers, groupId -> $groupId, storage -> $storage, " +
    s"subscriptions -> $subscriptions")

  private val offsetsCache = new mutable.LinkedHashMap[Long, Map[TopicAndPartition, Long]]()

  def currentCache: Map[Long, Map[TopicAndPartition, Long]] = offsetsCache.toMap

  def committedOffsets(reset: Boolean = true): Map[TopicAndPartition, Long] = {
    val loaded = storage.load(groupId, subscriptions)
    val ret = if (reset)
      OffsetsUtil.validatedOrResetOffsets(brokers, loaded)
    else
      loaded
    logInfo(s"Get committedOffsets -> $ret")
    ret
  }

  def getLag(topics: Seq[String] = Seq.empty[String], committed: Boolean = true): Map[String, Long] = {
    if (!committed && offsetsCache.isEmpty)
      throw NotCacheAnyOffsetsException("OffsetManager has not cached any offsets.")

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
    val filter: ((String, Map[TopicAndPartition, Long])) => Boolean =
      if (topics.isEmpty) _ => true
      else f => topics.contains(f._1)

    latest.filter(filter).map { topicAndTapO =>
      topicAndTapO._1 -> topicAndTapO._2.map { tapO =>
        current.get(tapO._1) match {
          case Some(offset) => tapO._2 - offset
          case None =>
            logWarning(s"Partition not in cache -> $tapO")
            tapO._2 - earliest(topicAndTapO._1)(tapO._1)
        }
      }.sum
    }
  }

  def cacheOffsets(time: Long, offsetRanges: Seq[OffsetRange]): Unit = {
    val offsets = offsetRanges.map { offsetRange =>
      TopicAndPartition(offsetRange.topic, offsetRange.partition) -> offsetRange.untilOffset
    }.toMap
    offsetsCache += time -> offsets
    logInfo(s"CachedOffsets -> {$time -> $offsets}, current cache -> $currentCache")
  }

  def updateOffsets(offsets: Map[TopicAndPartition, Long]): Unit = {
    storage.save(groupId, offsets)
    logInfo(s"Offsets updated, groupId -> $groupId, offsets -> $offsets")
  }

  def updateOffsetsByDelay(time: Long, delay: Long = conf.offsetsAutoUpdateDelay): Unit = {
    logInfo(s"Start update offsets by:time -> $time, delay -> $delay")
    while (offsetsCache.nonEmpty && time - offsetsCache.head._1 >= delay) {
      val head = offsetsCache.head
      updateOffsets(head._2)
      offsetsCache -= head._1
      logInfo(s"Updated offsets -> $head")
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

case class NotCacheAnyOffsetsException(msg: String = null, throwable: Throwable = null) extends Exception(msg, throwable)

trait OffsetsStorage {
  def save(groupId: String, offsets: Map[TopicAndPartition, Long]): Boolean

  def load(groupId: String, taps: Set[TopicAndPartition]): Map[TopicAndPartition, Long]
}

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
