package potato.kafka010.offsets.manager

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import potato.common.exception.PotatoMethodException
import potato.kafka010.conf._
import potato.kafka010.offsets.storage._
import potato.kafka010.offsets.listener.OffsetsUpdateListener
import potato.kafka010.offsets.utils.KafkaConsumerOffsetsUtil

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * KafkaOffsets 管理工具。提供offsets基本管理功能。
 */
class OffsetsManager(val kafkaConf: PotatoKafkaConf) extends Logging {

  def this(conf: SparkConf, kafkaParams: Map[String, String] = Map.empty) {
    this(new PotatoKafkaConf(conf, kafkaParams))
  }

  //  val kafkaConf = new PotatoKafkaConf(conf, kafkaParams)
  val consumerProps: Properties = kafkaConf.toConsumerProperties

  // 初始化offsets存储。
  private val storage: OffsetsStorage = kafkaConf.offsetsStorageType match {
    case "kafka" => new KafkaOffsetsStorage(consumerProps)
    case "hbase" => new HBaseOffsetsStorage(kafkaConf.offsetsStorageHBaseTable, kafkaConf.offsetsStorageHBaseFamily, kafkaConf.hbaseConf)
    case "none" => new NoneOffsetsStorage
    case unknown => throw new KafkaException(s"storage type not supported: $unknown")
  }

  private val dStreamCreated = new AtomicBoolean(false)

  private[kafka010] var subscriptions: Set[TopicPartition] = KafkaConsumerOffsetsUtil.getTopicsPartition(consumerProps, kafkaConf.subscribeTopics)

  def subscribePartetion(parts: Set[TopicPartition]): OffsetsManager = this.synchronized {
    if (dStreamCreated.get()) throw new PotatoMethodException(s"Subscribe is not allowed when dstream created.")
    subscriptions = parts
    this
  }

  def subscribeTopic(tpcs: Set[String]): OffsetsManager = this.synchronized {
    if (dStreamCreated.get()) throw new PotatoMethodException(s"Subscribe is not allowed when dstream created.")
    subscriptions = KafkaConsumerOffsetsUtil.getTopicsPartition(consumerProps, tpcs)
    this
  }

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
  def committedOffsets(reset: Boolean = false): Map[TopicPartition, Long] = {
    val loaded = storage.load(kafkaConf.groupId, subscriptions)
    val ret = if (reset)
      KafkaConsumerOffsetsUtil.validatedOffsets(consumerProps, loaded, reset = true)
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
    val cur = committedOffsets()
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
  def updateOffsets(offsets: Map[TopicPartition, Long]): Unit = this.synchronized {
    storage.save(kafkaConf.groupId, offsets)
    logInfo(s"Offsets updated, groupId -> ${kafkaConf.groupId}, offsets -> $offsets")
  }

  /**
   * 在offsets缓存中，查找所有 key <= time - delay 的offsets，并按序提交。
   *
   * @param time  当前批次的生成时间。
   * @param delay 提交offsets的延后时间。
   */
  def updateOffsetsByTime(time: Long, delay: Long = kafkaConf.offsetsStorageUpdateDelay): Unit = this.synchronized {
    while (offsetsCache.nonEmpty && offsetsCache.head._1 <= time - delay) {
      val head = offsetsCache.head
      logInfo(s"Start update offsets by:time -> $time, delay -> $delay, offsets -> $head")
      updateOffsets(head._2)
      offsetsCache -= head._1
      logInfo(s"Offsets updated by:time -> $time, delay -> $delay -> $head")
    }
  }

  /**
   * 对给定kafka流处理对应批次的offsets。
   */
  def mapOffsets[T: ClassTag](stream: DStream[T], f: (Time, Array[OffsetRange]) => Unit): DStream[T] = {
    stream.transform { (rdd, time) =>
      f(time, rdd.asInstanceOf[HasOffsetRanges].offsetRanges)
      rdd
    }
  }

  /**
   * 使用给定offsets创建流。
   *
   * @param reset      是否对给定的offsets进行检查重置。
   * @param autoUpdate 是否在每批次执行完毕后自动提交offsets，启用此参数时会忽略cache参数，强制缓存offsets并在offsets提交后清理对应缓存。
   *                   如配置为false，则提交offsets时需要调用[[updateOffsetsByTime]]。
   *                   默认读取配置中的[[POTATO_KAFKA_OFFSETS_STORAGE_AUTO_UPDATE_KEY]]决定。
   *                   该功能又[[OffsetsUpdateListener]]实现。
   * @param cache      是否将每批次的offsets缓存到OffsetsManager，缓存offsets可用于控制提交。
   *                   默认读取配置中的[[POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY]]决定。
   */
  def createDStream[K, V](
                           ssc: StreamingContext,
                           offsets: Map[TopicPartition, Long] = committedOffsets(),
                           reset: Boolean = kafkaConf.offsetResetEnable,
                           autoUpdate: Boolean = kafkaConf.offsetsStorageAutoUpdate,
                           cache: Boolean = false
                         ): DStream[ConsumerRecord[K, V]] = this.synchronized {
    assert(offsets.nonEmpty, "No partition assigned.")
    if (dStreamCreated.get()) throw new PotatoMethodException(s"DStream not allowed to create twice.")
    dStreamCreated.set(true)
    val stream = KafkaUtils.createDirectStream[K, V](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[K, V](offsets.keySet, kafkaConf.consumerProps, {
        if (reset)
          KafkaConsumerOffsetsUtil.validatedOffsets(kafkaConf.toConsumerProperties, offsets, reset = true)
        else
          offsets
      })
    )
    if (autoUpdate) {
      ssc.addStreamingListener(new OffsetsUpdateListener(this))
    }
    if (autoUpdate || cache)
      mapOffsets(stream, (time, offsets) => cacheOffsets(time.milliseconds, offsets))
    else
      stream
  }
}
