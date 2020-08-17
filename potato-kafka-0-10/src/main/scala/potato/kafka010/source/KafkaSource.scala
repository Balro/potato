package potato.kafka010.source

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import potato.kafka010.conf._
import potato.kafka010.offsets.manager.OffsetsManager

/**
 * 创建kafkaDStream的工具类。
 */
object KafkaSource extends Logging {
  /**
   * 由已提交的offsets创建流。
   *
   * @param offsets 订阅的partition信息。
   * @param reset   是否重置offsets。
   */
  def createDStream[K, V](ssc: StreamingContext, kafkaParams: Map[String, String], offsets: Map[TopicPartition, Long], reset: Boolean): DStream[ConsumerRecord[K, V]] = {
    val manager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams).subscribePartetion(offsets.keySet)
    manager.createDStream[K, V](ssc, offsets = offsets, reset = reset, autoUpdate = false)
  }

  /**
   * 由已提交的offsets创建流。
   *
   * @param tpcs  订阅的topic。
   * @param reset 是否重置offsets。
   */
  def createDStream[K, V](ssc: StreamingContext, kafkaParams: Map[String, String], tpcs: Set[String], reset: Boolean): DStream[ConsumerRecord[K, V]] = {
    val manager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams).subscribeTopic(tpcs)
    manager.createDStream[K, V](ssc, reset = reset, autoUpdate = false)
  }

  /**
   * 由已提交的offsets创建流。
   * 由配置[[POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY]]决定订阅的topic。
   *
   * @param reset 是否重置offsets。
   */
  def createDStream[K, V](ssc: StreamingContext, kafkaParams: Map[String, String], reset: Boolean): DStream[ConsumerRecord[K, V]] = {
    val manager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams)
    manager.createDStream[K, V](ssc, reset = reset, autoUpdate = false)
  }

  /**
   * 由已提交的offsets创建流。
   * 由配置[[POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY]]决定订阅的topic。
   * 由配置[[POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY]]决定是否对offsets进行重置。
   */
  def createDStream[K, V](ssc: StreamingContext, kafkaParams: Map[String, String]): DStream[ConsumerRecord[K, V]] = {
    val manager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams)
    manager.createDStream[K, V](ssc, autoUpdate = false)
  }

  /**
   * 由已提交的offsets创建流，强制缓存offsets。
   *
   * @param offsets    订阅的topic信息。
   * @param reset      否对offsets进行重置。
   * @param autoUpdate 是否缓存offsets并自动提交，启用自动提交则自动开启offsets缓存。
   */
  def createDStreamWithManager[K, V](ssc: StreamingContext, kafkaParams: Map[String, String], offsets: Map[TopicPartition, Long], reset: Boolean, autoUpdate: Boolean): (DStream[ConsumerRecord[K, V]], OffsetsManager) = {
    val manager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams).subscribePartetion(offsets.keySet)
    (manager.createDStream[K, V](ssc, offsets = offsets, reset = reset, autoUpdate = autoUpdate), manager)
  }

  /**
   * 由已提交的offsets创建流，强制缓存offsets。
   *
   * @param tpcs       订阅的topic信息。
   * @param reset      否对offsets进行重置。
   * @param autoUpdate 是否缓存offsets并自动提交，启用自动提交则自动开启offsets缓存。
   */
  def createDStreamWithManager[K, V](ssc: StreamingContext, kafkaParams: Map[String, String], tpcs: Set[String], reset: Boolean, autoUpdate: Boolean): (DStream[ConsumerRecord[K, V]], OffsetsManager) = {
    val manager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams).subscribeTopic(tpcs)
    (manager.createDStream[K, V](ssc, reset = reset, autoUpdate = autoUpdate), manager)
  }

  /**
   * 由已提交的offsets创建流，强制缓存offsets。
   * 由配置[[POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY]]获取订阅的topic信息。
   *
   * @param reset      是否对offsets进行重置。
   * @param autoUpdate 是否自动提交。
   */
  def createDStreamWithManager[K, V](ssc: StreamingContext, kafkaParams: Map[String, String], reset: Boolean, autoUpdate: Boolean): (DStream[ConsumerRecord[K, V]], OffsetsManager) = {
    val manager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams)
    (manager.createDStream[K, V](ssc, cache = true, reset = reset, autoUpdate = autoUpdate), manager)
  }

  /**
   * 由已提交的offsets创建流，强制缓存offsets。
   * 由配置[[POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY]]获取订阅的topic信息。
   * 由配置[[POTATO_KAFKA_OFFSETS_STORAGE_AUTO_UPDATE_KEY]]决定是否自动提交。
   *
   * @param reset 是否对offsets进行重置。
   */
  def createDStreamWithManager[K, V](ssc: StreamingContext, kafkaParams: Map[String, String], reset: Boolean): (DStream[ConsumerRecord[K, V]], OffsetsManager) = {
    val manager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams)
    (manager.createDStream[K, V](ssc, cache = true, reset = reset), manager)
  }

  /**
   * 由已提交的offsets创建流，强制缓存offsets。
   * 由配置[[POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY]]获取订阅的topic信息。
   * 由配置[[POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY]]决定是否对offsets进行重置。
   * 由配置[[POTATO_KAFKA_OFFSETS_STORAGE_AUTO_UPDATE_KEY]]决定是否自动提交。
   */
  def createDStreamWithManager[K, V](ssc: StreamingContext, kafkaParams: Map[String, String]): (DStream[ConsumerRecord[K, V]], OffsetsManager) = {
    val manager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams)
    (manager.createDStream[K, V](ssc, cache = true), manager)
  }
}
