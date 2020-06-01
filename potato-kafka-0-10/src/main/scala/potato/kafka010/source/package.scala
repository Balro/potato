package potato.kafka010

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import spark.potato.kafka.offsets.listener.OffsetsUpdateListener
import spark.potato.kafka.offsets.manager.OffsetsManager

import scala.reflect.ClassTag

/**
 * 创建kafkaDStream的工具类。
 */
package object source extends Logging {
  /**
   * @param kafkaParams 添加到kafka的额外参数。
   */
  def createDStream[K, V](ssc: StreamingContext, kafkaParams: Map[String, String] = Map.empty): DStream[ConsumerRecord[K, V]] = {
    val offsetsManager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams)

    val subscribePartitions = offsetsManager.committedOffsets()

    KafkaUtils.createDirectStream[K, V](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[K, V](subscribePartitions.keys, offsetsManager.kafkaConf.consumerProps, subscribePartitions)
    )
  }

  /**
   * 创建附带offsets manager的kafka流。
   * 打开自动提交[[spark.potato.kafka.conf.POTATO_KAFKA_OFFSETS_STORAGE_AUTO_UPDATE_KEY]]可以在每批次流处理结束后自动提交offset。
   * 如未设置自动提交，则需要在每次流结束后手动调用[[OffsetsManager.updateOffsets]]方法。
   *
   * @param kafkaParams 添加到kafka的额外参数。
   * @return (DStream,OffsetsManager)
   */
  def createDStreamWithOffsetsManager[K, V](ssc: StreamingContext, kafkaParams: Map[String, String] = Map.empty): (DStream[ConsumerRecord[K, V]], OffsetsManager) = {
    val offsetsManager = new OffsetsManager(ssc.sparkContext.getConf, kafkaParams)

    // 是否启用offsets自动提交。
    if (offsetsManager.kafkaConf.offsetsStorageAutoUpdate)
      ssc.addStreamingListener(new OffsetsUpdateListener(offsetsManager))

    val subscribePartitions = offsetsManager.committedOffsets()

    KafkaUtils.createDirectStream[K, V](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[K, V](subscribePartitions.keys, offsetsManager.kafkaConf.consumerProps, subscribePartitions)
    ).transform((rdd, time) => { // 在后续每次对stream进行操作时，将当前stream的offsetrange存储缓存。
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetsManager.cacheOffsets(time.milliseconds, offsetRanges)
      rdd
    }) -> offsetsManager
  }
}
