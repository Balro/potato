package spark.potato.kafka.source

import kafka.serializer.Decoder
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import spark.potato.kafka.offsets.listener.OffsetsUpdateListener
import spark.potato.kafka.offsets.manager
import spark.potato.kafka.offsets.manager.OffsetsManagerConf

import scala.reflect.ClassTag

/**
 * 创建kafkaDStream的工具类。
 */
object KafkaSourceUtil extends Logging {
  def createDStreamWithOffsetsManager[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag, R: ClassTag
  ](ssc: StreamingContext, kafkaParams: Map[String, String] = Map.empty)(
    messageHandler: MessageAndMetadata[K, V] => R)
  : (DStream[R], OffsetsManager) = {
    val offsetsManagerConf = new OffsetsManagerConf(ssc.sparkContext.getConf, kafkaParams)

    val offsetsManager = new manager.OffsetsManager(offsetsManagerConf)

    // 是否启用offsets自动提交。
    if (offsetsManagerConf.offsetsAutoUpdate)
      ssc.addStreamingListener(new OffsetsUpdateListener(offsetsManager))
    val stream: InputDStream[R] = KafkaUtils.createDirectStream[K, V, KD, VD, R](ssc, offsetsManagerConf.consumerConfigs,
      offsetsManager.committedOffsets(), messageHandler)

    stream.transform((rdd, time) => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetsManager.cacheOffsets(time.milliseconds, offsetRanges)
      rdd
    }) -> offsetsManager
  }
}
