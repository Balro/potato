package spark.streaming.potato.source.kafka.utils

import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import spark.streaming.potato.source.kafka.tools.{OffsetsManager, OffsetsManagerConfig}

import scala.reflect.ClassTag

object KafkaSourceUtil extends Logging {
  def createDStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag, R: ClassTag
  ](ssc: StreamingContext, kafkaParams: Map[String, String]
   )(implicit messageHandler: MessageAndMetadata[K, V] => R): (DStream[R], OffsetsManager) = {
    val offsetsManagerConfig = new OffsetsManagerConfig(
      ssc.sparkContext.getConf.getAllWithPrefix(KafkaSourceConstants.KAFKA_SOURCE_PREFIX).toMap ++ kafkaParams
    )
    val offsetsManager = new OffsetsManager(offsetsManagerConfig)

    val stream: InputDStream[R] = KafkaUtils.createDirectStream[K, V, KD, VD, R](ssc, offsetsManagerConfig,
      offsetsManager.getStartOffsets(), messageHandler)

    stream.transform((rdd, time) => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetsManager.cacheOffsets(time.milliseconds, offsetRanges)
      rdd
    }) -> offsetsManager
  }
}

object KafkaSourceConstants {
  val KAFKA_SOURCE_PREFIX = "spark.potato.source.kafka"
}

object KafkaSourceUtilImplicits {
  implicit def defaultMessageHandler[K, V, R](msg: MessageAndMetadata[K, V]): MessageAndMetadata[K, V] => (K, V) = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
    messageHandler
  }
}
