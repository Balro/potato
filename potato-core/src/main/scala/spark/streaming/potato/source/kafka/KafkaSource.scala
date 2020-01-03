package spark.streaming.potato.source.kafka

import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import spark.streaming.potato.conf.PotatoConfKeys
import spark.streaming.potato.source.kafka.offsets.{OffsetsManager, OffsetsManagerConf, OffsetsUpdateListener}

import scala.reflect.ClassTag

object KafkaSource extends Logging {
  def createDStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag, R: ClassTag
  ](ssc: StreamingContext, kafkaParams: Map[String, String] = Map.empty, messageHandler: MessageAndMetadata[K, V] => R
   ): (DStream[R], OffsetsManager) = {
    val offsetsManagerConf = new OffsetsManagerConf(ssc.sparkContext.getConf.getAll.toMap, kafkaParams)

    val offsetsManager = new OffsetsManager(offsetsManagerConf)

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

  def defaultDStream(ssc: StreamingContext, kafkaParams: Map[String, String] = Map.empty
                    ): (DStream[(String, String)], OffsetsManager) = {
    createDStream[String, String, StringDecoder, StringDecoder, (String, String)](
      ssc, kafkaParams, defaultMessageHandler)
  }

  def defaultMessageHandler[K, V]: MessageAndMetadata[K, V] => (K, V) = {
    mmd: MessageAndMetadata[K, V] => (mmd.key, mmd.message)
  }
}

object KafkaSourceConstants {
  val KAFKA_SOURCE_PREFIX: String = PotatoConfKeys.POTATO_SOURCE_PREFIX + "kafka."
}
