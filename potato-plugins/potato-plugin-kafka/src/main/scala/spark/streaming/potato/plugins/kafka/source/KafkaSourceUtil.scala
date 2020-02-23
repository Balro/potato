package spark.streaming.potato.plugins.kafka.source

import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import spark.streaming.potato.plugins.kafka.source.offsets.{OffsetsManagerConf, OffsetsUpdateListener}

import scala.reflect.ClassTag

object KafkaSourceUtil extends Logging {
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

  def kvDStream(ssc: StreamingContext, kafkaParams: Map[String, String] = Map.empty
               ): (DStream[(String, String)], OffsetsManager) = {
    createDStream[String, String, StringDecoder, StringDecoder, (String, String)](
      ssc, kafkaParams, kvMessageHandler)
  }

  def kvMessageHandler[K, V]: MessageAndMetadata[K, V] => (K, V) = {
    mmd: MessageAndMetadata[K, V] => (mmd.key, mmd.message)
  }

  def valueDStream(ssc: StreamingContext, kafkaParams: Map[String, String] = Map.empty
                  ): (DStream[String], OffsetsManager) = {
    createDStream[String, String, StringDecoder, StringDecoder, String](
      ssc, kafkaParams, valueMessageHandler)
  }

  def valueMessageHandler[K, V]: MessageAndMetadata[K, V] => V = {
    mmd: MessageAndMetadata[K, V] => mmd.message
  }
}
