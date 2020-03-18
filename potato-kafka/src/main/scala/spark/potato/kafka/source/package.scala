package spark.potato.kafka

import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * 提供kafkaSource的快速隐式转换。
 */
package object source {
  type OffsetsManager = spark.potato.kafka.offsets.manager.OffsetsManager
  type StringDecoder = kafka.serializer.StringDecoder
  type MessageAndMetadata[K, V] = kafka.message.MessageAndMetadata[K, V]

  def createDStreamWithOffsetsManager[
    K: ClassTag, V: ClassTag,
    KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag,
    R: ClassTag
  ](ssc: StreamingContext, kafkaParams: Map[String, String] = Map.empty)(
    messageHandler: MessageAndMetadata[K, V] => R): (DStream[R], OffsetsManager) =
    KafkaSourceUtil.createDStreamWithOffsetsManager[K, V, KD, VD, R](ssc, kafkaParams)(messageHandler)

  /**
   * 将MessageAndMetadata进行全解析，返回(topic,partition,offset,key,value)。
   */
  def fullMessageHandler[K, V]: MessageAndMetadata[K, V] => (String, Int, Long, K, V) = {
    mmd: MessageAndMetadata[K, V] => (mmd.topic, mmd.partition, mmd.offset, mmd.key, mmd.message)
  }

  /**
   * 将MessageAndMetadata解析为 MessageAndMetadata.key -> MessageAndMetadata.value。
   */
  def kvMessageHandler[K, V]: MessageAndMetadata[K, V] => (K, V) = {
    mmd: MessageAndMetadata[K, V] => (mmd.key, mmd.message)
  }

  /**
   * 将MessageAndMetadata解析为 MessageAndMetadata.value。
   */
  def valueMessageHandler[K, V]: MessageAndMetadata[K, V] => V = {
    mmd: MessageAndMetadata[K, V] => mmd.message
  }
}
