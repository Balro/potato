package spark.potato.template.streaming

import kafka.serializer.Decoder
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.DStream
import spark.potato.kafka.source._

import scala.reflect.ClassTag

/**
 * 在StreamingTemplate上封装了KafkaSource。泛型参数对应Spark的KafkaUtils.createDirectStream方法。
 */
abstract class KafkaSourceStreamingTemplate[
  K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag, R: ClassTag
] extends StreamingTemplate with Logging {
  /**
   * source: 初始化后的DStream。offsetsManager: kafka offsets管理工具。
   */
  protected lazy val (source: DStream[R], offsetsManager: OffsetsManager) = createStream()

  /**
   * 初始化kafka source。
   */
  def createStream(): (DStream[R], OffsetsManager) =
    KafkaSourceUtil.createDStreamWithOffsetsManager[K, V, KD, VD, R](ssc)(metaHandler)

  /**
   * kafka source处理函数，需要将MessageAndMetadata解析为可用数据。
   */
  def metaHandler: MessageAndMetadata[K, V] => R
}
