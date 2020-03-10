package spark.potato.template.streaming

import kafka.serializer.Decoder
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.DStream
import spark.potato.kafka.source._

import scala.reflect.ClassTag

abstract class KafkaSourceStreamingTemplate[
  K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag, R: ClassTag
] extends StreamingTemplate with Logging {
  protected lazy val (source: DStream[R], offsetsManager: OffsetsManager) = createStream()

  def createStream(): (DStream[R], OffsetsManager) =
    KafkaSourceUtil.createDStreamWithOffsetsManager[K, V, KD, VD, R](ssc)(metaHandler())

  def metaHandler(): MessageAndMetadata[K, V] => R
}
