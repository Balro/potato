package spark.streaming.potato.template.template

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spark.streaming.potato.plugins.kafka.source.offsets.OffsetsManager

abstract class KafkaSourceTemplate[E] extends GeneralTemplate {
  var oStream: Option[DStream[E]] = None
  var oOffsetsManager: Option[OffsetsManager] = None

  def initKafka(ssc: StreamingContext): (DStream[E], OffsetsManager)

  def stream: DStream[E] = oStream.get

  def offsetsManager: OffsetsManager = oOffsetsManager.get

  override def afterContextCreated(args: Array[String]): Unit = {
    super.afterContextCreated(args)
    val (s, om) = initKafka(ssc)
    oStream = Option(s)
    oOffsetsManager = Option(om)
  }
}
