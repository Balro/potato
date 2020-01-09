package spark.streaming.potato.core.template

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spark.streaming.potato.core.source.kafka.offsets.OffsetsManager

abstract class KafkaSourceTemplate[E](f: (StreamingContext, Map[String, String]) => (DStream[E], OffsetsManager)) extends GeneralTemplate {
  var oStream: Option[DStream[E]] = None
  var oOffsetsManager: Option[OffsetsManager] = None

  def stream: DStream[E] = oStream.get

  def offsetsManager: OffsetsManager = oOffsetsManager.get

  override def afterContextCreated(args: Array[String]): Unit = {
    super.afterContextCreated(args)
    val (s, om) = f(ssc, Map.empty[String, String])
    oStream = Option(s)
    oOffsetsManager = Option(om)
  }
}
