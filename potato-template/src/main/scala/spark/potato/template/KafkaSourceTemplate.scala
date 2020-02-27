package spark.potato.template

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spark.potato.kafka.offsets.manager.OffsetsManager
import spark.potato.kafka.source._

abstract class KafkaSourceTemplate[E] extends GeneralTemplate {
  private var stream: DStream[E] = _
  private var offsetsManager: OffsetsManager = _

  def initKafka(ssc: StreamingContext): (DStream[E], OffsetsManager)

  def getStream: DStream[E] = stream

  def getOffsetsManager: OffsetsManager = offsetsManager

  override def afterContextCreated(args: Array[String]): Unit = {
    super.afterContextCreated(args)
    val (s, o) = initKafka(getSsc)
    stream = s
    offsetsManager = o
  }
}
