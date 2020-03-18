package spark.potato.quickstart.streaming

import spark.potato.kafka.source._
import spark.potato.template.streaming.StreamingTemplate

object KafkaSourceStreamingDemo extends StreamingTemplate {

  override def doWork(): Unit = {
    val ssc = createStreamingContext()
    val (source, _) = createDStreamWithOffsetsManager[String, String, StringDecoder, StringDecoder, String](ssc)(valueMessageHandler)
    source.print()
    start(ssc)
  }
}
