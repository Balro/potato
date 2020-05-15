package streaming

import spark.potato.kafka.source.{StringDecoder, createDStreamWithOffsetsManager, valueMessageHandler}
import spark.potato.template.streaming.StreamingTemplate

object KafkaSourceStreamingDemo extends StreamingTemplate {

  override def doWork(): Unit = {
    val ssc = createStreamingContext()
    val (source, _) = createDStreamWithOffsetsManager[String, String, StringDecoder, StringDecoder, String](ssc)(valueMessageHandler)
    source.print()
    start(ssc)
  }
}
