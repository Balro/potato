package potato.demo.streaming

import potato.kafka010.source._
import potato.spark.template._

object KafkaSourceStreamingDemo extends FullTemplate {

  override def main(args: Array[String]): Unit = {
    val ssc = createSSC().withService.stopWhenShutdown
    val source = createDStream[String, String](ssc, Map.empty[String, String])
    source.map(_.value).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
