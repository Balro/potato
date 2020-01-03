package spark.streaming.potato.template

import kafka.serializer.StringDecoder
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import spark.streaming.potato.conf.PotatoConfKeys
import spark.streaming.potato.context.PotatoContextUtil

object KafkaPrintTest extends SparkStreamingTemplate with Logging {
  override def doWork(args: Array[String]): Unit = {
    val props = Map(
      "bootstrap.servers" -> "test01:9092",
      "auto.offset.reset" -> "largest"
    )
    val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, props, Set("test_topic"))

    stream.flatMap(f => f._2.split("\\s+").map(_ -> 1)).reduceByKey(_ + _).print(10)
  }

  override def initConf(args: Array[String]): Unit = {
    conf.setMaster("local[10]").setAppName("test")
    conf.set(PotatoConfKeys.POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY, "20")
    super.initConf(args)
  }
}
