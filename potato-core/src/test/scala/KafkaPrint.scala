import kafka.serializer.StringDecoder
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import spark.streaming.potato.template.SparkStreamingTemplate

object KafkaPrint extends SparkStreamingTemplate with Logging {
  override def process(args: Array[String]): Unit = {
    val props = Map(
      "bootstrap.servers" -> "test01:9092",
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> "true"
    )
    val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, props, Set("test_topic"))

    stream.flatMap(f => f._2.split("\\s+").map(_ -> 1)).reduceByKey(_ + _).print(10)
  }

  override def initConf(args: Array[String]): Unit = {
    conf.setMaster("local[10]").setAppName("test")
    super.initConf(args)
  }
}
