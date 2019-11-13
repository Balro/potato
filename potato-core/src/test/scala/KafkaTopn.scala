import kafka.serializer.StringDecoder
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import spark.streaming.potato.conf.ConfigKeys
import spark.streaming.potato.template.SparkStreamingTemplate

object KafkaTopn extends SparkStreamingTemplate with Logging {
  override def process(args: Array[String]): Unit = {
    val props = Map(
      "bootstrap.servers" -> "test01:9092",
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> "true"
    )
    val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, props, Set("test_topic"))
    stream.flatMap(_._2.split("\\s+").map(_.toInt))
      .transform(rdd => ssc.sparkContext.makeRDD(rdd.top(10))).repartition(1)
      .foreachRDD(rdd => rdd.top(10).foreach(println))
  }

  override def initConf(args: Array[String]): Unit = {
    conf.setMaster("local[10]").setAppName("test")
      .set(ConfigKeys.POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY, 10.toString)
    super.initConf(args)
  }
}
