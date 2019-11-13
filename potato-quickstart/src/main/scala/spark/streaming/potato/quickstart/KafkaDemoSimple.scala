package spark.streaming.potato.quickstart

import java.util.concurrent.TimeUnit

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDemoSimple extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[6]").setAppName("haha")
    val ssc = new StreamingContext(conf, Seconds(10))

    val props = Map(
      "bootstrap.servers" -> "test01:9092",
      //      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      //      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> "true"
    )
    val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, props, Set("test_topic"))

    stream.foreachRDD(rdd => rdd.foreach(r => println(r._2)))

    ssc.start()
    TimeUnit.SECONDS.sleep(10)
    1 to 10 foreach (f => println("hello world"))

    ssc.awaitTermination()
    1 to 10 foreach (f => println("hello world"))
  }
}
