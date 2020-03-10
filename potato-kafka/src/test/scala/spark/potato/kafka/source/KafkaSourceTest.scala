package spark.potato.kafka.source

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test
import spark.potato.kafka.conf._

class KafkaSourceTest {
  @Test
  def test0(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    conf.setAll(Map(
      POTATO_KAFKA_OFFSETS_STORAGE_KEY -> "kafka",
      POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY -> "test",
      POTATO_KAFKA_OFFSETS_AUTO_UPDATE_KEY -> "true"
    ))

    val kafkaParams = Map(
      "bootstrap.servers" -> "test01:9092",
      "group.id" -> "ks_test_kafka",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> "true"
    )

    val ssc = new StreamingContext(conf, Seconds(10))

    val (stream, _) = createDStreamWithOffsetsManager[String, String, StringDecoder, StringDecoder, String](ssc, kafkaParams)(valueMessageHandler)

    val a = ssc.sparkContext.longAccumulator("test")

    stream.foreachRDD { rdd =>
      rdd.take(10).foreach(println)
      a.add(1L)
      println(s"-------- ${a.sum} --------")
      //      if (a.sum > 3)
      //        throw new Exception("batch is over than 3")
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
