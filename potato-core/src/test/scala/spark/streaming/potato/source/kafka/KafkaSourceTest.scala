package spark.streaming.potato.source.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

class KafkaSourceTest {
  @Test
  def test0(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    conf.setAll(Map(
      "spark.potato.source.kafka.offsets.storage" -> "kafka",
      "spark.potato.source.kafka.subscribe.topics" -> "test",
      "spark.potato.source.kafka.offsets.auto.update" -> "true"
    ))

    val kafkaParams = Map(
      "bootstrap.servers" -> "test01:9092",
      "group.id" -> "ks_test_kafkao",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> "true"
    )

    val ssc = new StreamingContext(conf, Seconds(10))

    val (stream, manager) = KafkaSource.defaultDStream(ssc, kafkaParams)

    stream.foreachRDD { rdd =>
      rdd.take(10).foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
