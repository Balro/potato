package spark.streaming.potato.template

import org.apache.spark.internal.Logging
import spark.streaming.potato.conf.PotatoConfKeys
import spark.streaming.potato.source.kafka.KafkaSource
import spark.streaming.potato.source.kafka.offsets.OffsetsManagerConf

object KafkaTopn2Test extends KafkaSourceTemplate(KafkaSource.valueDStream) with Logging {
  override def doWork(args: Array[String]): Unit = {
    stream.foreachRDD { rdd =>
      rdd.top(10).foreach(println)
    }
  }

  override def initConf(args: Array[String]): Unit = {
    super.initConf(args)
    conf.setMaster("local[10]").setAppName("test")
    conf.set(PotatoConfKeys.POTATO_STREAMING_SLIDE_DURATION_SECONDS_KEY, "20")
    conf.set(OffsetsManagerConf.OFFSETS_STORAGE_KEY, "zookeeper")
    conf.set(OffsetsManagerConf.OFFSET_RESET_POLICY, "earliest")
    conf.set(OffsetsManagerConf.SUBSCRIBE_TOPICS_KEY, "test")
    conf.set(OffsetsManagerConf.BOOTSTRAP_SERVERS_KEY, "test01:9092,test02:9092")
    conf.set(OffsetsManagerConf.GROUP_ID_KEY, "kafka_print_test")
  }
}
