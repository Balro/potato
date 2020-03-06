package spark.potato.template

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spark.potato.common.conf.CommonConfigKeys._
import spark.potato.kafka.conf.KafkaConfigKeys._
import spark.potato.kafka.offsets.manager.OffsetsManager
import spark.potato.kafka.source.KafkaSourceUtil
import spark.potato.lock.conf.LockConfigKeys._
import spark.potato.template.streaming.KafkaSourceTemplate

object KafkaTopn1Test extends KafkaSourceTemplate[(String, String)] with Logging {
  override def initKafka(ssc: StreamingContext): (DStream[(String, String)], OffsetsManager) =
    KafkaSourceUtil.kvDStream(ssc)

  override def doWork(args: Array[String]): Unit = {
    getStream.foreachRDD { rdd =>
      rdd.map(_._2).top(10).foreach(println)
    }
  }

  override def afterConfCreated(args: Array[String],conf:SparkConf): Unit = {
    super.afterConfCreated(args,conf)
    conf.setMaster("local[10]").setAppName("test")
    conf.set(POTATO_STREAMING_BATCH_DURATION_SECONDS_KEY, "20")
    conf.set(KAFKA_OFFSETS_STORAGE_KEY, "zookeeper")
    conf.set(KAFKA_CONSUMER_OFFSET_RESET_POLICY, "earliest")
    conf.set(KAFKA_SUBSCRIBE_TOPICS_KEY, "test")
    conf.set(KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY, "test01:9092,test02:9092")
    conf.set(KAFKA_CONSUMER_GROUP_ID_KEY, "kafka_print_test")

    conf.set(POTATO_RUNNING_LOCK_ENABLE_KEY, "true")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY, "test02:2181")
    conf.set(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, "/potato/lock/test")
  }
}
