package spark.potato.template.streaming

import org.apache.spark.SparkConf
import spark.potato.kafka.source._
import spark.potato.monitor.conf._
import spark.potato.common.conf._
import spark.potato.lock.conf._
import spark.potato.template.conf._
import spark.potato.kafka.conf._
import spark.potato.lock.runninglock.RunningLockManagerService
import spark.potato.monitor.backlog.BacklogMonitorService

object KafkaSourceStreamingTemplateTest extends KafkaSourceStreamingTemplate[String, String, StringDecoder, StringDecoder, String] {
  override def metaHandler(): MessageAndMetadata[String, String] => String = valueMessageHandler

  override def doWork(): Unit = {
    source.foreachRDD { rdd =>
      rdd.foreach { f =>
        println(f)
      }
    }
  }

  override def createConf(): SparkConf = {
    new SparkConf().setMaster("local[2]").setAppName("KafkaSourceStreamingTemplateTest")
      .set(POTATO_STREAMING_BATCH_DURATION_MS_KEY, 5000.toString)
      .set(POTATO_TEMPLATE_ADDITIONAL_SERVICES_KEY,
        Seq(classOf[BacklogMonitorService], classOf[RunningLockManagerService]).map {
          _.getName
        }.mkString(","))
      .set(POTATO_MONITOR_BACKLOG_DELAY_MS_KEY, 1.toString)
      .set(POTATO_MONITOR_BACKLOG_REPORTER_DING_TOKEN_KEY, "abc")
      .set(POTATO_RUNNING_LOCK_ZOOKEEPER_ADDR_KEY, "test02")

      .set(POTATO_KAFKA_OFFSETS_STORAGE_KEY, "kafka")
      .set(POTATO_KAFKA_SUBSCRIBE_TOPICS_KEY, "test")
      .set(POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY, "test02:9092")
      .set(POTATO_KAFKA_CONSUMER_GROUP_ID_KEY, "potato_test_group")
      .set(POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY, "largest")
  }
}
