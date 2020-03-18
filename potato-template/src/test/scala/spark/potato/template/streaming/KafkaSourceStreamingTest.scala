package spark.potato.template.streaming

import org.apache.spark.SparkConf
import org.junit.Test
import spark.potato.common.conf._
import spark.potato.common.util.LocalLauncherUtil
import spark.potato.kafka.conf._
import spark.potato.kafka.source._
import spark.potato.lock.conf._
import spark.potato.lock.running.StreamingRunningLockService
import spark.potato.monitor.backlog.BacklogMonitorService
import spark.potato.monitor.conf._

object KafkaSourceStreamingTest extends StreamingTemplate {
  /**
   * 业务逻辑。
   */
  override def doWork(): Unit = {
    val ssc = createStreamingContext()
    val (source, offsetsManager) = createDStreamWithOffsetsManager[String, String, StringDecoder, StringDecoder, String](
      ssc)(_.message())

    source.foreachRDD { (rdd, time) =>
      println(rdd.take(10).mkString("\n"))
      offsetsManager.updateOffsetsByDelay(time.milliseconds)
    }

    start(ssc)
  }

  override def createConf(): SparkConf = {
    super.createConf()
      .set(POTATO_COMMON_STREAMING_BATCH_DURATION_MS_KEY, 5000.toString)
      .set(POTATO_COMMON_ADDITIONAL_SERVICES_KEY,
        Seq(
          classOf[BacklogMonitorService],
          classOf[StreamingRunningLockService]
        ).map(_.getName).mkString(","))

      // backlog monitor
      .set(POTATO_MONITOR_BACKLOG_DELAY_MS_KEY, "1")
      .set(POTATO_MONITOR_BACKLOG_REPORTER_INTERVAL_MS_KEY, "60000")
      .set(POTATO_MONITOR_BACKLOG_REPORTER_MAX_KEY, "60")
      .set(POTATO_MONITOR_BACKLOG_REPORTER_DING_TOKEN_KEY, "https://oapi.dingtalk.com/robot/send?access_token=2aa713587501102395004b0f87650cc5509b0d99af25868921d6509020785483")

      // running lock
      .set(POTATO_RUNNING_LOCK_ZOOKEEPER_QUORUM_KEY, "test01:2181")
      .set(POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY, "/potato/lock/test")
      .set(POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_KEY, "90000")
      .set(POTATO_RUNNING_LOCK_TRY_INTERVAL_MS_KEY, "5000")
      .set(POTATO_RUNNING_LOCK_HEARTBEAT_INTERVAL_MS_KEY, "5000")

      // kafka source
      .set(POTATO_KAFKA_OFFSETS_STORAGE_KEY, "kafka")
      .set(POTATO_KAFKA_SOURCE_SUBSCRIBE_TOPICS_KEY, "test1,test2")
      .set(POTATO_KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY, "test02:9092")
      .set(POTATO_KAFKA_CONSUMER_GROUP_ID_KEY, "potato_test_group")
      .set(POTATO_KAFKA_CONSUMER_OFFSET_RESET_KEY, "largest")
  }
}

class KafkaSourceStreamingTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.test(KafkaSourceStreamingTest)
  }
}