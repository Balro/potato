package spark.potato.template.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.junit.Test
import spark.potato.common.conf._
import spark.potato.common.spark.LocalLauncherUtil
import spark.potato.common.spark.streaming.StreamingContextUtil
import spark.potato.lock.conf._
import spark.potato.lock.singleton.StreamingSingletonLockService
import spark.potato.monitor.backlog.BacklogMonitorService
import spark.potato.monitor.conf._

import scala.collection.mutable

object StreamingTemplateTest extends StreamingTemplate {
  /**
   * 业务逻辑。
   */
  override def doWork(): Unit = {
    val ssc = createStreamingContext(durMS = 5000)

    val source = ssc.queueStream(queue)
    source.print()

    start(ssc)
  }

  private val queue = mutable.Queue.empty[RDD[String]]

  override def afterStart(ssc: StreamingContext): Unit = {
    while (!ssc.sparkContext.isStopped) {
      queue += ssc.sparkContext.makeRDD(Seq(new Date().toString))
      TimeUnit.MILLISECONDS.sleep(StreamingContextUtil.getBatchDuration(ssc).milliseconds)
    }
  }

  override def createConf(): SparkConf = {
    super.createConf()
      // additional service
      .set(POTATO_COMMON_ADDITIONAL_SERVICES_KEY,
        Seq(
          POTATO_MONITOR_BACKLOG_SERVICE_NAME,
          POTATO_LOCK_SINGLETON_STREAMING_SERVICE_NAME
        ).mkString(","))

      //      .set(POTATO_COMMON_CUSTOM_SERVICES_CLASS_KEY,
      //        Seq(
      //          classOf[BacklogMonitorService],
      //          classOf[StreamingRunningLockService]
      //        ).map(_.getName).mkString(","))

      // backlog monitor
      .set(POTATO_MONITOR_BACKLOG_THRESHOLD_MS_KEY, "1")
      .set(POTATO_MONITOR_BACKLOG_REPORTER_INTERVAL_MS_KEY, "60000")
      .set(POTATO_MONITOR_BACKLOG_REPORTER_MAX_KEY, "60")
      .set(POTATO_MONITOR_BACKLOG_NOTIFY_DING_TOKEN_KEY, "abc")

      // running lock
      .set(POTATO_LOCK_SINGLETON_ZOOKEEPER_QUORUM_KEY, "test01:2181")
      .set(POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_KEY, "/potato/lock/test")
      .set(POTATO_LOCK_SINGLETON_HEARTBEAT_TIMEOUT_MS_KEY, "90000")
      .set(POTATO_LOCK_SINGLETON_TRY_INTERVAL_MS_KEY, "5000")
      .set(POTATO_LOCK_SINGLETON_HEARTBEAT_INTERVAL_MS_KEY, "5000")
  }
}

class StreamingTemplateTest {
  @Test
  def local(): Unit = {
    LocalLauncherUtil.localTest(StreamingTemplateTest)
  }
}
