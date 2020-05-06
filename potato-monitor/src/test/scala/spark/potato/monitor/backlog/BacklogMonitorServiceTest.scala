package spark.potato.monitor.backlog

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.potato.common.service.ServiceManager
import spark.potato.monitor.conf._
import java.util.concurrent.TimeUnit

object BacklogMonitorServiceTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("BacklogMonitorTest")
    conf.set(POTATO_MONITOR_BACKLOG_THRESHOLD_MS_KEY, "1")
    conf.set(POTATO_MONITOR_BACKLOG_REPORTER_INTERVAL_MS_KEY, "10000")
    conf.set(POTATO_MONITOR_BACKLOG_REPORTER_MAX_KEY, "60")
    conf.set(POTATO_MONITOR_BACKLOG_NOTIFY_DING_TOKEN_KEY, "abc")

    val ssc = new StreamingContext(conf, Seconds(10))
    val monitor = new ServiceManager().ssc(ssc).registerByClass("StreamingBacklogMonitor", classOf[BacklogMonitorService])
    monitor.start()

    TimeUnit.MINUTES.sleep(10)
  }
}
