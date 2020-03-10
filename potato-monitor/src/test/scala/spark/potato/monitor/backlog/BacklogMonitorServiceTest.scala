package spark.potato.monitor.backlog

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.potato.common.service.ServiceManager
import spark.potato.monitor.conf._

import java.util.concurrent.TimeUnit

object BacklogMonitorServiceTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("BacklogMonitorTest")
    conf.set(POTATO_MONITOR_BACKLOG_DELAY_MS_KEY, "1")
    conf.set(POTATO_MONITOR_BACKLOG_REPORTER_INTERVAL_MS_KEY, "60000")
    conf.set(POTATO_MONITOR_BACKLOG_REPORTER_MAX_KEY, "60")
    conf.set(POTATO_MONITOR_BACKLOG_REPORTER_DING_TOKEN_KEY, "https://oapi.dingtalk.com/robot/send?access_token=2aa713587501102395004b0f87650cc5509b0d99af25868921d6509020785483")

    val ssc = new StreamingContext(conf, Seconds(10))
    val monitor = new ServiceManager().ssc(ssc).serve(classOf[BacklogMonitorService])
    monitor.start()

    TimeUnit.MINUTES.sleep(10)
  }
}
