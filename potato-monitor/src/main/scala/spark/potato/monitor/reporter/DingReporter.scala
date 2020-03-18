package spark.potato.monitor.reporter

import org.apache.spark.internal.Logging
import spark.potato.common.util.DingRobotUtil
import spark.potato.monitor.conf._

/**
 * 钉钉reporter，用于将信息发送给指定机器人。
 */
class DingReporter(conf: Map[String, String]) extends Reporter with Logging {
  val token: String = conf(POTATO_MONITOR_BACKLOG_REPORTER_DING_TOKEN_KEY)
  val atAll: Boolean = conf.get(POTATO_MONITOR_BACKLOG_REPORTER_DING_AT_ALL_KEY) match {
    case Some(bool) => bool.toBoolean
    case None => POTATO_MONITOR_BACKLOG_REPORTER_DING_AT_ALL_DEFAULT
  }
  val atPhones: Array[String] = conf.get(POTATO_MONITOR_BACKLOG_REPORTER_DING_AT_PHONES_KEY) match {
    case Some(phones) => phones.split(",")
    case None => POTATO_MONITOR_BACKLOG_REPORTER_DING_AT_PHONES_DEFAULT
  }

  override def report(msg: String): Unit = {
    DingRobotUtil.ding(token, msg, atAll, atPhones)
  }
}
