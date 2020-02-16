package spark.streaming.potato.plugins.monitor.reporter

import org.apache.spark.internal.Logging
import spark.streaming.potato.common.util.DingRobot
import spark.streaming.potato.plugins.monitor.MonitorConfigKeys._

class DingReporter(conf: Map[String, String]) extends Reporter with Logging {
  val token: String = conf(BACKLOG_REPORTER_DING_TOKEN_KEY)
  val atAll: Boolean = conf.get(BACKLOG_REPORTER_DING_ATALL_KEY) match {
    case Some(bool) => bool.toBoolean
    case None => BACKLOG_REPORTER_DING_ATALL_DEFAULT
  }
  val atPhones: Array[String] = conf.get(BACKLOG_REPORTER_DING_ATPHONEs_KEY) match {
    case Some(phones) => phones.split(",")
    case None => BACKLOG_REPORTER_DING_ATPHONEs_DEFAULT
  }

  override def report(msg: String): Unit = {
    DingRobot.ding(token, msg, atAll, atPhones)
  }
}
