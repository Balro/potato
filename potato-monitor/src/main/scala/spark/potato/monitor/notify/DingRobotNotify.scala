package spark.potato.monitor.notify

import org.apache.spark.internal.Logging
import scalaj.http.HttpResponse
import spark.potato.common.notify.DingRobotUtil
import spark.potato.monitor.conf._
import org.json4s.jackson._
import org.json4s.DefaultFormats

/**
 * 钉钉reporter，用于将信息发送给指定机器人。
 */
class DingRobotNotify(conf: Map[String, String]) extends Notify with Logging {
  val token: String = conf(POTATO_MONITOR_BACKLOG_NOTIFY_DING_TOKEN_KEY)
  val atAll: Boolean = conf.get(POTATO_MONITOR_BACKLOG_NOTIFY_DING_AT_ALL_KEY) match {
    case Some(bool) => bool.toBoolean
    case None => POTATO_MONITOR_BACKLOG_NOTIFY_DING_AT_ALL_DEFAULT
  }
  val atPhones: Array[String] = conf.get(POTATO_MONITOR_BACKLOG_NOTIFY_DING_AT_PHONES_KEY) match {
    case Some(phones) => phones.split(",")
    case None => POTATO_MONITOR_BACKLOG_NOTIFY_DING_AT_PHONES_DEFAULT
  }

  override def notify(msg: String): Unit = {
    implicit val fmt: DefaultFormats.type = DefaultFormats
    val resp: HttpResponse[String] = DingRobotUtil.ding(token, msg, atAll, atPhones)
    if (resp.isSuccess && parseJson(resp.body).\("errcode").extract[String] == "0")
      logInfo(s"Notify to ding robot success, msg -> $msg .")
    else
      logError(s"Notify to ding robot failed, token -> $token, msg -> $msg, resp -> $resp")
  }
}
