package spark.potato.monitor.reporter

import org.apache.spark.internal.Logging
import spark.potato.monitor.notify.Notify
import spark.potato.monitor.conf._

class MaxNumReporter(conf: Map[String, String], notify: Notify) extends Reporter with Logging {
  var curNum = 0
  val maxNum: Int = conf.get(POTATO_MONITOR_BACKLOG_REPORTER_MAX_KEY) match {
    case Some(num) => num.toInt
    case None => POTATO_MONITOR_BACKLOG_REPORTER_MAX_DEFAULT
  }
  var lastTime = 0L
  val minInterval: Long = conf.get(POTATO_MONITOR_BACKLOG_REPORTER_INTERVAL_MS_KEY) match {
    case Some(num) => num.toLong
    case None => POTATO_MONITOR_BACKLOG_REPORTER_INTERVAL_MS_DEFAULT
  }

  override def problem(msg: String): Unit = {
    val curTime = System.currentTimeMillis()
    val curInterval = curTime - lastTime
    // 如果未到达最大上报次数，且距上次上报事件达到上报间隔。
    if (curNum < maxNum && curInterval > minInterval) {
      notify.notify(msg + s"\n\n当前告警次数:$curNum/$maxNum")
      curNum += 1
      lastTime = curTime
      logWarning(s"Report message[$curNum/$maxNum]: problem -> $msg .")
    } else logInfo(s"Suppression message: problem -> $msg, because num: $curNum/$maxNum or interval $curInterval/$minInterval .")
  }

  override def recover(msg: String): Unit = {
    if (curNum > 0) {
      notify.notify(msg)
      curNum = 0
      lastTime = System.currentTimeMillis()
      logInfo(s"Report message: recover -> $msg")
    }
  }
}
