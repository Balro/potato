package spark.potato.monitor.notify

import org.junit.Test
import spark.potato.monitor.conf._

class DingRobotNifyTest {
  @Test
  def reportTest(): Unit = {
    val notify = new DingRobotNotify(Map(
      POTATO_MONITOR_BACKLOG_NOTIFY_DING_TOKEN_KEY -> "abc"
    ))
    notify.notify("hello")
  }
}
