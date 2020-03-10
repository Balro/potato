package spark.potato.monitor.reporter

import org.junit.Test
import spark.potato.monitor.conf._

class DingReporterTest {
  @Test
  def reportTest(): Unit = {
    val reporter = new DingReporter(Map(
      POTATO_MONITOR_BACKLOG_REPORTER_DING_TOKEN_KEY -> "abc"
    ))
    reporter.report("hello")
  }
}
