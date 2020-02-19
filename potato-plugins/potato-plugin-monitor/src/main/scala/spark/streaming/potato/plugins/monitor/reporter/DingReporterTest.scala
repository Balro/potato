package spark.streaming.potato.plugins.monitor.reporter

import org.junit.Test

class DingReporterTest {
  @Test
  def reportTest(): Unit = {
    val reporter = new DingReporter(Map(
      "spark.potato.monitor.backlog.reporter.ding.token" -> "abc"
    ))
    reporter.report("hello")
  }
}
