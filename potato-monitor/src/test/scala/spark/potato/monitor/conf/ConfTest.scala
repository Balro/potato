package spark.potato.monitor.conf

import java.util.Properties

import org.apache.spark.SparkConf
import org.junit.Test

class ConfTest {
  @Test
  def keyTest(): Unit = {
    val props = new Properties()
    props.load(this.getClass.getResourceAsStream("/template.properties"))
    System.setProperties(props)
    val conf = new SparkConf()

    val escape = Seq(
      POTATO_MONITOR_BACKLOG_DELAY_MS_KEY,
      POTATO_MONITOR_BACKLOG_REPORTER_INTERVAL_MS_KEY,
      POTATO_MONITOR_BACKLOG_REPORTER_MAX_KEY,
      POTATO_MONITOR_BACKLOG_REPORTER_TYPE_KEY,
      POTATO_MONITOR_BACKLOG_REPORTER_DING_TOKEN_KEY,
      POTATO_MONITOR_BACKLOG_REPORTER_DING_AT_ALL_KEY,
      POTATO_MONITOR_BACKLOG_REPORTER_DING_AT_PHONES_KEY
    ).filter(!conf.contains(_))

    assert(escape.isEmpty, escape)
  }
}
