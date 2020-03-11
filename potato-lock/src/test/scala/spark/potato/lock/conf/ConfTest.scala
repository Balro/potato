package spark.potato.lock.conf

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
      POTATO_RUNNING_LOCK_TRY_MAX_KEY,
      POTATO_RUNNING_LOCK_TRY_INTERVAL_MS_KEY,
      POTATO_RUNNING_LOCK_FORCE_KEY,
      POTATO_RUNNING_LOCK_HEARTBEAT_INTERVAL_MS_KEY,
      POTATO_RUNNING_LOCK_HEARTBEAT_TIMEOUT_MS_KEY,
      POTATO_RUNNING_LOCK_TYPE_KEY,
      POTATO_RUNNING_LOCK_ZOOKEEPER_QUORUM_KEY,
      POTATO_RUNNING_LOCK_ZOOKEEPER_PATH_KEY
    ).filter(!conf.contains(_))

    assert(escape.isEmpty, escape)
  }
}
