package potato.spark.conf

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
      POTATO_APP_NAME_KEY,
      POTATO_MAIN_CLASS,
      POTATO_SPARK_STREAMING_BATCH_DURATION_MS_KEY,
      POTATO_SPARK_ADDITIONAL_SERVICES_KEY,
      POTATO_LOCK_SINGLETON_TRY_MAX_KEY,
      POTATO_LOCK_SINGLETON_TRY_INTERVAL_MS_KEY,
      POTATO_LOCK_SINGLETON_FORCE_KEY,
      POTATO_LOCK_SINGLETON_TYPE_KEY,
      POTATO_LOCK_SINGLETON_ZOOKEEPER_QUORUM_KEY,
      POTATO_LOCK_SINGLETON_ZOOKEEPER_PATH_KEY
    ).filter(!conf.contains(_))

    assert(escape.isEmpty, escape)
  }
}
