package spark.potato.common.conf

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
      POTATO_COMMON_STREAMING_BATCH_DURATION_MS_KEY,
      POTATO_COMMON_ADDITIONAL_SERVICES_KEY
    ).filter(!conf.contains(_))

    assert(escape.isEmpty, escape)
  }
}
