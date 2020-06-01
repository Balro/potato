package potato.common.conf

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
      POTATO_COMMON_SENDER_DING_TOKEN_KEY,
      POTATO_COMMON_SENDER_DING_AT_KEY
    ).filter(!conf.contains(_))

    assert(escape.isEmpty, escape)
  }
}
