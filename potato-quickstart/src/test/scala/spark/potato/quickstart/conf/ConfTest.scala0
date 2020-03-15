package spark.potato.quickstart.conf

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
      POTATO_SUBMIT_BIN_KEY,
      POTATO_SUBMIT_MAIN_CLASS_KEY,
      POTATO_SUBMIT_MAIN_JAR_KEY
    ).filter(!conf.contains(_))

    assert(escape.isEmpty, escape)
  }
}
