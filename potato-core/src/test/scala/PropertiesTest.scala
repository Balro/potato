import java.util.Properties

import org.apache.spark.SparkConf

import scala.collection.JavaConversions.propertiesAsScalaMap

object PropertiesTest {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("test/potato.properties"))
    System.setProperties(props)
    val conf = new SparkConf()
    println(conf.get("spark.driver.cores"))
    println(conf.get("spark.potato.main.class", "not found"))
    for ((k, v) <- conf.getAllWithPrefix("spark.potato.")) println(s"$k -> $v")
  }
}
