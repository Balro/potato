package spark.potato.common.conf

import java.util.Properties

import org.apache.spark.SparkConf

/**
 * Properties操作工具。
 */
object PropertiesUtil {
  def mapToProperties(map: Map[String, String]): Properties = {
    import scala.collection.JavaConversions.propertiesAsScalaMap
    val props = new Properties()
    props ++= map
    props
  }

  def confToProperties(conf: SparkConf, prefix: String): Properties = {
    mapToProperties(conf.getAllWithPrefix(prefix).toMap)
  }
}
