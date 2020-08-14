package potato.common.conf

import java.util.Properties

import org.apache.spark.SparkConf

/**
 * Properties操作工具。提供Map和SparkConf隐式转换为Properties的方法。
 */
object PropertiesImplicits {
  implicit def mapToProperties(map: Map[String, String]): Properties = {
    import scala.collection.JavaConversions.propertiesAsScalaMap
    val props = new Properties()
    props ++= map
    props
  }

  implicit def confToProperties(conf: SparkConf, prefix: String): Properties = {
    mapToProperties(conf.getAllWithPrefix(prefix).toMap)
  }
}
