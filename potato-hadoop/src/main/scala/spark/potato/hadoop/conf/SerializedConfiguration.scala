package spark.potato.hadoop.conf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

/**
 * 包装 org.apache.hadoop.conf.Configuration 提供序列化与反序列化能力。
 **/
class SerializedConfiguration private(val conf: Map[String, String]) extends Serializable

/**
 * 提供与 org.apache.hadoop.conf.Configuration, Map[String,String] 的隐式转换。
 */
object SerializedConfiguration {
  def apply(conf: Configuration): SerializedConfiguration = {
    import scala.collection.JavaConversions.iterableAsScalaIterable
    new SerializedConfiguration(conf.map { entry =>
      entry.getKey -> entry.getValue
    }.toMap)
  }

  def unapply(ser: SerializedConfiguration): Option[Configuration] = {
    val conf = new Configuration(false)
    ser.conf.foreach { kv => conf.set(kv._1, kv._2) }
    Option(conf)
  }

  def readSparkConf(conf: SparkConf, prefix: String): SerializedConfiguration = {
    mapToSerializableConfiguration(conf.getAllWithPrefix(prefix).toMap)
  }

  implicit def configurationToSerialized(conf: Configuration): SerializedConfiguration = {
    SerializedConfiguration(conf)
  }

  implicit def configurationUnSerialized(ser: SerializedConfiguration): Configuration = {
    val SerializedConfiguration(conf) = ser
    conf
  }

  implicit def mapToConfiguration(map: Map[String, String]): Configuration = {
    val conf = new Configuration()
    map.foreach { kv =>
      conf.set(kv._1, kv._2)
    }
    conf
  }

  implicit def mapToSerializableConfiguration(map: Map[String, String]): SerializedConfiguration = {
    SerializedConfiguration(mapToConfiguration(map))
  }
}
