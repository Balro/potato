package spark.streaming.potato.plugins.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object HBaseImplicits {
  implicit def mapToConfiguration(map: Map[String, String]): Configuration = {
    val conf = HBaseConfiguration.create()
    map.foreach { kv =>
      conf.set(kv._1, kv._2)
    }
    conf
  }

  implicit def mapToSerializableConfiguration(map: Map[String, String]): SerializableConfiguration = {
    SerializableConfiguration(mapToConfiguration(map))
  }
}

class SerializableConfiguration(val conf: Map[String, String]) extends Serializable

object SerializableConfiguration {
  def apply(conf: Configuration): SerializableConfiguration = {
    import scala.collection.JavaConversions.iterableAsScalaIterable
    new SerializableConfiguration(conf.map { entry =>
      entry.getKey -> entry.getValue
    }.toMap)
  }

  def unapply(ser: SerializableConfiguration): Option[Configuration] = {
    val conf = new Configuration(false)
    ser.conf.foreach { kv => conf.set(kv._1, kv._2) }
    Option(conf)
  }

  implicit def configurationToSerialized(conf: Configuration): SerializableConfiguration = {
    SerializableConfiguration(conf)
  }

  implicit def configurationUnSerialized(ser: SerializableConfiguration): Configuration = {
    val SerializableConfiguration(conf) = ser
    conf
  }
}
