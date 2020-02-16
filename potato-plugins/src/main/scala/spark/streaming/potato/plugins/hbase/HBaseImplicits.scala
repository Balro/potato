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
}
