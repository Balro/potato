package spark.streaming.potato.plugins.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.rdd.RDD
import spark.streaming.potato.plugins.hbase.sink.{MutationAction, MutationRdd}

object HBaseImplicits {
  implicit def toMutationRDD(rdd: RDD[MutationAction]): MutationRdd = {
    new MutationRdd(rdd)
  }

  implicit def mapToConfiguration(map: Map[String, String]): Configuration = {
    val conf = HBaseConfiguration.create()
    map.foreach { kv =>
      conf.set(kv._1, kv._2)
    }
    conf
  }
}
