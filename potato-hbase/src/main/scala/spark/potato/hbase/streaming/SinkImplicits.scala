package spark.potato.hbase.streaming

import org.apache.hadoop.hbase.client.ConnectionConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import spark.potato.hadoop.conf.SerializedConfiguration
import spark.potato.hbase.util.{HBaseCommonUtil, MutationAction}

/**
 * 提供DStream落HBase的隐式转换。
 */
object SinkImplicits {
  type Put = org.apache.hadoop.hbase.client.Put
  type Append = org.apache.hadoop.hbase.client.Append
  type Delete = org.apache.hadoop.hbase.client.Delete
  type Increment = org.apache.hadoop.hbase.client.Increment

  class MutationActionRDD(rdd: RDD[MutationAction]) extends Serializable {
    def saveToHBase(conf: SerializedConfiguration, table: String, bufferSize: Long = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT): Unit = {
      HBaseCommonUtil.saveToHBase(rdd, conf, table, bufferSize)
    }
  }

  class MutationActionDStream(stream: DStream[MutationAction]) extends Serializable {
    def saveToHBase(conf: SerializedConfiguration, table: String, bufferSize: Long = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT): Unit = {
      stream.foreachRDD { rdd =>
        HBaseCommonUtil.saveToHBase(rdd, conf, table, bufferSize)
      }
    }
  }

  implicit def toMutationActionRDD(rdd: RDD[MutationAction]): MutationActionRDD = {
    new MutationActionRDD(rdd)
  }

  implicit def toMutationActionDStream(stream: DStream[MutationAction]): MutationActionDStream = {
    new MutationActionDStream(stream)
  }
}
