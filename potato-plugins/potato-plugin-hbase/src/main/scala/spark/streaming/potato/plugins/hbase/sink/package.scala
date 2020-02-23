package spark.streaming.potato.plugins.hbase

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

package object sink {

  class MutationActionRDD(rdd: RDD[MutationAction]) extends Serializable {
    def saveToHBase(conf: SerializableConfiguration, table: String, bufferSize: Int = 1000): Unit = {
      HBaseSinkUtil.saveToHBase(rdd, conf, table, bufferSize)
    }
  }

  class MutationActionDStream(stream: DStream[MutationAction]) extends Serializable {
    def saveToHBase(conf: SerializableConfiguration, table: String, bufferSize: Int = 1000): Unit = {
      HBaseSinkUtil.saveToHBase(stream, conf, table, bufferSize)
    }
  }

  implicit def toMutationActionRDD(rdd: RDD[MutationAction]): MutationActionRDD = {
    new MutationActionRDD(rdd)
  }

  implicit def toMutationActionDStream(stream: DStream[MutationAction]): MutationActionDStream = {
    new MutationActionDStream(stream)
  }
}
