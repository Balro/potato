package spark.potato.hbase.sink

import org.apache.hadoop.hbase.client.{ConnectionConfiguration, Mutation}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import spark.potato.hbase.SerializableConfiguration
import spark.potato.hbase.TableUtil._

object HBaseSinkUtil extends Logging {
  def saveToHBase(rdd: RDD[MutationAction], conf: SerializableConfiguration, table: String,
                  bufferSize: Long = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT): Unit = {
    rdd.foreachPartition { part =>
      withMutator(conf, table, bufferSize) { mutator =>
        withBufferedSinkTable(conf, table, bufferSize) { btbl =>
          part.foreach {
            case MutationAction(MutationType.APPEND, mutation) =>
              btbl.add(mutation)
            case MutationAction(MutationType.INCREMENT, mutation) =>
              btbl.add(mutation)
            case MutationAction(MutationType.DELETE, mutation) =>
              mutator.mutate(mutation)
            case MutationAction(MutationType.PUT, mutation) =>
              mutator.mutate(mutation)
            case m: MutationAction =>
              logWarning(s"Uknown mutation $m")
          }
        }
      }
    }
  }

  def saveToHBase(stream: DStream[MutationAction], conf: SerializableConfiguration, table: String, bufferSize: Long): Unit = {
    stream.foreachRDD { rdd =>
      saveToHBase(rdd, conf, table, bufferSize)
    }
  }

  val a = new Bytes

}

case class MutationAction(action: MutationType.Type, mutation: Mutation)

object MutationType extends Enumeration {
  type Type = Value
  val APPEND, DELETE, PUT, INCREMENT = Value
}
