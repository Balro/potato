package potato.hbase.sink

import org.apache.hadoop.hbase.client.{ConnectionConfiguration, Mutation}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import potato.hadoop.conf.SerializedConfiguration
import spark.potato.hbase.table.TableUtil.{withBufferedSinkTable, withMutator}

/**
 * hbase sink 工具类。
 */
object HBaseSinkUtil extends Logging {
  /**
   * rdd写入hbase，非线程安全。
   *
   * @param rdd        要写入的rdd。
   * @param conf       HBaseConfiguration。
   * @param table      要写入的表。
   * @param bufferSize buffer大小，单位B。
   */
  def saveToHBase(rdd: RDD[MutationAction], conf: SerializedConfiguration, table: String,
                  bufferSize: Long = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT): Unit = {
    rdd.foreachPartition { part =>
      withMutator(conf, table, bufferSize) { mutator =>
        withBufferedSinkTable(conf, table, synchronized = false, bufferSize) { btbl =>
          part.foreach {
            case MutationAction(MutationType.APPEND, mutation) =>
              btbl.sink(mutation)
            case MutationAction(MutationType.INCREMENT, mutation) =>
              btbl.sink(mutation)
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
}

case class MutationAction(action: MutationType.Type, mutation: Mutation)

object MutationType extends Enumeration {
  type Type = Value
  val APPEND, DELETE, PUT, INCREMENT = Value
}
