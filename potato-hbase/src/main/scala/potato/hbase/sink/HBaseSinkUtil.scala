package potato.hbase.sink

import org.apache.hadoop.hbase.client.{ConnectionConfiguration, Mutation}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import potato.hadoop.conf.SerializedConfiguration
import potato.hbase.table.TableUtil.{withBufferedSinkTable, withMutator}

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
  def saveToHBase(rdd: RDD[MutationAction], conf: SerializedConfiguration, table: String = null,
                  bufferSize: Long = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT): Unit = {
    rdd.foreachPartition { part =>
      if (table != null) {
        withMutator(conf, table, bufferSize) { mutator =>
          withBufferedSinkTable(conf, table, synchronized = false, bufferSize) { btbl =>
            part.foreach { f =>
              f.action match {
                case MutationType.APPEND =>
                  btbl.sink(f.mutation)
                case MutationType.INCREMENT =>
                  btbl.sink(f.mutation)
                case MutationType.DELETE =>
                  mutator.mutate(f.mutation)
                case MutationType.PUT =>
                  mutator.mutate(f.mutation)
                case m: MutationType.Type =>
                  logWarning(s"Uknown mutation $m")
              }
            }
          }
        }
      } else {
        part.foreach { f =>
          assert(f.table != null)
          withMutator(conf, f.table, bufferSize) { mutator =>
            withBufferedSinkTable(conf, f.table, synchronized = false, bufferSize) { btbl =>
              f.action match {
                case MutationType.APPEND =>
                  btbl.sink(f.mutation)
                case MutationType.INCREMENT =>
                  btbl.sink(f.mutation)
                case MutationType.DELETE =>
                  mutator.mutate(f.mutation)
                case MutationType.PUT =>
                  mutator.mutate(f.mutation)
                case m: MutationType.Type =>
                  logWarning(s"Uknown mutation $m")
              }
            }
          }
        }
      }
    }
  }
}

case class MutationAction(action: MutationType.Type, mutation: Mutation, table: String = null)

object MutationType extends Enumeration {
  type Type = Value
  val APPEND, DELETE, PUT, INCREMENT = Value
}
