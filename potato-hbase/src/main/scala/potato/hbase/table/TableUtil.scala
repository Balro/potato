package potato.hbase.table

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, ConnectionConfiguration, Table}
import potato.hadoop.conf.SerializedConfiguration
import spark.potato.hbase.connection.GlobalConnectionCache.getCachedConnection

/**
 * hbase表工具类。
 */
object TableUtil {
  /**
   * 通过mutator对表进行批量操作，获取的mutator参考org.apache.hadoop.hbase.client.Connection#getBufferedMutator(org.apache.hadoop.hbase.client.BufferedMutatorParams)
   *
   * @param f 对mutator的操作函数。
   * @tparam R 操作函数 f 的返回值类型。
   * @return 操作函数 f 的返回值。
   */
  def withMutator[R](conf: SerializedConfiguration, table: String,
                     bufferSize: Long = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT)(f: BufferedMutator => R): R = {
    val mtt = getCachedConnection(conf).getBufferedMutator(
      new BufferedMutatorParams(TableName.valueOf(table)).writeBufferSize(bufferSize)
    )
    val ret = f(mtt)
    mtt.close()
    ret
  }

  /**
   * 通过table对表进行批量操作，获取的table参考org.apache.hadoop.hbase.client.Connection#getTable(org.apache.hadoop.hbase.TableName)
   *
   * @param f 对table的操作函数。
   * @tparam R 操作函数 f 的返回值类型。
   * @return 操作函数 f 的返回值。
   */
  def withTable[R](conf: SerializedConfiguration, table: String)(f: Table => R): R = {
    val tbl = getCachedConnection(conf).getTable(TableName.valueOf(table))
    val ret = f(tbl)
    tbl.close()
    ret
  }

  /**
   * 通过BufferedSinkTable将数据批量写入HBase。
   *
   * @param synchronized 是否同步写入。
   * @param f            对BufferedSinkTable的操作函数。
   * @tparam R 操作函数 f 的返回值类型。
   * @return 操作函数 f 的返回值。
   */
  def withBufferedSinkTable[R](conf: SerializedConfiguration, table: String, synchronized: Boolean = false,
                               bufferSize: Long = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT)(f: BufferedSinkTable => R): R = {
    val sinkTable = if (synchronized)
      new SynchronizedBufferedSinkTableImp(getCachedConnection(conf).getTable(TableName.valueOf(table)), bufferSize)
    else
      new BufferedSinkTableImp(getCachedConnection(conf).getTable(TableName.valueOf(table)), bufferSize)
    val ret = f(sinkTable)
    sinkTable.close()
    ret
  }

}
