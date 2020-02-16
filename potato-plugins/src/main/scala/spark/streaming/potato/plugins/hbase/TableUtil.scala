package spark.streaming.potato.plugins.hbase

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, Row, Table}
import spark.streaming.potato.common.exception.PotatoException
import spark.streaming.potato.plugins.hbase.GlobalConnectionCache.getCachedConnection

import scala.collection.mutable.ListBuffer

object TableUtil {
  def withMutator[R](conf: Configuration, table: String)(f: BufferedMutator => R): R = {
    val mtt = getCachedConnection(conf).getBufferedMutator(TableName.valueOf(table))
    val ret = f(mtt)
    mtt.close()
    ret
  }

  def withTable[R](conf: Configuration, table: String)(f: Table => R): R = {
    val tbl = getCachedConnection(conf).getTable(TableName.valueOf(table))
    val ret = f(tbl)
    tbl.close()
    ret
  }

  def withBufferedSinkTable[R](conf: Configuration, table: String, bufferSize: Int)(f: BufferedSinkTable => R): R = {
    val sinkTable = new BufferedSinkTable(getCachedConnection(conf).getTable(TableName.valueOf(table)), bufferSize)
    val ret = f(sinkTable)
    sinkTable.close()
    ret
  }

  def withSynchronizedBufferedSinkTable[R](conf: Configuration, table: String, bufferSize: Int)(f: SynchronizedBufferedSinkTable => R): R = {
    val sinkTable = new SynchronizedBufferedSinkTable(getCachedConnection(conf).getTable(TableName.valueOf(table)), bufferSize)
    val ret = f(sinkTable)
    sinkTable.close()
    ret
  }

  /**
   * Table包装类，用于数据落地。
   * 非线程安全，多线程访问会造成数据丢失。
   * 如需多线程访问，请使用SynchronizedBufferedTable。
   */
  class BufferedSinkTable(table: Table, bufferSize: Int) {

    import scala.collection.JavaConversions.bufferAsJavaList

    private var closed = false
    private val buffer: ListBuffer[Row] = ListBuffer.empty[Row]

    def close(): Unit = {
      flush()
      table.close()
      closed = true
    }

    def flush(): Unit = {
      table.batch(buffer, new Array[AnyRef](buffer.size))
      buffer.clear()
    }

    def add(row: Row): Unit = {
      if (closed) throw TableClosedException(s"Table ${table.getName} is already closed.")
      buffer += row
      if (buffer.size >= bufferSize)
        flush()
    }
  }

  /**
   * Table包装类，用于数据落地，线程安全。
   */
  class SynchronizedBufferedSinkTable(table: Table, bufferSize: Int) {

    import scala.collection.JavaConversions.seqAsJavaList

    private var closed = false
    private val buffer = new LinkedBlockingQueue[Row]()
    // 开启公平锁为了避免线程饿死，对性能的影响未测试。
    private val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock(true)

    def close(): Unit = synchronized {
      lock.writeLock().lock()
      try {
        flush()
        table.close()
        closed = true
      } finally lock.writeLock().unlock()
    }

    def flush(): Unit = synchronized {
      lock.writeLock().lock()
      try {
        table.batch(buffer.toArray[Row](Array.empty).toList, new Array[AnyRef](buffer.size))
        buffer.clear()
      } finally lock.writeLock().unlock()
    }

    def add(row: Row): Unit = synchronized {
      if (closed) throw TableClosedException(s"Table ${table.getName} is already closed.")
      lock.readLock().lock()
      try {
        buffer.put(row)
      } finally lock.readLock().unlock()
      if (buffer.size() >= bufferSize) {
        lock.writeLock().lock()
        try {
          if (buffer.size() >= bufferSize) flush()
        } finally lock.writeLock().unlock()
      }
    }
  }

  case class TableClosedException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

}
