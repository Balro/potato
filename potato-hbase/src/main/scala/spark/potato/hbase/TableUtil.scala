package spark.potato.hbase

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, ConnectionConfiguration, Mutation, Table}
import spark.potato.common.exception.PotatoException
import GlobalConnectionCache.getCachedConnection

import scala.collection.mutable.ListBuffer

object TableUtil {
  def withMutator[R](conf: SerializableConfiguration, table: String,
                     bufferSize: Long = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT)(f: BufferedMutator => R): R = {
    val mtt = getCachedConnection(conf).getBufferedMutator(
      new BufferedMutatorParams(TableName.valueOf(table)).writeBufferSize(bufferSize)
    )
    val ret = f(mtt)
    mtt.close()
    ret
  }

  def withTable[R](conf: SerializableConfiguration, table: String)(f: Table => R): R = {
    val tbl = getCachedConnection(conf).getTable(TableName.valueOf(table))
    val ret = f(tbl)
    tbl.close()
    ret
  }

  def withBufferedSinkTable[R](conf: SerializableConfiguration, table: String,
                               bufferSize: Long = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT)(f: BufferedSinkTable => R): R = {
    val sinkTable = new BufferedSinkTable(getCachedConnection(conf).getTable(TableName.valueOf(table)), bufferSize)
    val ret = f(sinkTable)
    sinkTable.close()
    ret
  }

  def withSynchronizedBufferedSinkTable[R](conf: SerializableConfiguration, table: String,
                                           bufferSize: Long = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT)(f: SynchronizedBufferedSinkTable => R): R = {
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
  class BufferedSinkTable(table: Table, bufferSize: Long) {

    import scala.collection.JavaConversions.bufferAsJavaList

    private var closed = false
    private val buffer: ListBuffer[Mutation] = ListBuffer.empty[Mutation]
    private var currentBufferSize = 0L

    def close(): Unit = {
      flush()
      table.close()
      closed = true
    }

    def flush(): Unit = {
      table.batch(buffer, new Array[AnyRef](buffer.size))
      buffer.clear()
      currentBufferSize = 0L
    }

    def add(row: Mutation): Unit = {
      if (closed) throw TableClosedException(s"Table ${table.getName} is already closed.")
      buffer += row
      currentBufferSize += row.heapSize()
      if (currentBufferSize >= bufferSize)
        flush()
    }
  }

  /**
   * Table包装类，用于数据落地，线程安全。
   * 原htable为非线程安全，这里所有对htable的操作均加写锁。
   */
  class SynchronizedBufferedSinkTable(table: Table, bufferSize: Long) {

    import scala.collection.JavaConversions.seqAsJavaList

    private var closed = false
    private val buffer = new LinkedBlockingQueue[Mutation]()
    private val currentBufferSize = new AtomicLong(0L)
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
        table.batch(buffer.toArray[Mutation](Array.empty).toList, new Array[AnyRef](buffer.size))
        buffer.clear()
        currentBufferSize.set(0L)
      } finally lock.writeLock().unlock()
    }

    def add(row: Mutation): Unit = synchronized {
      if (closed) throw TableClosedException(s"Table ${table.getName} is already closed.")
      lock.readLock().lock()
      try {
        buffer.put(row)
        currentBufferSize.addAndGet(row.heapSize())
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
