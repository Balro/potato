package potato.hbase.table

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.hadoop.hbase.client.{Mutation, Table}
import potato.hbase.exception.TableClosedException

import scala.collection.mutable.ListBuffer

trait SinkTable extends Serializable {
  def sink(row: Mutation): Unit
}

trait BufferedSinkTable extends SinkTable {
  def flush(): Unit

  def close(): Unit
}

/**
 * Table包装类，用于数据落地。
 * 非线程安全，多线程访问会造成数据丢失。
 * 如需多线程访问，请使用SynchronizedBufferedTable。
 */
class BufferedSinkTableImp(table: Table, bufferSize: Long) extends BufferedSinkTable {

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

  def sink(row: Mutation): Unit = {
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
class SynchronizedBufferedSinkTableImp(table: Table, bufferSize: Long) extends BufferedSinkTable {

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

  def sink(row: Mutation): Unit = synchronized {
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