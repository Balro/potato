package spark.streaming.potato.plugins.hbase

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, Connection, ConnectionFactory, Row, Table}

import scala.collection.mutable.ListBuffer

object GlobalConnectionCache {
  private val connections = new ConcurrentHashMap[HBaseZKInfo, Connection]()

  private def getCachedConnection(conf: Configuration): Connection = this.synchronized {
    import scala.collection.JavaConversions.mapAsScalaConcurrentMap
    connections.getOrElseUpdate(HBaseZKInfo(conf), ConnectionFactory.createConnection(conf))
  }

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

  def withBufferedTable[R](conf: Configuration, table: String)(f: BufferedTable => R): R = {
    val btbl = new BufferedTable(getCachedConnection(conf).getTable(TableName.valueOf(table)))
    val ret = f(btbl)
    btbl.close()
    ret
  }

  /**
   * 非线程安全，多线程访问可能造成数据丢失。
   * 如需多线程访问，请使用SynchronizedBufferedTable。
   */
  class BufferedTable(table: Table) {

    import scala.collection.JavaConversions.bufferAsJavaList

    private val buffer: ListBuffer[Row] = ListBuffer.empty[Row]

    def close(): Unit = {
      flush()
      table.close()
    }

    def flush(): Unit = {
      table.batch(buffer, new Array[AnyRef](buffer.size))
      buffer.clear()
    }

    def add(row: Row): Unit = {
      buffer += row
    }
  }

  /**
   * 线程安全。
   */
  class SynchronizedBufferedTable(table: Table) {

    import scala.collection.JavaConversions.bufferAsJavaList

    private val buffer: ListBuffer[Row] = ListBuffer.empty[Row]

    def close(): Unit = synchronized {
      flush()
      table.close()
    }

    def flush(): Unit = synchronized {
      table.batch(buffer, new Array[AnyRef](buffer.size))
      buffer.clear()
    }

    def add(row: Row): Unit = synchronized {
      buffer += row
    }
  }

  case class HBaseZKInfo(quorum: String, port: Int, parent: String, metaServer: String)

  object HBaseZKInfo {
    def apply(conf: Configuration): HBaseZKInfo = HBaseZKInfo(
      conf.get("hbase.zookeeper.quorum"),
      conf.getInt("hbase.zookeeper.property.clientPort", 2181),
      conf.get("zookeeper.znode.parent", "/hbase"),
      conf.get("zookeeper.znode.metaserver", "meta-region-server")
    )
  }

}
