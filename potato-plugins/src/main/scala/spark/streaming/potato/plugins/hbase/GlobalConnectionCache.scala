package spark.streaming.potato.plugins.hbase

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, Connection, ConnectionFactory, Table}

object GlobalConnectionCache extends Serializable {
  val connections = new ConcurrentHashMap[HBaseZKInfo, Connection]()

  private def getCachedConnection(conf: Configuration): Connection = this.synchronized {
    import scala.collection.JavaConversions.mapAsScalaMap
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
