package spark.potato.hbase.connection

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
 * 全局connection缓存。
 * 通过(
 *    hbase.zookeeper.quorum,
 *    hbase.zookeeper.property.clientPort,
 *    zookeeper.znode.parent,
 *    zookeeper.znode.metaserver
 * ) 四个参数作为标识id。
 */
object GlobalConnectionCache {
  private val connections = new ConcurrentHashMap[ConnectionInfo, Connection]()

  def getCachedConnection(conf: Configuration): Connection = this.synchronized {
    import scala.collection.JavaConversions.mapAsScalaConcurrentMap
    connections.getOrElseUpdate(ConnectionInfo(conf), ConnectionFactory.createConnection(conf))
  }

  case class ConnectionInfo(quorum: String, port: Int, parent: String, metaServer: String)

  object ConnectionInfo {
    def apply(conf: Configuration): ConnectionInfo = ConnectionInfo(
      conf.get("hbase.zookeeper.quorum"),
      conf.getInt("hbase.zookeeper.property.clientPort", 2181),
      conf.get("zookeeper.znode.parent", "/hbase"),
      conf.get("zookeeper.znode.metaserver", "meta-region-server")
    )
  }

}
