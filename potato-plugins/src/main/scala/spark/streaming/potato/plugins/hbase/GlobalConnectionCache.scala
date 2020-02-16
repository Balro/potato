package spark.streaming.potato.plugins.hbase

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
 * jvm全局connection缓存。
 * 通过
 * hbase.zookeeper.quorum,
 * hbase.zookeeper.property.clientPort,
 * zookeeper.znode.parent,
 * zookeeper.znode.metaserver
 * 四个参数作为key。
 */
object GlobalConnectionCache {
  private val connections = new ConcurrentHashMap[HBaseInfo, Connection]()

  def getCachedConnection(conf: Configuration): Connection = this.synchronized {
    import scala.collection.JavaConversions.mapAsScalaConcurrentMap
    connections.getOrElseUpdate(HBaseInfo(conf), ConnectionFactory.createConnection(conf))
  }

  case class HBaseInfo(quorum: String, port: Int, parent: String, metaServer: String)

  object HBaseInfo {
    def apply(conf: Configuration): HBaseInfo = HBaseInfo(
      conf.get("hbase.zookeeper.quorum"),
      conf.getInt("hbase.zookeeper.property.clientPort", 2181),
      conf.get("zookeeper.znode.parent", "/hbase"),
      conf.get("zookeeper.znode.metaserver", "meta-region-server")
    )
  }

}
