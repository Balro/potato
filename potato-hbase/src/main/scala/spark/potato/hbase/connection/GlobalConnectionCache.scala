package spark.potato.hbase.connection

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher
import spark.potato.common.cache.KeyedCacheBase
import spark.potato.hbase.connection.ConnectionInfo.ConnectionAddress

/**
 * 全局connection缓存。
 * 通过(
 *    hbase.zookeeper.quorum,
 *    hbase.zookeeper.property.clientPort,
 *    zookeeper.znode.parent,
 *    zookeeper.znode.metaserver
 * ) 四个参数作为标识id。
 */
object GlobalConnectionCache extends KeyedCacheBase[ConnectionInfo, Connection] {
  /**
   * 通过Configuration获取connection，Configuration中的(
   * hbase.zookeeper.quorum,
   * hbase.zookeeper.property.clientPort,
   * zookeeper.znode.parent,
   * zookeeper.znode.metaserver)被用作唯一key。
   */
  def getCachedConnection(conf: Configuration): Connection = this.synchronized {
    internalGetOrCreate(conf) { () =>
      ConnectionFactory.createConnection(conf)
    }
  }

  /**
   * 释放所有连接，清理缓存。
   */
  def close(): Unit = this.synchronized {
    internalClose(_.close())
  }

  /**
   * 检查给定值是否可用。
   */
  override protected def check(v: Connection): Boolean = !(v.isClosed || v.isAborted)
}

class ConnectionInfo(val id: ConnectionAddress, val conf: Configuration) {
  override def equals(obj: Any): Boolean = obj.isInstanceOf[ConnectionInfo] && obj.asInstanceOf[ConnectionInfo].id.equals(this.id)

  override def hashCode(): Int = id.hashCode()
}

object ConnectionInfo {

  case class ConnectionAddress(quorum: String, port: Int, parent: String, metaserver: String)

  val ZK_METASERVER_KEY = "zookeeper.znode.metaserver"

  implicit def parse(conf: Configuration): ConnectionInfo = new ConnectionInfo(
    ConnectionAddress(conf.get(HConstants.ZOOKEEPER_QUORUM, HConstants.LOCALHOST),
      conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT),
      conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT),
      conf.get(ZK_METASERVER_KEY, ZooKeeperWatcher.META_ZNODE_PREFIX)
    ), conf)
}
