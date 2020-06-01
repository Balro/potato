package potato.hbase.connection

import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.junit.Test

class GlobalConnectionCacheTest {
  @Test
  def getCachedConnectionTest(): Unit = {
    val conf1 = HBaseConfiguration.create()
    conf1.set(HConstants.ZOOKEEPER_QUORUM, "test01,test02")
    val conf2 = HBaseConfiguration.create()
    conf2.set(HConstants.ZOOKEEPER_QUORUM, "test01,test03")
    val conf3 = HBaseConfiguration.create()
    conf3.set(HConstants.ZOOKEEPER_QUORUM, "test01,test02")
    GlobalConnectionCache.getCachedConnection(conf1)
    GlobalConnectionCache.getCachedConnection(conf2)
    GlobalConnectionCache.getCachedConnection(conf3)
    println("Current connection num " + GlobalConnectionCache.size())
    GlobalConnectionCache.close()
    println("Connection number after close " + GlobalConnectionCache.size())
  }
}
