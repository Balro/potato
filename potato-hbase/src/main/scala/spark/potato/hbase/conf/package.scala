package spark.potato.hbase

import org.apache.hadoop.hbase.HConstants
import spark.potato.common.conf.POTATO_PREFIX

package object conf {
  val POTATO_HBASE_PREFIX: String = POTATO_PREFIX + "hbase."
  // 用于从配置文件加载HBaseConfiguration。
  val POTATO_HBASE_CONF_PREFIX: String = POTATO_HBASE_PREFIX + "conf."
  val POTATO_HBASE_CONF_ZOOKEEPER_QUORUM_KEY: String = POTATO_HBASE_CONF_PREFIX + HConstants.ZOOKEEPER_QUORUM
  val POTATO_HBASE_CONF_ZOOKEEPER_CLIENT_PORT_KEY: String = POTATO_HBASE_CONF_PREFIX + HConstants.ZOOKEEPER_CLIENT_PORT
}
