package spark.potato.hbase.conf

import org.apache.hadoop.hbase.HConstants
import spark.potato.common.conf.CommonConfigKeys.POTATO_PREFIX

object HBaseConfigKeys {
  val POTATO_HBASE_PREFIX: String = POTATO_PREFIX + "hbase."
  // 用于从配置文件加载HBaseConfiguration。
  val POTATO_HBASE_CONF_PREFIX: String = POTATO_HBASE_PREFIX + "conf."
  val POTATO_HBASE_CONF_ZOOKEEPER_QUORUM: String = POTATO_HBASE_CONF_PREFIX + HConstants.ZOOKEEPER_QUORUM
  val POTATO_HBASE_CONF_ZOOKEEPER_CLIENT_PORT: String = POTATO_HBASE_CONF_PREFIX + HConstants.ZOOKEEPER_CLIENT_PORT
}
