package spark.potato.hbase.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import spark.potato.hadoop.conf.SerializedConfiguration
import spark.potato.hbase.conf._

object HBaseConfigurationUtil {
  /**
   * 用于从SparkConf加载HBaseConfiguration。
   */
  def readSparkConf(conf: SparkConf, prefix: String = POTATO_HBASE_CONF_PREFIX): Configuration = {
    HBaseConfiguration.addHbaseResources(
      SerializedConfiguration.readSparkConf(conf, prefix)
    )
  }
}
