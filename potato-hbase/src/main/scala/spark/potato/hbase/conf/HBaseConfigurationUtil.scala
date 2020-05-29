package spark.potato.hbase.conf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import spark.potato.hadoop.conf.SerializedConfiguration

object HBaseConfigurationUtil {
  /**
   * 用于从SparkConf加载HBaseConfiguration。
   */
  def sparkToConfiguration(conf: SparkConf, prefix: String = POTATO_HBASE_CONF_PREFIX): Configuration = {
    HBaseConfiguration.addHbaseResources(
      SerializedConfiguration.readSparkConf(conf, prefix)
    )
  }
}
