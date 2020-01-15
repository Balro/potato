package spark.streaming.potato.plugins.hbase

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

class HBasePluginTest {
  @Test
  def saveToHBaseTableTest(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]").setAppName("hbase plugin test")
    val ssc = new StreamingContext(conf, Seconds(10))

    
  }
}
