package spark.potato.hbase.sink

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import spark.potato.hbase.util.HBaseConfigurationUtil
import spark.potato.hbase.conf._

class HBaseSinkUtilTest {
  @Test
  def saveToHbaseTest(): Unit = {
    val conf = new SparkConf().setAppName("HBaseSinkUtilTest").setMaster("local")
    conf.set(POTATO_HBASE_CONF_ZOOKEEPER_QUORUM_KEY, "test01,test02")
    val sc = SparkContext.getOrCreate(conf)
    val rdd = sc.makeRDD(0 until 10).map { f =>
      MutationAction(MutationType.PUT, new Put(Bytes.toBytes(f.toString)).addColumn(
        Bytes.toBytes("f1"),
        Bytes.toBytes("c1"),
        Bytes.toBytes(System.currentTimeMillis().toString)
      ))
    }
    val hbaseConf = HBaseConfigurationUtil.sparkToConfiguration(conf)
    HBaseSinkUtil.saveToHBase(rdd, hbaseConf, "test")
  }

  @Test
  def hbaseSinkTest(): Unit = {
    val conf = new SparkConf().setAppName("HBaseSinkUtilTest").setMaster("local")
    conf.set(POTATO_HBASE_CONF_ZOOKEEPER_QUORUM_KEY, "test01,test02")
    val sc = SparkContext.getOrCreate(conf)
    val rdd = sc.makeRDD(0 until 10).map { f =>
      MutationAction(MutationType.PUT, new Put(Bytes.toBytes(f.toString)).addColumn(
        Bytes.toBytes("f1"),
        Bytes.toBytes("c1"),
        Bytes.toBytes(System.currentTimeMillis().toString)
      ))
    }
    val hbaseConf = HBaseConfigurationUtil.sparkToConfiguration(conf)
    rdd.saveToHBase(hbaseConf, "test")
  }
}
