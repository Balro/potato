package spark.potato.hadoop.utils

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.junit.Test

class HDFSUtilTest {
  System.setProperty("HADOOP_USER_NAME", "hdfs")

  @Test
  def mergeTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("hdfs_merge_test").getOrCreate()
    HDFSUtil.merge(spark,
      "hdfs://test01/user/hive/warehouse/baluo_test.db/test2",
      "hdfs://test01/baluo_out",
      "text")

    TimeUnit.DAYS.sleep(1)
  }
}
