package potato.hadoop.utils

import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.spark.sql.SparkSession
import org.junit.Test
import potato.hadoop.cmd.FileMergeCli

class HDFSUtilTest {
  System.setProperty("HADOOP_USER_NAME", "hdfs")

  @Test
  def mergeCliTest(): Unit = {
    FileMergeCli.main(("--spark-conf spark.master local[*] " +
      "--source hdfs://test01/user/hive/warehouse/baluo_test.db/test2/ " +
      "--partition-filter type='a' " +
      "--source-format text " +
      "--target-format parquet " +
      "--compression gzip").split("\\s+"))
  }

  @Test
  def mergeTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("hdfs_merge_test")
      .config("spark.default.parallelism", 1)
      .getOrCreate()
    HDFSUtil.mergePartitionedPathV1(spark,
      "hdfs://test01/user/hive/warehouse/baluo_test.db/test2/",
      "hdfs://test01/user/hive/warehouse/baluo_test.db/test3/",
      "text",
      "text",
      compression = "gzip"
    )

    TimeUnit.DAYS.sleep(1)
  }

  @Test
  def dirOnlyTest(): Unit = {
    val fs = FileSystem.get(new HdfsConfiguration())
    println(HDFSUtil.dirOnly(fs, new Path("/baluo_out_merge_1590068372135_local-1590068370367/ymd=20200519")))
  }

  @Test
  def gatPartitionSpecFromPathTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("hdfs_util_test").getOrCreate()
    val parts = HDFSUtil.getPartitionSpecFromPath(spark, "/baluo_out")
    println(parts)
  }

  @Test
  def mergeNoPartitionedPathTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("hdfs_util_test").getOrCreate()
    HDFSUtil.mergeNoPartitionedPath(spark, "/baluo_out/ymd=20200519/type=a", "/baluo_out/ymd=20200519/type=a",
      "parquet", "parquet", compression = "none")
  }

  @Test
  def mergePartitionedPathV1Test(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("hdfs_util_test").getOrCreate()
    println(HDFSUtil.mergePartitionedPathV1(spark, "/baluo_out", "/baluo_out",
      "parquet", "parquet", compression = "none"))
    TimeUnit.DAYS.sleep(1)

  }
}
