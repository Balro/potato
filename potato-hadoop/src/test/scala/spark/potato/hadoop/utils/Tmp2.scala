package spark.potato.hadoop.utils

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, Trash}
import org.apache.hadoop.hdfs.HdfsConfiguration

object Tmp2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new HdfsConfiguration()
    val fs = FileSystem.get(conf)
    val source = new Path("/baluo")
    val target = new Path("/baluo_out")
    println(fs.rename(source, target))
  }
}
