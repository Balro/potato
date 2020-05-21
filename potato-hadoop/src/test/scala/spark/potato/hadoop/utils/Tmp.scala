package spark.potato.hadoop.utils

import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

object Tmp {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    val df: DataFrame = spark.read.format("text").load("hdfs://test01/user/hive/warehouse/baluo_test.db/test")
    df.printSchema()

    val ps = HDFSUtil.getPartitionSpec(df)
    println(ps)
    val partitions: Seq[Row] = ps.partitions.map(f => Row.fromSeq(f.values.toSeq(ps.partitionColumns).map(f => if (f.isInstanceOf[UTF8String]) f.toString else f)))
    val rdd: RDD[Row] = spark.sparkContext.makeRDD(partitions)
    println("======")
    val a: Seq[Map[String, String]] = partitions.map { f =>
      ps.partitionColumns.map(_.name).zip(f.toSeq.map(_.toString)).toMap
    }
    a.map { f =>
      println(PartitioningUtils.getPathFragment(f, ps.partitionColumns))
    }
    //    ps.partitionColumns.map(_.name)
    //    val test = spark.createDataFrame(rdd, ps.partitionColumns)
    //
    //
    //    val a: Array[String] = test.where("ymd=20200519")
    //      .collect().map { f =>
    //      ps.partitionColumns.zip(f.toSeq).map { f =>
    //        s"${f._1.name}='${f._2}'"
    //      }.mkString(" and ")
    //    }
    //    a.foreach { f =>
    //      df.where(f).show()
    //      df.where(f).write.format("text").mode(SaveMode.Overwrite).partitionBy(ps.partitionColumns.map(_.name): _*).save("hdfs://test01/user/hive/warehouse/baluo_test.db/test2")
    //    }

    TimeUnit.DAYS.sleep(1)
  }
}