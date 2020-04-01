package spark.potato.hadoop.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, FileStatusCache}
import org.apache.spark.sql.types.StructType

object FileSchemaUtil {
  /**
   * 解析指定目录的数据结构和分区结构。
   *
   * @param spark   sparksession实例。
   * @param format  sparksession.read.format(source:String)支持的文件格式。
   * @param path    需要解析的文件或目录。
   * @param options 用于提供给DataSource的配置，等同于sparksession.read.option(key:String,value:String)支持的参数。
   * @return (dataSchema,partitionSchema)
   */
  def getFileSchema(spark: SparkSession, format: String, path: String, options: Map[String, String] = Map.empty): (StructType, StructType) = {
    val source = DataSource(spark, paths = Seq(path), className = format, options = options)
    val method = source.getClass.getDeclaredMethod("getOrInferFileFormatSchema", classOf[FileFormat], classOf[FileStatusCache])
    method.setAccessible(true)
    val (dataSchema, partitionSchema) = method.invoke(source,
      source.providingClass.newInstance().asInstanceOf[FileFormat],
      FileStatusCache.getOrCreate(spark)).asInstanceOf[(StructType, StructType)]
    (dataSchema, partitionSchema)
  }

  /**
   * 解析指定目录的数据结构和分区结构。
   *
   * @param conf   用于创建sparksession实例的配置。
   * @param format sparksession.read.format()支持的文件格式。
   * @param path   需要解析的文件或目录。
   * @return (dataSchema,partitionSchema)
   */
  def getFileSchema(conf: SparkConf, format: String, path: String): (StructType, StructType) = {
    getFileSchema(SparkSession.builder().config(conf).getOrCreate(), format, path)
  }

  /**
   * 解析指定目录的数据结构和分区结构。
   *
   * @param sc     用于创建sparksession实例的context。
   * @param format sparksession.read.format()支持的文件格式。
   * @param path   需要解析的文件或目录。
   * @return (dataSchema,partitionSchema)
   */
  def getFileSchema(sc: SparkContext, format: String, path: String): (StructType, StructType) = {
    getFileSchema(SparkSession.builder().config(sc.getConf).getOrCreate(), format, path)
  }
}