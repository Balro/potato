package spark.potato.hadoop.utils

import org.apache.hadoop.fs.{FileSystem, Path, Trash}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileMergeUtil {
  def setSparkConf(key: String, value: String): Unit = {
    System.setProperty(key, value)
  }

  /**
   * 合并后文件大小影响因素: 读取文件时的dataframe_partition大小，写入文件时，每个dataframe_partition写入file_partition的个数。
   * 计算公式: maxSplitBytes = min(maxPartitionBytes,min(totalSize/parallelism,fileOpenCost))
   * 参考 org.apache.spark.sql.execution.FileSourceScanExec#createNonBucketedReadRDD
   *
   * @param sourceDir         原路径。
   * @param targetDir         目标路径。
   * @param format            文件格式。
   * @param whereExpr         where过滤条件，会追加到sql的where条件中。只可以分区字段过滤，如无分区，参数须为null。
   * @param parallelism       并行度，影响合并后文件大小。
   * @param maxPartitionBytes 分区最大文件读取大小，影响合并后文件大小。
   * @param fileOpenCost      打开文件成本，影响合并后文件大小。
   * @param hadoopConf        额外加载入hadoopConfiguration的参数。
   * @param readOptions       额外加载入DataFrameReader的参数。
   * @param writeOptions      额外加载入DataFrameWriter的参数。
   */
  def merge(sourceDir: String, targetDir: String, format: String, whereExpr: String = null,
            parallelism: Int = 1,
            maxPartitionBytes: Long = 128 * 1024 * 1024, //默认分区大小128m。
            fileOpenCost: Long = 0, // 默认不考虑小文件打开成本。
            hadoopConf: Map[String, String] = Map.empty,
            readOptions: Map[String, String] = Map.empty,
            writeOptions: Map[String, String] = Map.empty
           ): Unit = {
    val spark = SparkSession.builder()
      .appName(s"FileMerge_[$sourceDir]to[$targetDir]")
      .config("spark.default.parallelism", parallelism)
      .getOrCreate()

    val tmpDir = s"${targetDir}_merge_file_${spark.sparkContext.applicationId}"

    // 不生成_SUCCESS文件。
    spark.sparkContext.hadoopConfiguration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false)
    // 在客户端开启回收站，该参数以服务端为准，如服务端未开启回收站，则程序报错。。
    spark.sparkContext.hadoopConfiguration.setInt("fs.trash.interval", 1)
    // 加载hadoop额外配置。
    hadoopConf.foreach(f => spark.sparkContext.hadoopConfiguration.set(f._1, f._2))

    // 设置分区文件相关参数。
    spark.conf.set(SQLConf.FILES_MAX_PARTITION_BYTES.key, maxPartitionBytes)
    spark.conf.set(SQLConf.FILES_OPEN_COST_IN_BYTES.key, fileOpenCost)

    // 获取路径分区结构。
    val (_, partitionSchema) = FileSchemaUtil.getFileSchema(spark, "orc", sourceDir)
    val partitionColumns = partitionSchema.map(_.name)

    // 检查分区。
    checkPartition(partitionColumns, whereExpr)

    // etl。
    val df = spark.read.options(readOptions).format(format).load(sourceDir)
    val out = if (whereExpr != null) df.where(whereExpr) else df
    out.write.options(writeOptions).partitionBy(partitionColumns: _*).format(format).save(tmpDir)

    // 临时文件替换原文件，原文件入回收站。
    checkAndReplace(spark, partitionColumns, out, targetDir, tmpDir)

    spark.close()
  }

  def checkAndReplace(spark: SparkSession, partitionColumns: Seq[String], out: DataFrame,
                      targetDir: String, tmpDir: String): Unit = {
    import spark.implicits._
    val fs = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
    val trash = new Trash(fs, fs.getConf)
    if (partitionColumns.nonEmpty) { // 分区目录，对单个分区进行处理。
      // 获取合并后生成的partition信息。
      out.select(partitionColumns.map(f => $"$f"): _*).distinct().collect().foreach { f =>
        val suffix = partitionColumns.zip(f.toSeq).map(f => s"${f._1}=${f._2}").mkString("/")
        val partPath = new Path(s"$targetDir/$suffix")

        // 如果目标目录存在，且入回收站失败，则抛出异常。
        if (fs.exists(partPath) && !trash.moveToTrash(partPath))
          throw new Exception(s"move to trash failed $partPath")

        // 如果父目录不存在，则创建。
        val parentPath = partPath.getParent
        if (!fs.exists(parentPath)) fs.mkdirs(parentPath)
        if (!fs.rename(new Path(s"$tmpDir/$suffix"), parentPath))
          throw new Exception(s"rename failed $tmpDir/$suffix to $parentPath")
      }
      if (safeToDelete(fs, new Path(tmpDir))) trash.moveToTrash(new Path(tmpDir))
    } else { // 非分区目录，直接替换整个目录。
      // 如果目标目录存在，且入回收站失败，则抛出异常。
      val targetDirPath = new Path(targetDir)
      if (fs.exists(targetDirPath) && !trash.moveToTrash(targetDirPath))
        throw new Exception(s"move to trash failed $targetDir")
      if (!fs.rename(new Path(tmpDir), targetDirPath))
        throw new Exception(s"rename failed $tmpDir to $targetDirPath")
    }

    fs.close()
  }

  def checkPartition(partitionColumns: Seq[String], whereExpr: String): Unit = {
    if (partitionColumns.isEmpty) {
      // 非分区目录无法指定分区条件。
      if (whereExpr != null)
        throw new Exception("normal directory cannot specify partition conditions")
    } else {
      // 分区目录必须指定分区条件。
      if (whereExpr == null)
        throw new Exception(s"partitioned directory must specify partition conditions, valid partition ${partitionColumns.mkString("[", ",", "]")}")
      // 分区目录是否存在条件指定子分区，但未指定父分区的情况。
      val w = whereExpr.split("and|or").map(f => (f.trim.split("[=<> ]")(0), true)).toMap
      val diff = w.keySet.&~(partitionColumns.toSet)
      if (diff.nonEmpty)
        throw new Exception(s"specified partition not found ${diff.mkString("[", ",", "]")}, available ${partitionColumns.mkString("[", ",", "]")}")
      for (i <- 0 until partitionColumns.length - 1) {
        if (!w.getOrElse(partitionColumns(i), false) && w.getOrElse(partitionColumns(i + 1), false)) {
          throw new Exception(s"parent partition not specified, parent name ${partitionColumns(i)}, child name ${partitionColumns(i + 1)}, valid order ${partitionColumns.mkString("->")}")
        }
      }
    }
  }

  // 检查指定路径下是否存在数据文件。
  def safeToDelete(fs: FileSystem, path: Path): Boolean = {
    val childStatus = fs.listStatus(path)
    childStatus.foreach { f =>
      if (f.isFile) return false
      else safeToDelete(fs, f.getPath)
    }
    true
  }
}
