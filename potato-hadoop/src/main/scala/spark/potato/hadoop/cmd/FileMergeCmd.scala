package spark.potato.hadoop.cmd

import spark.potato.common.cmd.ActionCMDBase
import spark.potato.hadoop.utils.FileMergeUtil

object FileMergeCmd extends ActionCMDBase {
  /**
   * 添加action,argument以及其他初始化。
   */
  override protected def init(): Unit = {
    addArgument("--source", describe = "source directory", needValue = true)
    addArgument("--target", describe = "target directory, if not specified, use source config", needValue = true)
    addArgument("--format", describe = "format used for spark-sql, tested format [orc]", needValue = true)
    addArgument("--where-expr", describe = "where expression append to  sql 'where' clause", needValue = true)
    addArgument("--parallelism", describe = "default parallelism", needValue = true)
    addArgument("--max-partition-bytes", describe = "max file size per partition", needValue = true)
    addArgument("--file-open-cost", describe = "extra size to calculate for each file", needValue = true)
    addArgument("--hadoop-conf", describe = "options add to hadoop configuration", needValue = true)
    addArgument("--read-options", describe = "options add to DataFrameReader", needValue = true)
    addArgument("--write-options", describe = "options add to DataFrameWriter", needValue = true)
    addArgument("--compression", describe = "compression used when writing file", needValue = true)

    addAction("merge", describe = "merge file util with partition awareness", needArgs = Set("--source", "--format"), action = () => {
      FileMergeUtil.merge(
        sourceDir = props("--source"),
        targetDir = props.getOrElse("--target", props("--source")),
        format = props("--format"),
        whereExpr = props.getOrElse("--where-expr", null),
        parallelism = props.getOrElse("--parallelism", "1").toInt,
        maxPartitionBytes = props.getOrElse("--max-partition-bytes", (128 * 1024 * 1024).toString).toLong,
        fileOpenCost = props.getOrElse("--file-open-cost", "0").toLong,
        hadoopConf = string2Map(props.getOrElse("--hadoop-conf", null)),
        readOptions = string2Map(props.getOrElse("--read-options", null)),
        writeOptions = string2Map(props.getOrElse("--write-options", null)),
        compression = props.getOrElse("--compression", "snappy")
      )
    })
  }

  def string2Map(str: String): Map[String, String] = {
    if (str != null)
      str.split(",").map(f => f.split("=")).map(f => f(0) -> f(1)).toMap
    else
      Map.empty[String, String]
  }
}
