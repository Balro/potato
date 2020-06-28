package potato.hadoop.cmd

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import potato.hadoop.utils.HDFSUtil
import potato.common.cmd.CommonCliBase

object FileMergeCli extends CommonCliBase {
  override val cliName: String = "FileMergeCli"
  override val usageHeader: String =
    """
      |It is used to merge a large number of small files, e.g. hive table.
      |It is implemented by SparkSql, supports partition discovery, and supports SQL to represent partition filtering conditions.
      |""".stripMargin.trim
  override val helpWidth: Int = 100

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  override def initOptions(opts: Options): Unit = {
    optBuilder().longOpt("source")
      .desc("Source directory to merge.").hasArg.required
      .add()
    optBuilder().longOpt("target")
      .desc("Target directory to store merged files.Default: source directory.").hasArg
      .add()
    optBuilder().longOpt("source-format")
      .desc("Source file format, e.g. text/parquet/org etc.").hasArg.required
      .add()
    optBuilder().longOpt("target-format")
      .desc("Target file format, e.g. text/parquet/org etc. Default: equals --source-format").hasArg
      .add()
    optBuilder().longOpt("partition-filter")
      .desc("Sql expression to filter partitions, like \"ymd=20200101 and type='a'\".").hasArg
      .add()
    optBuilder().longOpt("no-partition")
      .desc("Turn off partition awareness to improve performance. Only used for no partition path.").hasArg(false)
      .add()
    optBuilder().longOpt("parallelism")
      .desc("Equals spark conf: spark.default.parallelism. Default: 1.").hasArg
      .add()
    optBuilder().longOpt("path-discover-parallelism")
      .desc(s"Equals spark conf: ${SQLConf.PARALLEL_PARTITION_DISCOVERY_PARALLELISM.key}. Default: 20.").hasArg
      .add()
    optBuilder().longOpt("max-partition-bytes")
      .desc(s"Equals spark conf: ${SQLConf.FILES_MAX_PARTITION_BYTES.key}. Default: ${SQLConf.FILES_MAX_PARTITION_BYTES.defaultValue.get}")
      .hasArg
      .add()
    optBuilder().longOpt("file-open-cost")
      .desc(s"Equals spark conf: ${SQLConf.FILES_OPEN_COST_IN_BYTES.key}. Default: 100k.").hasArg
      .add()
    optBuilder().longOpt("hadoop-conf")
      .desc("Configs add to hadoop Configuration. e.g. --hadoop-conf key1 value1 --hadoop-conf key2 value2")
      .numberOfArgs(2)
      .add()
    optBuilder().longOpt("spark-conf")
      .desc("Configs add to SparkConf. e.g. --spark-conf key1 value1 --spark-conf key2 value2")
      .numberOfArgs(2)
      .add()
    optBuilder().longOpt("reader-opts")
      .desc("Configs add to DataFrameReader. e.g. --reader-opts key1 value1 --reader-opts key2 value2")
      .numberOfArgs(2)
      .add()
    optBuilder().longOpt("writer-opts")
      .desc("Configs add to DataFrameWriter. e.g. --writer-opts key1 value1 --writer-opts key2 value2")
      .numberOfArgs(2)
      .add()
    optBuilder().longOpt("compression")
      .desc("Compression codec to compress outfiles, e.g. none/snappy/lz4 etc. Default: snappy.").hasArg
      .add()
    optBuilder().longOpt("overwrite")
      .desc("Whether to overwrite the target directory. Default: false.").hasArg(false)
      .add()
    optBuilder().longOpt("max-job-parallelism")
      .desc("Max concurrence job number. Default: 20.").hasArg
      .add()
    optBuilder().longOpt("disable-schema-sample")
      .desc("Enable schema sample will infer schema by the first data file to improve performance. Disable schema sample will use default schema infer policy.")
      .add()
    optBuilder().longOpt("schema-sample-file")
      .desc("File to infer global schema.").hasArg
      .add()
  }

  /**
   * 根据已解析命令行参数进行处理。
   */
  override def handleCmd(cmd: CommandLine): Unit = {
    import scala.collection.JavaConversions.propertiesAsScalaMap
    val conf = new SparkConf()
    conf.set("spark.default.parallelism", cmd.getOptionValue("parallelism", "1"))
    conf.set(SQLConf.FILES_MAX_PARTITION_BYTES.key, cmd.getOptionValue("max-partition-bytes", (128 * 1024 * 1024).toString))
    conf.set(SQLConf.FILES_OPEN_COST_IN_BYTES.key, cmd.getOptionValue("file-open-cost", (100 * 1024).toString))
    conf.set(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key, 1.toString)

    handleValue("path-discover-parallelism", value => {
      conf.set(SQLConf.PARALLEL_PARTITION_DISCOVERY_PARALLELISM.key, value)
    }, () => {
      conf.set(SQLConf.PARALLEL_PARTITION_DISCOVERY_PARALLELISM.key, 20.toString)
    })

    handleValues("spark-conf", _.foreach { f =>
      val kv = f.split("=")
      conf.set(kv(0), kv(1))
    })
    val spark = SparkSession.builder().appName(cliName).config(conf).getOrCreate()

    handleValues("hadoop-conf", _.foreach { f =>
      val kv = f.split("=")
      spark.sparkContext.hadoopConfiguration.set(kv(0), kv(1))
    })

    val schema = handleValue("schema-sample-file", file =>
      HDFSUtil.getDataFileSchema(spark, file, cmd.getOptionValue("source-format"))
    )

    // 是否启用快速schema推测，即为每个分区使用分区下第一个数据文件进行推测。
    val quickSchemaSample = handleKey("disable-schema-sample", () => false, () => true)

    console("Merged paths:")
    handleKey("no-partition", { () =>
      console(HDFSUtil.mergeNoPartitionedPath(spark,
        source = cmd.getOptionValue("source"),
        target = cmd.getOptionValue("target", cmd.getOptionValue("source")),
        sourceFormat = cmd.getOptionValue("source-format"),
        targetFormat = cmd.getOptionValue("target-format", cmd.getOptionValue("source-format")),
        overwrite = cmd.hasOption("overwrite"),
        readerOptions = cmd.getOptionProperties("reader-opts").toMap,
        writerOptions = cmd.getOptionProperties("reader-opts").toMap,
        compression = cmd.getOptionValue("compression", "snappy"),
        schema = schema
      ))
    }, { () =>
      console(HDFSUtil.mergePartitionedPath(spark,
        source = cmd.getOptionValue("source"),
        target = cmd.getOptionValue("target", cmd.getOptionValue("source")),
        sourceFormat = cmd.getOptionValue("source-format"),
        targetFormat = cmd.getOptionValue("target-format", cmd.getOptionValue("source-format")),
        overwrite = cmd.hasOption("overwrite"),
        partitionFilter = cmd.getOptionValue("partition-filter"),
        maxParallel = cmd.getOptionValue("max-job-parallelism", "20").toInt,
        readerOptions = cmd.getOptionProperties("reader-opts").toMap,
        writerOptions = cmd.getOptionProperties("reader-opts").toMap,
        compression = cmd.getOptionValue("compression", "snappy"),
        schema = schema,
        quickInferSchema = quickSchemaSample
      ).mkString("\n"))
    })

    spark.stop()
  }
}
