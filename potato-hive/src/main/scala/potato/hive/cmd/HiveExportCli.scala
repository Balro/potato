package potato.hive.cmd

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import potato.common.cmd.CommonCliBase
import potato.spark.sql.writer._
import potato.spark.conf.SparkConfUtil.conf2Loadable

object HiveExportCli extends CommonCliBase {
  override val cliName: String = "HiveExportCli"
  override val usageFooter: String =
    """
      |export to kafka usage:
      | ./bin/potato hive --export \
      |   --conf spark.master=yarn-client \
      |   --sql "select 1" \
      |   --writer kafka \
      |   --writer-conf bootstrap.servers=localhost:9092 \
      |   --writer-conf topic=test1 \
      |   --writer-conf writer.format=first \ # first/json/csv
      |   [--writer-conf csv.sep=,] \ # seperator for csv format, default is  ','
      |   [--writer-conf spark.potato.kafka.producer.speed.limit=100] \ # msg/sec rate limit per executor
      |   [--writer-conf key=value] # other producer configs
      |""".stripMargin

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  override def initOptions(opts: Options): Unit = {
    optBuilder().longOpt("prop-file")
      .desc("Specify default prop file to load.").hasArg
      .add()
    optBuilder().longOpt("writer")
      .desc("Specify writer implement, support kafka.").hasArg.required
      .add()
    optBuilder().longOpt("writer-conf")
      .desc("Config writer, e.g. --writer-conf k1=v1 --writer-conf k2=v2.").hasArg
      .add()
    optBuilder().longOpt("sql")
      .desc("Spark sql to extra hive data.").hasArg.required
      .add()
  }

  /**
   * 根据已解析命令行参数进行处理。
   */
  override def handleCmd(cmd: CommandLine): Unit = {
    val conf = new SparkConf()
    handleValue("prop-file", file => conf.load(file))
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val extraDF: DataFrame = handleValue("sql", sql => spark.sql(sql))
    val writer = extraDF.potatoWrite
    handleValue("writer", f => writer.format(f))
    handleValues("writer-conf", _.foreach { f =>
      val kv = f.split("=")
      writer.option(kv(0), kv(1))
    })
    writer.save()
  }
}
