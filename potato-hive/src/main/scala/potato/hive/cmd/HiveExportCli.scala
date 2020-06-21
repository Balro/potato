package potato.hive.cmd

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import potato.common.cmd.CommonCliBase
import potato.spark.sql.writer._
import potato.spark.conf.SparkConfUtil._

object HiveExportCli extends CommonCliBase {
  override val cliName: String = "HiveExportCli"

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
    optBuilder().longOpt("spark-conf")
      .desc("Config writer, e.g. --writer-conf k1=v1 --writer-conf k2=v2.").hasArg
      .add()
  }

  /**
   * 根据已解析命令行参数进行处理。
   */
  override def handleCmd(cmd: CommandLine): Unit = {
    val conf = new SparkConf()
    handleValue("prop-file", file => conf.loadPropertyFile(file))
    handleValues("spark-conf", _.foreach { f =>
      val kv = f.split("=")
      conf.set(kv(0), kv(1))
    })
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val extraDF: DataFrame = handleValue("sql", sql => spark.sql(sql))
    val writer = extraDF.potatoWrite
    handleValue("writer", f => writer.format(f))
    handleValues("writer-conf", _.foreach { f =>
      val kv = f.split("=")
      conf.set(kv(0), kv(1))
    })
    writer.save()
  }
}
