package potato.hive.cmd

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import potato.common.cmd.CommonCliBase
import potato.common.exception.PotatoCmdException
import potato.spark.sql.writer._

object HiveExportCli extends CommonCliBase {
  override val cliName: String = "HiveExportCli"
  override val usageFooter: String =
    """
      |export to kafka usage:
      | ./bin/potato \
      |   --module hive \
      |   --export \
      |   --mode append \
      |   --writer kafka \
      |   --writer-conf bootstrap.servers=localhost:9092 \
      |   --writer-conf topic=test1 \
      |   --json / --csv [--csv-sep ','] \
      |   --sql "select 1" \
      |   [--writer-conf spark.potato.kafka.producer.speed.limit=100] \ # msg/sec rate limit per executor
      |   [--writer-conf key=value] # other producer configs
      |""".stripMargin

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  override def initOptions(opts: Options): Unit = {
    optBuilder().longOpt("writer").hasArg.required
      .desc("Specify writer implement, support kafka.")
      .add()
    optBuilder().longOpt("writer-conf")
      .desc("Config writer, e.g. --writer-conf k1=v1 --writer-conf k2=v2.").hasArg
      .add()
    optBuilder().longOpt("mode").hasArg.required
      .desc("Writing mode reference to org.apache.spark.sql.SaveMode")
      .add()
    optBuilder().longOpt("sql")
      .desc("Spark sql to extra hive data.").hasArg.required
      .add()
    groupBuilder()
      .addOption(optBuilder().longOpt("json").hasArg(false).desc("use json format").build())
      .addOption(optBuilder().longOpt("csv").hasArg(false).desc("use csv format").build())
      .addOption(optBuilder().longOpt("raw").hasArg(false).desc("use raw format").build())
      .required()
      .add()
    optBuilder().longOpt("csv-sep").hasArg
      .desc("Specify the scv separator character, default ','")
      .add()
  }

  /**
   * 根据已解析命令行参数进行处理。
   */
  override def handleCmd(cmd: CommandLine): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val rawDF: DataFrame = handleValue("sql", sql => spark.sql(sql))

    val formattedDF = handleKey("json", () => DataFrameFormatter.toJSON(rawDF),
      () => handleKey("csv", () => DataFrameFormatter.toCSV(rawDF, handleValue("csv-sep", v => v, () => ",")),
        () => handleKey("raw", () => rawDF)))

    val writer = formattedDF.potatoWrite
    handleValue("writer", f => writer.format(f))
    handleValues("writer-conf", _.foreach { f =>
      val kv = f.split("=")
      if (kv.length != 2) throw new PotatoCmdException(s"args format err, need k=v, found $f")
      writer.option(kv(0), kv(1))
    })
    handleValue("mode", mode => writer.mode(mode))

    writer.save()
  }
}
