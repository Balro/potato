package spark.potato.hadoop.cmd

import org.apache.commons.cli.{CommandLine, Options}
import spark.potato.common.cmd.CommonCliBase

object FileMergeCli extends CommonCliBase {
  override val cliName: String = "FileMergeCli"
  override val cliDescription: String = "Used to merge little files in the specified directory."

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  override def initOptions(opts: Options): Unit = {
    opts.addOption(optionBuilder()
      .longOpt("source")
      .build())
  }

  /**
   * 根据已解析命令行参数进行处理。
   */
  override def handleCmd(cmd: CommandLine): Unit = ???
}
