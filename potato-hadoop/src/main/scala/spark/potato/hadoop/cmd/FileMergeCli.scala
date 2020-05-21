package spark.potato.hadoop.cmd

import org.apache.commons.cli.{CommandLine, Options}
import spark.potato.common.cmd.CommonCliBase

object FileMergeCli extends CommonCliBase {
  override val cliName: String = "FileMergeCli"

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  override def initOptions(opts: Options): Unit = {
  }

  /**
   * 根据已解析命令行参数进行处理。
   */
  override def handleCmd(cmd: CommandLine): Unit = ???
}
