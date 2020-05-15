package spark.potato.common.cmd

import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options, ParseException}

/**
 * 基于 apache commons cli 构造的命令行基类。
 */
abstract class CommonCmdBase {
  private val parser = new DefaultParser()
  private val opts = new Options()
  private var cmd: CommandLine = _

  val cmdName: String

  def main(args: Array[String]): Unit = {
    initOptions()
    try {
      cmd = parser.parse(opts, args)
      handleCmd(cmd)
    } catch {
      case _: ParseException => new HelpFormatter().printHelp(cmdName, opts)
    }
  }

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  def initOptions(): Unit

  /**
   * 根据已解析命令行参数进行处理。
   */
  def handleCmd(cmd: CommandLine): Unit
}
