package spark.potato.common.cmd

import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Option, Options, ParseException}

/**
 * 基于 apache commons cli 构造的命令行基类。
 */
abstract class CommonCliBase {
  private val parser = new DefaultParser()
  private val opts = new Options()
  private var cmd: CommandLine = _

  val cliName: String
  val cliDescription: String

  def main(args: Array[String]): Unit = {
    initOptions(opts)
    try {
      cmd = parser.parse(opts, args)
      handleCmd(cmd)
    } catch {
      case e: ParseException =>
        println(e.getMessage)
        println(cliDescription)
        new HelpFormatter().printHelp(cliName, opts)
    }
  }

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  def initOptions(opts: Options): Unit

  /**
   * 根据已解析命令行参数进行处理。
   */
  def handleCmd(cmd: CommandLine): Unit

  def optionBuilder(shortName: String = null) = Option.builder(shortName)
}
