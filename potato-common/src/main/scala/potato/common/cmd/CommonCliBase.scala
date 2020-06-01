package potato.common.cmd

import org.apache.commons.cli._

/**
 * 基于 apache commons cli 构造的命令行基类。
 */
abstract class CommonCliBase {
  private val parser = new DefaultParser()
  private val opts = new Options()
  private var cmd: CommandLine = _

  val cliName: String
  val usageHeader: String = null
  val usageFooter: String = null
  val helpWidth: Int = HelpFormatter.DEFAULT_WIDTH
  val strBuffer = new StringBuffer("\n")

  def main(args: Array[String]): Unit = {
    initOptions(opts)
    try {
      cmd = parser.parse(opts, args)
      handleCmd(cmd)
      println(strBuffer.toString)
    } catch {
      case e: ParseException =>
        console(e.getMessage)
        println(strBuffer.toString)
        printHelp()
    }
  }

  /**
   * 缓存输出结果，待程序结束后统一输出。避免中间输出结果与日志穿插问题，提高可读性。
   */
  def console(msg: String): Unit = strBuffer.append(msg).append("\n")

  def printHelp(): Unit = {
    new HelpFormatter().printHelp(helpWidth, cliName, usageHeader, opts, usageFooter, false)
  }

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  def initOptions(opts: Options): Unit

  /**
   * 根据已解析命令行参数进行处理。
   */
  def handleCmd(cmd: CommandLine): Unit

  def optBuilder(short: String = null): Option.Builder = Option.builder(short)

  def groupBuilder(): OptionGroup = new OptionGroup()

  implicit def addable(builder: Option.Builder): AddableOption = new AddableOption(builder)

  implicit def addable(group: OptionGroup): AddableGroup = new AddableGroup(group)

  class AddableOption(builder: Option.Builder) {
    /**
     * 创建Option并添加至参数列表。
     */
    def add(): Unit = opts.addOption(builder.build())
  }

  class AddableGroup(group: OptionGroup) {
    /**
     * 创建Option并添加至参数列表。
     */
    def add(required: Boolean = false): Unit = {
      group.setRequired(required)
      opts.addOptionGroup(group)
    }
  }

}
