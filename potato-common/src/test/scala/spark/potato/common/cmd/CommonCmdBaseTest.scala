package spark.potato.common.cmd

import org.apache.commons.cli.{CommandLine, Option, OptionGroup, Options}
import org.junit.Test

class CommonCmdBaseTest {
  @Test
  def mainTest(): Unit = {
    CommonCmdBaseImp.main(Array("a", "hello"))
  }

  object CommonCmdBaseImp extends CommonCliBase {
    override val cliName: String = this.getClass.getSimpleName
    override val usageHeader: String = null
    override val usageFooter: String = null

    /**
     * 预处理，添加[[org.apache.commons.cli.Option]]。
     */
    override def initOptions(opts: Options): Unit = {
      optBuilder("a")
        .longOpt("action")
        .desc("this is action arg")
        .hasArg
        .required()
        .add()
      optBuilder("b")
        .longOpt("build")
        .required()
        .desc("this is build arg")
        .add()
      val g1 = new OptionGroup()
      g1.addOption(Option.builder("g").build())
      g1.addOption(Option.builder("h").build())
      g1.setRequired(true)
      opts.addOptionGroup(g1)
      val g2 = new OptionGroup()
      g2.addOption(Option.builder("i").required().build())
      g2.addOption(Option.builder("j").build())
      opts.addOptionGroup(g2)
    }

    /**
     * 根据已解析命令行参数进行处理。
     */
    override def handleCmd(cmd: CommandLine): Unit = {
      println(cmd.getOptionValue("a"))
      println(cmd.getOptionValue("b"))
    }
  }

}