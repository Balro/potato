package potato.common.cmd

import org.apache.commons.cli.{CommandLine, Options}
import org.junit.Test

class CommonCmdBaseTest {
  @Test
  def mainTest(): Unit = {
    CommonCmdBaseImp.main(Array("--action", "hello", "-b", "a", "b"))
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
        .required
        .add()
      optBuilder("b")
        .longOpt("build")
        .hasArgs
        .required
        .desc("this is build arg")
        .add()
    }

    /**
     * 根据已解析命令行参数进行处理。
     */
    override def handleCmd(cmd: CommandLine): Unit = {
      console(cmd.getOptionValue("a"))
      console(cmd.getOptionValues("b").toString)
      console(cmd.getOptionProperties("b").toString)
    }
  }

}