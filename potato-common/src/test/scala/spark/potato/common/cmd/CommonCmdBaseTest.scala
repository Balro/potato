package spark.potato.common.cmd

import org.apache.commons.cli.{CommandLine, Option, Options}
import org.junit.Test

class CommonCmdBaseTest {
  @Test
  def mainTest(): Unit = {
    CommonCmdBaseImp.main(Array("-a", "hello"))
  }

  object CommonCmdBaseImp extends CommonCliBase {
    override val cliName: String = this.getClass.getSimpleName
    override val cliDescription: String = "test"

    /**
     * 预处理，添加[[org.apache.commons.cli.Option]]。
     */
    override def initOptions(opts: Options): Unit = {
      opts.addOption(
        optionBuilder("a")
          .longOpt("action")
          .desc("this is action arg")
          .hasArg
          .required()
          .build()
      )
      opts.addOption(
        Option.builder("b")
          .longOpt("build")
          .desc("this is build arg")
          .build()
      )
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