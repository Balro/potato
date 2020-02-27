package spark.potato.common.cmd

object GeneralCmdImp extends ActionCMDBase {
  /**
   * 添加action以及其他初始化。
   */
  override def init(): Unit = {
    addAction("a1", "this is action a1", action = () => println("action a1 act"))
    addAction("a2", "this is action a2")
    addAction("a3", "this is action a3", Set("arg1", "arg3"),
      () =>
        println(s"received args -> $props")
    )

    addArgument("arg1", "this is arg1", needValue = true)
    addArgument("arg2", "this is arg2", needValue = true)
    addArgument("arg3", "this is arg3")
  }
}

object GeneralCmdTest {
  def main(args: Array[String]): Unit = {
    //    GeneralCmdImp.main(Array.empty[String])
    //    GeneralCmdImp.main(Array("abc"))
    //    GeneralCmdImp.main(Array("a1"))
    //    GeneralCmdImp.main(Array("a2"))
    GeneralCmdImp.main(Array("a3", "arg1", "haha", "arg2", "gaga", "arg3"))
  }
}
