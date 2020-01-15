package spark.streaming.potato.common.utils

/**
 * 命令行参数请务必指定 --action 参数，指定调用的Action。
 * 使用:
 * addAction 添加指定动作。
 * addArg 添加无值参数。
 * addProp 添加带值参数。。
 */
trait GeneralCmd {
  private var action: Action = _
  private var actions = Map.empty[String, Action]
  private var arguments = Map.empty[String, Argument]
  protected var props = Map.empty[String, String]
  private val outputBuffer = new StringBuffer("\n")

  def output(msg: Any): Unit = outputBuffer.append(msg.toString + "\n")

  lazy val keyWithValue: Set[String] = arguments.filter {
    _._2.needValue == true
  }.keySet
  lazy val keyWithoutValue: Set[String] = arguments.keySet.diff(keyWithValue)

  /**
   * 添加action,argument以及其他初始化。
   */
  def init(): Unit

  def usage(): Unit = {
    println("\nusage:")
    println(s"  ${this.getClass.getSimpleName} <action> [args]")
    if (arguments.nonEmpty) {
      println("\nsupport arguments:")
      arguments.values.foreach(println)
    }
    if (actions.nonEmpty) {
      println("\nsupport actions:")
      actions.values.foreach(println)
    }
  }

  def addAction(name: String, describe: String = "no description",
                needArgs: Set[String] = Set.empty, action: () => Unit = () => println("do nothing")): Unit = {
    actions += (name -> Action(name, describe, needArgs, action))
  }

  def addArgument(key: String, describe: String = "no description",
                  needValue: Boolean = false, default: String = null): Unit = {
    arguments += (key -> Argument(key, describe, needValue, default))
  }

  def main(args: Array[String]): Unit = {
    init()
    try {
      parseArgs(args)
      action.act()
    } catch {
      case e: Throwable =>
        output(e)
        output(e.getStackTrace.take(10).mkString("\n"))
        usage()
    }
    println(outputBuffer.toString)
  }

  def parseArgs(args: Array[String]): Unit = {
    if (args.isEmpty) {
      usage()
      System.exit(0)
    }

    action = actions.get(args.head) match {
      case Some(a) => a
      case None =>
        println(s"action ${args.head} not found")
        usage()
        System.exit(1)
        throw new Exception(s"action ${args.head} not found")
    }

    props = ArgsParserUtil.parse(args.tail, keyWithoutValue, keyWithValue)
  }

  case class Action(name: String, describe: String, requiredArgs: Set[String], action: () => Unit) {
    def act(): Unit = {
      val require = requiredArgs.diff(props.keySet)
      if (require.nonEmpty)
        throw new Exception(s"Require args not found $require")
      action()
    }

    override def toString: String = {
      if (requiredArgs.isEmpty)
        s"  $name\n    describe: $describe"
      else
        s"  $name\n    needArgs: ${requiredArgs.mkString(",")}\n    describe: $describe"
    }
  }

  case class Argument(key: String, describe: String, needValue: Boolean, default: String) {
    override def toString: String = {
      if (needValue)
        if (default == null)
          s"  $key  <some value>\n    describe: $describe"
        else
          s"  $key  <default: $default>\n    describe: $describe"
      else
        s"  $key\n    describe: $describe"
    }
  }

}