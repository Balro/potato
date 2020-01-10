package spark.streaming.potato.core.utils

/**
 * 命令行参数请务必指定 --action 参数，指定调用的Action。
 * 使用:
 * addAction 添加指定动作。
 * addArg 添加无值参数。
 * addProp 添加带值参数。。
 */
trait GeneralCmd {
  var action: Action = _
  var actions = Map.empty[String, Action]
  var arguments = Map.empty[String, Argument]
  var props = Map.empty[String, String]

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
    println("\nsupport actions:")
    actions.values.foreach(println)
    println("\nsupport arguments:")
    arguments.values.foreach(println)
  }

  def addAction(name: String, describe: String = "no description",
                needArgs: Set[String] = Set.empty, action: Map[String, String] => Unit = _ => println("do nothing")): Unit = {
    actions += (name -> Action(name, describe, needArgs, action))
  }

  def addArgument(key: String, describe: String = "no description",
                  needValue: Boolean = false, default: String = null): Unit = {
    arguments += (key -> Argument(key, describe, needValue, default))
  }

  def main(args: Array[String]): Unit = {
    init()
    parseArgs(args)
    action.act()
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

  case class Action(name: String, describe: String, needArgs: Set[String], action: Map[String, String] => Unit) {
    def act(): Unit = {
      action(props.filter(prop => {
        needArgs.contains(prop._1)
      }))
    }

    override def toString: String = {
      if (needArgs.isEmpty)
        s"  $name\n    describe: $describe"
      else
        s"  $name  needArgs: ${needArgs.mkString(",")}\n    describe: $describe"
    }
  }

  case class Argument(key: String, describe: String, needValue: Boolean, default: String) {
    override def toString: String = {
      if (needValue)
        if (default == null)
          s"  $key  <some value>\n    $describe"
        else
          s"  $key  <default: $default>\n    $describe"
      else
        s"  $key\n    describe: $describe"
    }
  }

}