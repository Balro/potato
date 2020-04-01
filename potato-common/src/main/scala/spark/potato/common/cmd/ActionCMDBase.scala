package spark.potato.common.cmd

import scala.collection.mutable

/**
 * 命令行工具类，集成次类实现对命令行的参数处理。
 * 命令行参数请务必指定 --action 参数，指定调用的Action。
 * 使用方法:
 * 实现init()，在init中重复调用 addAction() 添加指定动作。
 */
abstract class ActionCMDBase {
  private var action: Action = _
  private val actions = mutable.LinkedHashMap.empty[String, Action]
  private val arguments = mutable.LinkedHashMap.empty[String, Argument]
  protected var props = mutable.Map.empty[String, String]
  private val outputBuffer = new StringBuffer("\n")

  private lazy val keyWithValue: Set[String] = arguments.flatMap { f =>
    if (f._2.needValue) Some(f._1)
    else None
  }.toSet
  private lazy val keyWithoutValue: Set[String] = arguments.keySet.toSet &~ keyWithValue

  /**
   * 添加action,argument以及其他初始化。
   */
  protected def init(): Unit

  protected def output(msg: Any): Unit = outputBuffer.append(msg.toString + "\n")

  private def usage(): Unit = {
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

  protected def addAction(action: Action): Unit = {
    actions += action.name -> action
  }

  protected def addAction(name: String, describe: String = "no description",
                          needArgs: Set[String] = Set.empty, action: () => Unit = () => println("do nothing")): Unit = {
    addAction(Action(name, describe, needArgs, action))
  }

  protected def addArgument(key: String, describe: String = "no description",
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

  private def parseArgs(args: Array[String]): Unit = {
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

    props ++= CmdParserUtil.parseWithKeys(args.tail.toList, keyWithoutValue, keyWithValue)
  }

  case class Action(name: String, describe: String = "no description", neededArgs: Set[String] = Set.empty,
                    action: () => Unit = () => println("do nothing")) {
    def act(): Unit = {
      val require = neededArgs.diff(props.keySet)
      if (require.nonEmpty)
        throw new Exception(s"Require args not found $require")
      action()
    }

    override def toString: String = {
      if (neededArgs.isEmpty)
        s"  $name\n    describe: $describe"
      else
        s"  $name\n    needArgs: ${neededArgs.mkString(",")}\n    describe: $describe"
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
