package spark.potato.common.cmd

import scala.collection.mutable

/**
 * 命令行工具类，集成次类实现对命令行的参数处理。
 * 命令行参数请务必指定 --action 参数，指定调用的Action。
 * 使用方法:
 * 实现init()，在init中重复调用 addAction() 添加指定动作。
 */
abstract class ActionCMDBase {
  private var execAction: Action = _
  private val actions = mutable.LinkedHashMap.empty[String, Action]
  private val arguments = mutable.LinkedHashMap.empty[String, Argument]
  protected var props = Map.empty[String, String]
  protected var flags = Set.empty[String]
  private val outputBuffer = new StringBuffer("\n")

  private lazy val propsKey: Set[String] = arguments.flatMap { f =>
    if (f._2.needValue) Some(f._1)
    else None
  }.toSet
  private lazy val flagsKey: Set[String] = arguments.keySet.toSet &~ propsKey

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
                          neededArgs: Set[String] = Set.empty, neededFlags: Set[String] = Set.empty,
                          action: () => Unit = () => println("do nothing")): Unit = {
    addAction(Action(name, describe, neededArgs, neededFlags, action))
  }

  protected def addArgument(key: String, describe: String = "no description",
                            needValue: Boolean = false, default: String = null): Unit = {
    arguments += (key -> Argument(key, describe, needValue, default))
  }

  def main(args: Array[String]): Unit = {
    init()
    try {
      parseArgs(args)
      execAction.act()
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

    execAction = actions.get(args.head) match {
      case Some(a) => a
      case None =>
        println(s"action ${args.head} not found")
        usage()
        System.exit(1)
        throw new Exception(s"action ${args.head} not found")
    }

    val (ps, fs) = CmdParserUtil.parseWithKeys(args.tail.toList, flagsKey, propsKey)
    props = ps
    flags = fs
  }

  case class Action(name: String, describe: String = "no description",
                    neededProps: Set[String] = Set.empty, neededFlags: Set[String] = Set.empty,
                    action: () => Unit = () => println("do nothing")) {
    def act(): Unit = {
      val requireProps = neededProps &~ props.keySet
      val requireFlags = neededFlags &~ flags
      if (requireProps.nonEmpty)
        throw new Exception(s"Require args not found $requireProps")
      if (requireFlags.nonEmpty)
        throw new Exception(s"Require args not found $requireFlags")
      action()
    }

    override def toString: String = {
      if (neededProps.isEmpty)
        s"  $name\n    describe: $describe"
      else
        s"  $name\n    needArgs: ${neededProps.mkString(",")}\n    describe: $describe"
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
