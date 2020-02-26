package spark.potato.common.cmd

import spark.potato.common.exception.PotatoException

object ArgsParserUtil {
  def parse(args: TraversableOnce[String], flag: Set[String] = Set.empty, key: Set[String] = Set.empty): Map[String, String] = {
    args.toList match {
      case k :: tail if flag.contains(k) =>
        Map(k -> null) ++ parse(tail, flag, key)
      case k :: v :: tail if key.contains(k) =>
        Map(k -> v) ++ parse(tail, flag, key)
      case Nil => Map.empty
      case _ => throw ArgParseException(s"Remain args not parsed -> ${args.mkString(" ")}")
    }
  }

  /**
   * @param argString  待解析字符串。
   * @param flag  不带值参数。
   * @param key  带值参数。
   * @return 解析后的参数。
   */
  def parseString(argString: String, flag: Set[String] = Set.empty, key: Set[String] = Set.empty): Map[String, String] = {
    parse(argString.split("\\s+"), flag, key)
  }

  case class ArgParseException(msg: String = null, throwable: Throwable = null) extends PotatoException(msg, throwable)

}
