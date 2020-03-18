package spark.potato.common.cmd

import spark.potato.common.exception.ArgParseException

object CmdParserUtil {
  /**
   * 解析字符串参数。
   *
   * @return
   * 以'-'或'--'前缀的参数全部移除前缀后作为key，其后第二个参数如非key，则为第一个参数的值。
   * 若非key参数前一个参数非key，则将其收集入Seq后返回。
   * @example
   * Seq(-a, b, -c, d, e, -f, -g, h, i) 返回 (Map(a -> b, c -> d, f -> null, g -> h),List(e, i))
   */
  def parse(args: List[String]): (Map[String, String], Seq[String]) = {
    println(s"parse $args")
    val keyPattern = "-{1,2}([^-]+)".r
    val valuePattern = "([^-].*|[-]{3,}.*)".r
    args match {
      case keyPattern(key) :: valuePattern(value) :: tail =>
        val (m, s) = parse(tail)
        Map(key -> value) ++ m -> s
      case keyPattern(key) :: tail =>
        val (m, s) = parse(tail)
        Map(key -> null) ++ m -> s
      case value :: tail =>
        val (m, s) = parse(tail)
        m -> (Seq(value) ++ s)
      case Nil =>
        Map.empty[String, String] -> Seq.empty[String]
    }
  }

  /**
   * 解析字符串参数。
   *
   * @return
   * 以'-'或'--'前缀的参数全部移除前缀后作为key，其后第二个参数如非key，则为第一个参数的值。
   * 若非key参数前一个参数非key，则将其收集入Seq后返回。
   * @example
   * "-a b -c d e -f -g h i" 返回 (Map(a -> b, c -> d, f -> null, g -> h),List(e, i))
   */
  def parse(args: String): (Map[String, String], Seq[String]) = {
    parse(args.split("\\s+").toList)
  }

  /**
   * 对字符串进行解析，对传入的期望参数进行匹配，遇到无法匹配的参数则抛异常。
   *
   * @param args      待解析字符串列表。
   * @param keyOnly   不带值参数。
   * @param withValue 带值参数。
   * @return 解析后的参数。
   */
  def parseWithKeys(args: List[String], keyOnly: Set[String], withValue: Set[String]): Map[String, String] = {
    args match {
      case key :: tail if keyOnly.contains(key) =>
        Map(key -> null) ++ parseWithKeys(tail, keyOnly, withValue)
      case key :: value :: tail if withValue.contains(key) && !keyOnly.union(withValue).contains(value) =>
        Map(key -> value) ++ parseWithKeys(tail, keyOnly, withValue)
      case Nil => Map.empty
      case _ => throw ArgParseException(s"Remain args not parsed -> ${args.mkString(" ")}")
    }
  }

  /**
   * 对字符串进行解析，对传入的期望参数进行匹配，遇到无法匹配的参数则抛异常。
   *
   * @param argString 待解析字符串。
   * @param keyOnly   不带值参数。
   * @param withValue 带值参数。
   * @return 解析后的参数。
   */
  def parseWithKeys(argString: String, keyOnly: Set[String], withValue: Set[String]): Map[String, String] = {
    parseWithKeys(argString.split("\\s+").toList, keyOnly, withValue)
  }

}
