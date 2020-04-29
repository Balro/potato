package spark.potato.common.util

import spark.potato.common.exception.ArgParseException

import scala.collection.mutable

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
   * @param args    待解析字符串列表。
   * @param flagKey 不带值参数。
   * @param propKey 带值参数。
   * @return 解析后的参数。
   */
  def parseWithKeys(args: List[String], flagKey: Set[String], propKey: Set[String]): (Map[String, String], Set[String]) = {
    val props = mutable.Map.empty[String, String]
    val flags = mutable.Set.empty[String]
    var parse = args
    while (parse.nonEmpty) {
      parse match {
        case key :: tail if flagKey.contains(key) =>
          flags += key
          parse = tail
        case key :: value :: tail if propKey.contains(key) && !flagKey.union(propKey).contains(value) =>
          props += key -> value
          parse = tail
        case Nil =>
        case _ => throw ArgParseException(s"Remain args not parsed -> ${args.mkString(" ")}")
      }
    }
    props.toMap -> flags.toSet
  }

  /**
   * 对字符串进行解析，对传入的期望参数进行匹配，遇到无法匹配的参数则抛异常。
   *
   * @param argString 待解析字符串。
   * @param keyOnly   不带值参数。
   * @param withValue 带值参数。
   * @return 解析后的参数。
   */
  def parseWithKeys(argString: String, keyOnly: Set[String], withValue: Set[String]): (Map[String, String], Set[String]) = {
    parseWithKeys(argString.split("\\s+").toList, keyOnly, withValue)
  }

  /**
   * 将逗号分隔并带有等号的字符串解析成map。
   * Example: "a=b,c=d" -> Map("a"->"b","c"->"d")
   */
  def commaString2Map(str: String): Map[String, String] = {
    str.split(",").map(f => f.split("=")).map(f => f(0) -> f(1)).toMap
  }
}
