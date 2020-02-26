package spark.potato.common.cmd

object CmdParser {
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
      case keyPattern(key) :: valuePattern(value) :: Nil =>
        Map(key -> value) -> Seq.empty
      case keyPattern(key) :: tail =>
        val (m, s) = parse(tail)
        Map(key -> null) ++ m -> s
      case keyPattern(key) :: Nil =>
        Map(key -> null) -> Seq.empty
      case value :: tail =>
        val (m, s) = parse(tail)
        m -> (Seq(value) ++ s)
      case value :: Nil =>
        Map.empty[String, String] -> Seq(value)
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
}
