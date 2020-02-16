package spark.streaming.potato.common.util

object ArgsParserUtil {
  def parse(args: TraversableOnce[String], ks: Set[String] = Set.empty, kvs: Set[String] = Set.empty): Map[String, String] = {
    args.toList match {
      case key :: tail if ks.contains(key) =>
        Map(key -> null) ++ parse(tail, ks, kvs)
      case key :: value :: tail if kvs.contains(key) =>
        Map(key -> value) ++ parse(tail, ks, kvs)
      case Nil => Map.empty
      case _ => throw ArgParseException(s"Remain args not parsed -> ${args.mkString(" ")}")
    }
  }

  def parseString(argString: String, ks: Set[String] = Set.empty, kvs: Set[String] = Set.empty): Map[String, String] = {
    parse(argString.split("\\s+"), ks, kvs)
  }

  case class ArgParseException(msg: String = null, throwable: Throwable = null) extends Exception(msg, throwable)

}
