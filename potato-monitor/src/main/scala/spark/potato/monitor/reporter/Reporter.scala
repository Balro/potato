package spark.potato.monitor.reporter

/**
 * reporter特质，用于汇报特定信息。
 */
trait Reporter {
  def problem(msg: String): Unit = ???

  def recover(msg: String): Unit = ???

  def report(msg: String): Unit = ???
}
