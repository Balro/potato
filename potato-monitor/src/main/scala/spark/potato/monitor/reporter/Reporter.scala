package spark.potato.monitor.reporter

/**
 * reporter特质，用于汇报特定信息。
 */
trait Reporter {
  def report(msg: String)
}
