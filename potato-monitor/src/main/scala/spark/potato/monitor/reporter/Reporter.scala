package spark.potato.monitor.reporter

trait Reporter {
  def report(msg: String)
}
