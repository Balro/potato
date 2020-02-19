package spark.streaming.potato.plugins.monitor.reporter

trait Reporter {
  def report(msg: String)
}
