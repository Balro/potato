package spark.potato.monitor.notify

trait Notify {
  def notify(msg: String): Unit
}
