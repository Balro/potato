package spark.potato.monitor.notify

/**
 * 具有发送通知功能的特质。
 */
trait Notify {
  def notify(msg: String): Unit
}
