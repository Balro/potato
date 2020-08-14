package potato.common.sender

/**
 * 用于向外部发送消息的特质。比如实现监控警报功能等。
 */
trait Sender[T, R] {
  def send(msg: T): R
}
