package potato.common.sender

trait Sender[T, R] {
  def send(msg: T): R
}
